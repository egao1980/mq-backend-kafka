(in-package #:mq-backend-kafka)

;;; The default driver: librdkafka through CFFI (moved from mq-parity). One
;;; producer and one consumer handle per backend, made on first use. The
;;; consumer is a member of GROUP-ID, subscribes to the topics it is polled
;;; for, and commits offsets only on ACK (enable.auto.commit=false), so an
;;; unacknowledged message is delivered again to the group's next member.

(defstruct (rdkafka-session (:conc-name %session-))
  bootstrap
  group-id
  config
  producer
  consumer
  (topics '())
  (inflight (make-hash-table :test 'eql))
  (next-tag 0)
  (redeliver (make-hash-table :test 'equal))
  (lock (bt2:make-recursive-lock :name "rdkafka-session"))
  (closed nil))

(defun %kafka-error (format-control &rest args)
  (error 'mq-protocol:mq-error :message (apply #'format nil format-control args)))

;;; --- octets ----------------------------------------------------------------

(defun %to-octets (value)
  "VALUE (string, octets, symbol or other) as octets; strings are UTF-8."
  (etypecase value
    (null (make-array 0 :element-type '(unsigned-byte 8)))
    ((vector (unsigned-byte 8)) (coerce value '(simple-array (unsigned-byte 8) (*))))
    (string (encoding-protocol:encode value :encoding :utf-8))
    (symbol (encoding-protocol:encode (string-downcase (symbol-name value)) :encoding :utf-8))
    (t (encoding-protocol:encode (princ-to-string value) :encoding :utf-8))))

(defun %from-octets (octets)
  "OCTETS as a UTF-8 string, or the octets themselves when they are not UTF-8
(Kafka keys and header values may be binary)."
  (handler-case (encoding-protocol:decode octets :encoding :utf-8)
    ;; Invalid UTF-8 is a transcode error; the octets are the value then.
    (encoding-protocol:encoding-error () octets)))

(defun %foreign-octets (pointer n)
  (let ((out (make-array n :element-type '(unsigned-byte 8))))
    (dotimes (i n out)
      (setf (aref out i) (cffi:mem-aref pointer :uint8 i)))))

(defmacro %with-foreign-octets ((pointer length octets) &body body)
  "Bind POINTER / LENGTH to a foreign copy of OCTETS for BODY."
  (let ((o (gensym "OCTETS")))
    `(let* ((,o ,octets)
            (,length (length ,o))
            (,pointer (cffi:foreign-alloc :uint8 :count (max 1 ,length))))
       (unwind-protect
            (progn
              (dotimes (i ,length) (setf (cffi:mem-aref ,pointer :uint8 i) (aref ,o i)))
              ,@body)
         (cffi:foreign-free ,pointer)))))

;;; --- handles ---------------------------------------------------------------

(defun %conf-set (conf name value errstr)
  (unless (zerop (%rd-conf-set conf name value errstr 512))
    (%kafka-error "librdkafka config ~a=~a: ~a" name value
                  (cffi:foreign-string-to-lisp errstr))))

(defun %make-handle (type properties)
  "rd_kafka_new of TYPE with PROPERTIES, an alist of strings."
  (let ((conf (%rd-conf-new))
        (errstr (cffi:foreign-alloc :char :count 512)))
    (unwind-protect
         (progn
           (loop for (name . value) in properties
                 do (%conf-set conf name value errstr))
           (let ((rk (%rd-new type conf errstr 512)))
             (when (cffi:null-pointer-p rk)
               (%kafka-error "rd_kafka_new: ~a" (cffi:foreign-string-to-lisp errstr)))
             ;; rd_kafka_new owns CONF from here on.
             (setf conf (cffi:null-pointer))
             rk))
      (cffi:foreign-free errstr)
      (unless (cffi:null-pointer-p conf)
        (%rd-conf-destroy conf)))))

(defun %properties (session &rest defaults)
  "DEFAULTS overridden by the session's CONFIG, plus bootstrap.servers."
  (let ((props (append (list (cons "bootstrap.servers" (%session-bootstrap session)))
                       (%session-config session))))
    (loop for (name . value) in defaults
          unless (assoc name props :test #'string=)
            do (setf props (append props (list (cons name value)))))
    props))

(defun %producer (session)
  (or (%session-producer session)
      (setf (%session-producer session)
            (%make-handle +rd-kafka-producer+
                          (%properties session
                                       '("client.id" . "cl-stack-mq-backend-kafka")
                                       '("acks" . "all")
                                       '("linger.ms" . "5")
                                       '("allow.auto.create.topics" . "true"))))))

(defun %consumer (session)
  (or (%session-consumer session)
      (let ((rk (%make-handle +rd-kafka-consumer+
                              (%properties session
                                           '("client.id" . "cl-stack-mq-backend-kafka")
                                           (cons "group.id" (%session-group-id session))
                                           '("enable.auto.commit" . "false")
                                           '("auto.offset.reset" . "earliest")
                                           '("enable.partition.eof" . "false")
                                           '("allow.auto.create.topics" . "true")))))
        ;; Serve rebalances and errors from rd_kafka_consumer_poll.
        (%rd-poll-set-consumer rk)
        (setf (%session-consumer session) rk))))

(defun %ensure-subscribed (session topic)
  "Add TOPIC to the consumer's subscription (a rebalance follows)."
  (unless (member topic (%session-topics session) :test #'string=)
    (let ((topics (append (%session-topics session) (list topic)))
          (list (%rd-tpl-new 1)))
      (unwind-protect
           (progn
             (dolist (tp topics) (%rd-tpl-add list tp +rd-kafka-partition-ua+))
             (let ((rc (%rd-subscribe (%consumer session) list)))
               (unless (zerop rc)
                 (%kafka-error "rd_kafka_subscribe ~{~a~^, ~}: ~a" topics (%rd-err2str rc)))))
        (%rd-tpl-destroy list))
      (setf (%session-topics session) topics))))

;;; --- produce ---------------------------------------------------------------

(defun %vu (base i) (cffi:inc-pointer base (* i +rd-kafka-vu-size+)))

(defun %vu-set (base i vtype)
  (let ((p (%vu base i)))
    (dotimes (b +rd-kafka-vu-size+) (setf (cffi:mem-aref p :uint8 b) 0))
    (setf (cffi:mem-ref p :int) vtype)
    (cffi:inc-pointer p +rd-kafka-vu-union+)))

(defun %header-pairs (headers)
  "HEADERS, an alist or a plist, as (name . value) pairs."
  (cond
    ((null headers) nil)
    ((consp (first headers)) headers)
    (t (loop for (k v) on headers by #'cddr collect (cons k v)))))

(defun %make-headers (headers)
  "A new rd_kafka_headers_t for HEADERS, or a null pointer."
  (let ((pairs (%header-pairs headers)))
    (if (null pairs)
        (cffi:null-pointer)
        (let ((hdrs (%rd-headers-new (length pairs))))
          (loop for (name . value) in pairs
                do (cffi:with-foreign-string (name-ptr (if (stringp name)
                                                           name
                                                           (string-downcase (string name)))
                                                       :encoding :utf-8)
                     (%with-foreign-octets (ptr n (%to-octets value))
                       (let ((rc (%rd-header-add hdrs name-ptr -1 ptr n)))
                         (unless (zerop rc)
                           (%rd-headers-destroy hdrs)
                           (%kafka-error "rd_kafka_header_add ~a: ~a" name (%rd-err2str rc)))))))
          hdrs))))

(defun %produce (session topic message)
  (let ((rk (%producer session))
        (headers (%make-headers (mq-protocol:mq-message-headers message)))
        (arr (cffi:foreign-alloc :uint8 :count (* 5 +rd-kafka-vu-size+))))
    (unwind-protect
         (cffi:with-foreign-string (topic-ptr topic :encoding :utf-8)
           (%with-foreign-octets (key-ptr key-len (%to-octets (mq-protocol:mq-message-key message)))
             (%with-foreign-octets (val-ptr val-len (mq-protocol:mq-message-payload message))
               (setf (cffi:mem-ref (%vu-set arr 0 +rd-kafka-vtype-topic+) :pointer) topic-ptr)
               (let ((u (%vu-set arr 1 +rd-kafka-vtype-key+)))
                 (setf (cffi:mem-ref u :pointer) (if (zerop key-len) (cffi:null-pointer) key-ptr)
                       (cffi:mem-ref (cffi:inc-pointer u 8) :size) key-len))
               (let ((u (%vu-set arr 2 +rd-kafka-vtype-value+)))
                 (setf (cffi:mem-ref u :pointer) val-ptr
                       (cffi:mem-ref (cffi:inc-pointer u 8) :size) val-len))
               (setf (cffi:mem-ref (%vu-set arr 3 +rd-kafka-vtype-msgflags+) :int)
                     +rd-kafka-msg-f-copy+)
               (let ((n 4))
                 (unless (cffi:null-pointer-p headers)
                   (setf (cffi:mem-ref (%vu-set arr 4 +rd-kafka-vtype-headers+) :pointer) headers)
                   (setf n 5))
                 ;; The local queue may be full: serve delivery reports and retry.
                 (loop for attempt from 0
                       for err = (%rd-produceva rk arr n)
                       do (cond
                            ((cffi:null-pointer-p err)
                             ;; librdkafka owns HEADERS once the message is queued.
                             (setf headers (cffi:null-pointer))
                             (return))
                            (t
                             (let ((code (%rd-error-code err))
                                   (text (%rd-error-string err)))
                               (%rd-error-destroy err)
                               (unless (and (= code +rd-kafka-resp-err-queue-full+) (< attempt 50))
                                 (%kafka-error "rd_kafka_produceva ~a: ~a" topic text))
                               (%rd-poll rk 100)))))
                 (%rd-poll rk 0)))))
      (unless (cffi:null-pointer-p headers)
        (%rd-headers-destroy headers))
      (cffi:foreign-free arr))
    message))

;;; --- consume ---------------------------------------------------------------

(defun %read-headers (rkmessage)
  (cffi:with-foreign-object (hdrsp :pointer)
    (setf (cffi:mem-ref hdrsp :pointer) (cffi:null-pointer))
    (when (zerop (%rd-message-headers rkmessage hdrsp))
      (let ((hdrs (cffi:mem-ref hdrsp :pointer)))
        (unless (cffi:null-pointer-p hdrs)
          (loop for i from 0 below (%rd-header-cnt hdrs)
                collect (cffi:with-foreign-objects ((namep :pointer) (valuep :pointer)
                                                    (sizep :size))
                          (%rd-header-get-all hdrs i namep valuep sizep)
                          (let ((value (cffi:mem-ref valuep :pointer)))
                            (cons (cffi:foreign-string-to-lisp (cffi:mem-ref namep :pointer)
                                                               :encoding :utf-8)
                                  (if (cffi:null-pointer-p value)
                                      nil
                                      (%from-octets (%foreign-octets
                                                     value (cffi:mem-ref sizep :size)))))))))))))

(defun %message (session rkmessage)
  "An MQ-MESSAGE for a consumed rd_kafka_message_t; its delivery tag maps to
(topic partition offset) for ACK / NACK."
  (cffi:with-foreign-slots ((rkt partition payload len key key-len offset)
                            rkmessage (:struct rd-kafka-message))
    (let* ((topic (and (not (cffi:null-pointer-p rkt)) (%rd-topic-name rkt)))
           (meta (list topic partition offset))
           (tag (incf (%session-next-tag session))))
      (setf (gethash tag (%session-inflight session)) meta)
      (mq-protocol:make-mq-message
       :key (and (not (cffi:null-pointer-p key)) (plusp key-len)
                 (%from-octets (%foreign-octets key key-len)))
       :payload (if (cffi:null-pointer-p payload) #() (%foreign-octets payload len))
       :headers (%read-headers rkmessage)
       :delivery-tag tag
       :topic topic
       :redelivered-p (and (gethash meta (%session-redeliver session)) t)))))

(defun %poll (session topic &key (n 1) timeout)
  "Up to N messages of TOPIC. TIMEOUT seconds (NIL: one 100 ms poll)."
  (%ensure-subscribed session topic)
  (let* ((rk (%consumer session))
         (deadline (+ (get-internal-real-time)
                      (round (* (or timeout 0) internal-time-units-per-second))))
         (out '()))
    (loop
      (let* ((left (- deadline (get-internal-real-time)))
             (wait-ms (max 0 (min 200 (round (* 1000 left) internal-time-units-per-second))))
             (rkmessage (%rd-consumer-poll rk (if timeout wait-ms 100))))
        (unless (cffi:null-pointer-p rkmessage)
          (unwind-protect
               (let ((err (cffi:foreign-slot-value rkmessage '(:struct rd-kafka-message) 'err)))
                 ;; Non-zero ERR: a consumer event (rebalance in progress,
                 ;; unknown topic before auto-create, ...) that librdkafka
                 ;; retries by itself; it carries no message.
                 (when (zerop err)
                   (push (%message session rkmessage) out)))
            (%rd-message-destroy rkmessage))))
      (when (or (>= (length out) n)
                (null timeout)
                (>= (get-internal-real-time) deadline))
        (return (nreverse out))))))

(defun %one-partition (topic partition offset)
  (let* ((list (%rd-tpl-new 1))
         (tp (%rd-tpl-add list topic partition)))
    (setf (cffi:foreign-slot-value tp '(:struct rd-kafka-topic-partition) 'offset) offset)
    list))

(defun %take-inflight (session message)
  (let* ((tag (if (integerp message) message (mq-protocol:mq-message-delivery-tag message)))
         (meta (gethash tag (%session-inflight session))))
    (unless meta
      (%kafka-error "unknown or already settled delivery tag ~s" tag))
    (remhash tag (%session-inflight session))
    meta))

(defun %commit (session topic partition offset)
  "Commit OFFSET + 1 for TOPIC / PARTITION, synchronously."
  (let ((list (%one-partition topic partition (1+ offset))))
    (unwind-protect
         (let ((rc (%rd-commit (%consumer session) list 0)))
           (unless (zerop rc)
             (%kafka-error "rd_kafka_commit ~a[~d]@~d: ~a" topic partition (1+ offset)
                           (%rd-err2str rc))))
      (%rd-tpl-destroy list))))

(defun %ack (session message)
  "Commit the message's offset: the group resumes after it. As in Kafka, this
settles every earlier message of the same partition too."
  (destructuring-bind (topic partition offset) (%take-inflight session message)
    (remhash (list topic partition offset) (%session-redeliver session))
    (%commit session topic partition offset))
  message)

(defun %nack (session message &key (requeue t))
  "REQUEUE: seek the partition back to the message, so it (and what followed
it) is delivered again, marked redelivered. Otherwise commit past it."
  (destructuring-bind (topic partition offset) (%take-inflight session message)
    (if requeue
        (let ((list (%one-partition topic partition offset)))
          (unwind-protect
               (let ((err (%rd-seek-partitions (%consumer session) list 5000)))
                 (unless (cffi:null-pointer-p err)
                   (let ((text (%rd-error-string err)))
                     (%rd-error-destroy err)
                     (%kafka-error "rd_kafka_seek_partitions ~a[~d]@~d: ~a"
                                   topic partition offset text))))
            (%rd-tpl-destroy list))
          (setf (gethash (list topic partition offset) (%session-redeliver session)) t))
        (%commit session topic partition offset)))
  message)

(defun %flush (session timeout)
  "Wait up to TIMEOUT seconds for queued messages to be delivered. → T when
the queue drained."
  (let ((rk (%session-producer session)))
    (or (null rk)
        (zerop (%rd-flush rk (round (* 1000 timeout)))))))

(defun %close-session (session &key abort)
  "Leave the group, flush (unless ABORT) and destroy both handles. → T when
there were handles to release."
  (unless (%session-closed session)
    (setf (%session-closed session) t)
    (let ((consumer (%session-consumer session))
          (producer (%session-producer session)))
      (setf (%session-consumer session) nil
            (%session-producer session) nil)
      (when consumer
        (%rd-consumer-close consumer)
        (%rd-destroy consumer))
      (when producer
        (unless abort (%rd-flush producer 10000))
        (%rd-destroy producer))
      (and (or consumer producer) t))))

(defun %topic-name (destination)
  (etypecase destination
    (string destination)
    (symbol (string-downcase (symbol-name destination)))))

(defun make-rdkafka-driver (&key (bootstrap-servers "localhost:9092") group-id config)
  "The librdkafka driver plist (:publish :poll :ack :nack :flush :close) for
MAKE-KAFKA-BACKEND. CONFIG is an alist of librdkafka properties
(\"security.protocol\" . \"SASL_SSL\") applied to both handles; GROUP-ID
defaults to a fresh one."
  (let ((session (make-rdkafka-session
                  :bootstrap bootstrap-servers
                  :group-id (or group-id
                                (format nil "cl-stack-~36r-~36r" (get-universal-time)
                                        (random (expt 2 40) (make-random-state t))))
                  :config config)))
    (flet ((locked (fn)
             (lambda (backend &rest args)
               (declare (ignore backend))
               (bt2:with-recursive-lock-held ((%session-lock session))
                 (when (%session-closed session)
                   (error 'mq-protocol:mq-connection-error
                          :message "the Kafka backend is closed"))
                 (apply fn session args)))))
      (list :publish (locked (lambda (s destination message)
                               ;; The destination names the topic; a message
                               ;; polled from another topic keeps its own.
                               (%produce s (if destination
                                               (%topic-name destination)
                                               (mq-protocol:mq-message-topic message))
                                         message)))
            :poll (locked (lambda (s destination &rest keys)
                            (apply #'%poll s (%topic-name destination) keys)))
            :ack (locked #'%ack)
            :nack (locked #'%nack)
            :flush (locked (lambda (s &optional (timeout 10)) (%flush s timeout)))
            :close (lambda (backend &key abort)
                     (declare (ignore backend))
                     (bt2:with-recursive-lock-held ((%session-lock session))
                       (%close-session session :abort abort)))
            :group-id (%session-group-id session)))))

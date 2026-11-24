(in-package #:mq-backend-kafka)

;;; The default driver: librdkafka through CFFI (moved from mq-parity). One
;;; producer and one consumer handle per backend, made on first use. The
;;; consumer is a member of GROUP-ID, subscribes to the topics it is polled
;;; for, and commits offsets only on ACK (enable.auto.commit=false), so an
;;; unacknowledged message is delivered again to the group's next member.
;;; POLL-MESSAGES returns the polled topic only: messages of other
;;; subscribed topics are buffered per topic (their partition paused), and a
;;; message the backend still holds is never handed out twice.

(defstruct (rdkafka-session (:conc-name %session-))
  bootstrap
  group-id
  config
  producer
  consumer
  (topics '())
  (inflight (make-hash-table :test 'eql))
  ;; (topic partition offset) → delivery tag of every message this backend
  ;; holds: handed out and not settled, or buffered.
  (held (make-hash-table :test 'equal))
  ;; topic → messages of that topic fetched while another topic was
  ;; polled, oldest first.
  (buffered (make-hash-table :test 'equal))
  ;; topic → partitions paused because their messages were buffered.
  (paused (make-hash-table :test 'equal))
  (next-tag 0)
  (redeliver (make-hash-table :test 'equal))
  ;; (topic partition) → offsets settled (ACK, or NACK without requeue)
  ;; that the group's commit has not passed yet. A seek or a rebalance that
  ;; fetches one again skips it.
  (settled (make-hash-table :test 'equal))
  ;; (topic partition) → offsets NACKed with requeue and not fetched again
  ;; yet: unsettled, so the commit does not pass them.
  (requeued (make-hash-table :test 'equal))
  ;; (topic partition) → the next offset this backend committed last.
  (committed (make-hash-table :test 'equal))
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

(defun %rkmessage-meta (rkmessage)
  "(topic partition offset) of a consumed rd_kafka_message_t."
  (cffi:with-foreign-slots ((rkt partition offset) rkmessage (:struct rd-kafka-message))
    (list (and (not (cffi:null-pointer-p rkt)) (%rd-topic-name rkt)) partition offset)))

(defun %message (session rkmessage meta)
  "An MQ-MESSAGE for a consumed rd_kafka_message_t at META (topic partition
offset); its delivery tag maps to META for ACK / NACK, and the session
holds META until the message is settled."
  (cffi:with-foreign-slots ((payload len key key-len) rkmessage (:struct rd-kafka-message))
    (let ((tag (incf (%session-next-tag session))))
      (setf (gethash tag (%session-inflight session)) meta
            (gethash meta (%session-held session)) tag)
      (mq-protocol:make-mq-message
       :key (and (not (cffi:null-pointer-p key)) (plusp key-len)
                 (%from-octets (%foreign-octets key key-len)))
       :payload (if (cffi:null-pointer-p payload) #() (%foreign-octets payload len))
       :headers (%read-headers rkmessage)
       :delivery-tag tag
       :topic (first meta)
       :redelivered-p (and (gethash meta (%session-redeliver session)) t)))))

(defun %buffer (session topic message)
  "Keep MESSAGE, fetched for another topic, until TOPIC is polled."
  (setf (gethash topic (%session-buffered session))
        (append (gethash topic (%session-buffered session)) (list message))))

(defun %take-buffered (session topic n)
  "Up to N buffered messages of TOPIC, oldest first, removed from the buffer."
  (let* ((all (gethash topic (%session-buffered session)))
         (k (min n (length all))))
    (setf (gethash topic (%session-buffered session)) (nthcdr k all))
    (subseq all 0 k)))

(defun %partitions-call (session fn what topic partitions)
  "Call FN (rd_kafka_pause/resume_partitions) on TOPIC's PARTITIONS."
  (let ((list (%rd-tpl-new (length partitions))))
    (unwind-protect
        (progn
          (dolist (p partitions) (%rd-tpl-add list topic p))
          (let ((rc (funcall fn (%consumer session) list)))
            (unless (zerop rc)
              (%kafka-error "rd_kafka_~a_partitions ~a~a: ~a" what topic partitions
                            (%rd-err2str rc)))))
      (%rd-tpl-destroy list))))

(defun %pause-partition (session topic partition)
  "Stop fetching TOPIC / PARTITION while another topic is polled, so the
buffer holds few of its messages. librdkafka resumes from the message after
the last one it returned."
  (unless (member partition (gethash topic (%session-paused session)))
    (%partitions-call session #'%rd-pause-partitions "pause" topic (list partition))
    (push partition (gethash topic (%session-paused session)))))

(defun %resume-topic (session topic)
  "Resume the partitions of TOPIC that %PAUSE-PARTITION paused."
  (let ((partitions (gethash topic (%session-paused session))))
    (when partitions
      (%partitions-call session #'%rd-resume-partitions "resume" topic partitions)
      (remhash topic (%session-paused session)))))

(defun %poll (session topic &key (n 1) timeout)
  "Up to N messages of TOPIC. TIMEOUT seconds (NIL: one 100 ms poll).
The consumer is subscribed to every topic polled so far, so a message of
another topic can arrive: it is buffered for that topic, oldest first, and
its partition paused until that topic is polled. A message this backend
still holds (handed out and not settled, or buffered), or has settled
past the group's commit, is not handed out again when a rebalance or a
seek fetches it a second time."
  (%ensure-subscribed session topic)
  (%resume-topic session topic)
  (let* ((rk (%consumer session))
         (deadline (+ (get-internal-real-time)
                      (round (* (or timeout 0) internal-time-units-per-second))))
         ;; OUT is newest first until the final NREVERSE.
         (out (reverse (%take-buffered session topic n))))
    (loop
      (when (or (>= (length out) n)
                (and out (null timeout)))
        (return))
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
                  (let ((meta (%rkmessage-meta rkmessage)))
                    (cond
                     ((gethash meta (%session-held session)))
                     ((%settled-p session meta))
                     (t
                      (%unrequeue session meta)
                      (if (equal (first meta) topic)
                          (push (%message session rkmessage meta) out)
                          (progn
                            (%buffer session (first meta) (%message session rkmessage meta))
                            (%pause-partition session (first meta) (second meta)))))))))
            (%rd-message-destroy rkmessage))))
      (when (or (null timeout)
                (>= (get-internal-real-time) deadline))
        (return)))
    (nreverse out)))

(defun %one-partition (topic partition offset)
  (let* ((list (%rd-tpl-new 1))
         (tp (%rd-tpl-add list topic partition)))
    (setf (cffi:foreign-slot-value tp '(:struct rd-kafka-topic-partition) 'offset) offset)
    list))

(defun %take-inflight (session message)
  "Settle MESSAGE (or a delivery tag): the session no longer holds it.
→ its (topic partition offset)."
  (let* ((tag (if (integerp message) message (mq-protocol:mq-message-delivery-tag message)))
         (meta (gethash tag (%session-inflight session))))
    (unless meta
      (%kafka-error "unknown or already settled delivery tag ~s" tag))
    (remhash tag (%session-inflight session))
    (remhash meta (%session-held session))
    meta))

(defun %commit (session topic partition next)
  "Commit NEXT (the offset the group resumes at) for TOPIC / PARTITION,
synchronously."
  (let ((list (%one-partition topic partition next)))
    (unwind-protect
        (let ((rc (%rd-commit (%consumer session) list 0)))
          (unless (zerop rc)
            (%kafka-error "rd_kafka_commit ~a[~d]@~d: ~a" topic partition next
                          (%rd-err2str rc))))
      (%rd-tpl-destroy list))))

(defun %settled-p (session meta)
  "Has this backend settled the message at META (topic partition offset)
past the group's commit? A seek or a rebalance can fetch it again."
  (destructuring-bind (topic partition offset) meta
    (and (member offset (gethash (list topic partition) (%session-settled session)))
         t)))

(defun %unrequeue (session meta)
  "The message at META is fetched again: it is held from now on, no longer
only requeued."
  (destructuring-bind (topic partition offset) meta
    (let ((key (list topic partition)))
      (when (gethash key (%session-requeued session))
        (setf (gethash key (%session-requeued session))
              (remove offset (gethash key (%session-requeued session))))))))

(defun %unsettled-offsets (session topic partition)
  "Offsets of TOPIC / PARTITION this backend fetched and has not settled:
held (handed out or buffered) or NACKed with requeue and not fetched again."
  (let ((out (copy-list (gethash (list topic partition) (%session-requeued session)))))
    (maphash (lambda (meta tag)
               (declare (ignore tag))
               (when (and (eql (second meta) partition) (equal (first meta) topic))
                 (push (third meta) out)))
             (%session-held session))
    out))

(defun %commit-target (settled unsettled committed)
  "The offset to commit for one partition, or NIL for no commit. SETTLED:
offsets settled and not committed past yet (non-empty); UNSETTLED: offsets
fetched and not settled; COMMITTED: the offset last committed, or NIL.
The target is the earliest unsettled offset, or one past the last settled
offset when none is unsettled. It is committed only when it settles
something new and moves forward."
  (let ((target (if unsettled
                    (reduce #'min unsettled)
                    (1+ (reduce #'max settled)))))
    (and (some (lambda (o) (< o target)) settled)
         (or (null committed) (> target committed))
         target)))

(defun %settle (session topic partition offset)
  "Settle the message at OFFSET of TOPIC / PARTITION and commit as far as
the settled messages reach (%COMMIT-TARGET). A message settled while an
earlier one is unsettled is committed once that one is settled; if this
member leaves first, the group's next member gets both again (at least
once). The commit never moves back."
  (let* ((key (list topic partition))
         (settled (cons offset (gethash key (%session-settled session))))
         (target (%commit-target settled
                                 (%unsettled-offsets session topic partition)
                                 (gethash key (%session-committed session)))))
    (setf (gethash key (%session-settled session)) settled)
    (when target
      (%commit session topic partition target)
      (setf (gethash key (%session-committed session)) target
            (gethash key (%session-settled session))
            (remove-if (lambda (o) (< o target)) settled)))))

(defun %ack (session message)
  "Settle the message (%SETTLE): the group's commit moves past it once
every earlier message of its partition that this backend fetched is
settled too, so acks in any order never move the commit back."
  (destructuring-bind (topic partition offset) (%take-inflight session message)
    (remhash (list topic partition offset) (%session-redeliver session))
    (%settle session topic partition offset))
  message)

(defun %drop-buffered-after (session topic partition offset)
  "Forget the buffered messages of TOPIC / PARTITION past OFFSET: a seek to
OFFSET fetches them again, after the message at OFFSET."
  (flet ((later-p (m)
           (destructuring-bind (tp p o)
               (gethash (mq-protocol:mq-message-delivery-tag m) (%session-inflight session))
             (and (equal tp topic) (eql p partition) (> o offset)))))
    (let ((buffered (gethash topic (%session-buffered session))))
      (dolist (m buffered)
        (when (later-p m)
          (%take-inflight session m)))
      (setf (gethash topic (%session-buffered session))
            (remove-if (lambda (m)
                         (null (gethash (mq-protocol:mq-message-delivery-tag m)
                                        (%session-inflight session))))
                       buffered)))))

(defun %nack (session message &key (requeue t))
  "REQUEUE: seek the partition back to the message, so it is delivered
again, marked redelivered, followed by the later messages of its partition
that this backend has not handed out (buffered ones are dropped and fetched
again, in order). Later messages still held, or already settled, are not
repeated. If an earlier message of the partition was requeued and not
fetched again yet, the seek goes back to that one. Otherwise settle it
like ACK (%SETTLE)."
  (destructuring-bind (topic partition offset) (%take-inflight session message)
    (if requeue
        (let* ((key (list topic partition))
               (requeued (cons offset (gethash key (%session-requeued session))))
               (seek-to (reduce #'min requeued)))
          (setf (gethash key (%session-requeued session)) requeued)
          (%drop-buffered-after session topic partition seek-to)
          (when (member partition (gethash topic (%session-paused session)))
            ;; Fetch from the seek position now; a message of a topic that
            ;; is not being polled is buffered and paused again.
            (%partitions-call session #'%rd-resume-partitions "resume" topic (list partition))
            (setf (gethash topic (%session-paused session))
                  (remove partition (gethash topic (%session-paused session)))))
          (let ((list (%one-partition topic partition seek-to)))
            (unwind-protect
                (let ((err (%rd-seek-partitions (%consumer session) list 5000)))
                  (unless (cffi:null-pointer-p err)
                    (let ((text (%rd-error-string err)))
                      (%rd-error-destroy err)
                      (%kafka-error "rd_kafka_seek_partitions ~a[~d]@~d: ~a"
                                    topic partition seek-to text))))
              (%rd-tpl-destroy list)))
          (setf (gethash (list topic partition offset) (%session-redeliver session)) t))
        (progn
          (remhash (list topic partition offset) (%session-redeliver session))
          (%settle session topic partition offset))))
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

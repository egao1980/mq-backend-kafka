(in-package #:mq-backend-kafka)

;;; The default driver: librdkafka through CFFI (moved from mq-parity). One
;;; producer and one consumer handle per backend, made on first use. The
;;; consumer is a member of GROUP-ID, subscribes to the topics it is polled
;;; for, and commits offsets only on ACK (enable.auto.commit=false), so an
;;; unacknowledged message is delivered again to the group's next member.
;;; POLL-MESSAGES returns the polled topic only: messages of other
;;; subscribed topics are buffered per topic (their partition paused), and a
;;; message the backend still holds is never handed out twice.

;;; Threads: the handles are thread-safe, so PUBLISH never takes the
;;; consumer's locks, and POLL-MESSAGES holds the session lock only for its
;;; bookkeeping, never across a blocking rd_kafka_consumer_poll. CLOSE waits
;;; until no call uses the handles; a poll notices it within one slice.
;;; Rebalances go through the driver's own callback (%ON-REBALANCE), which
;;; tracks the partitions this member owns: a message of a partition revoked
;;; since it was fetched is never committed (KAFKA-PARTITION-REVOKED).
(defstruct (rdkafka-session (:conc-name %session-))
  bootstrap
  group-id
  config
  producer
  consumer
  (topics '())
  ;; delivery tag → (topic partition offset epoch) of every message this
  ;; backend fetched and has not settled. EPOCH is the partition's
  ;; assignment epoch when the message was fetched (%PARTITION-EPOCH).
  (inflight (make-hash-table :test 'eql))
  ;; (topic partition offset) → delivery tag of every message this backend
  ;; holds: handed out and not settled, or buffered.
  (held (make-hash-table :test 'equal))
  ;; topic → messages of that topic fetched while another topic was
  ;; polled, oldest first.
  (buffered (make-hash-table :test 'equal))
  ;; topic → partitions paused because their messages were buffered.
  (paused (make-hash-table :test 'equal))
  ;; topic → number of threads in POLL-MESSAGES of that topic. A message of
  ;; it that another thread's poll fetches is buffered, its partition is
  ;; not paused.
  (polling (make-hash-table :test 'equal))
  (next-tag 0)
  (redeliver (make-hash-table :test 'equal))
  ;; (topic partition) → offsets settled (ACK, or NACK without requeue)
  ;; that the group's commit has not passed yet. A seek or a rebalance that
  ;; fetches one again skips it.
  (settled (make-hash-table :test 'equal))
  ;; (topic partition) → offsets NACKed with requeue and not fetched again
  ;; yet: unsettled, so the commit does not pass them.
  (requeued (make-hash-table :test 'equal))
  ;; (topic partition) → the known commit: the group's committed offset
  ;; when the partition was last assigned (rd_kafka_committed;
  ;; RD_KAFKA_OFFSET_INVALID: none), moved by this member's own commits.
  ;; No entry: unknown.
  (committed (make-hash-table :test 'equal))
  ;; (topic partition) → the offset this member's own commit moved the
  ;; partition to since it was last assigned. A fetch that started before
  ;; that commit returns messages below it again: they are dropped.
  (own-commits (make-hash-table :test 'equal))
  ;; (topic partition) → :OWNED (assigned to this member), or :PENDING
  ;; (revoked by an eager rebalance that may give it back). No entry: not
  ;; this member's. Commits go only to :OWNED partitions.
  (ownership (make-hash-table :test 'equal))
  ;; (topic partition) → assignment epoch, bumped each time the partition
  ;; leaves this member for good (%REVOKE-FOR-GOOD).
  (epochs (make-hash-table :test 'equal))
  ;; An error %ON-REBALANCE caught; the poll that served it signals it.
  (callback-error nil)
  ;; The consumer's bookkeeping (the tables above), commits and seeks.
  (lock (bt2:make-recursive-lock :name "rdkafka-session"))
  ;; One rd_kafka_consumer_poll at a time: the message it returns is
  ;; registered before the next one is fetched, so a commit never passes a
  ;; message on its way to a caller.
  (poll-lock (bt2:make-lock :name "rdkafka-poll"))
  ;; Making the producer handle.
  (producer-lock (bt2:make-lock :name "rdkafka-producer"))
  ;; CLOSED, USERS, USER-THREADS and DEFERRED-CLOSE: the calls inside
  ;; %USING-HANDLES.
  (state-lock (bt2:make-lock :name "rdkafka-state"))
  (state-cv (bt2:make-condition-variable :name "rdkafka-state"))
  (users 0)
  ;; thread → the number of its calls inside %USING-HANDLES.
  (user-threads (make-hash-table :test 'eq))
  ;; (:abort abort) when CLOSE ran on a thread inside a call: that
  ;; thread's last %LEAVE releases the handles.
  (deferred-close nil)
  (closed nil)
  ;; This session's key in *SESSIONS*: the rebalance callback's opaque.
  (id nil))

(defun %kafka-error (format-control &rest args)
  (error 'mq-protocol:mq-error :message (apply #'format nil format-control args)))

(define-condition kafka-partition-revoked (mq-protocol:mq-error)
  ((topic :initarg :topic :reader kafka-revoked-topic)
   (partition :initarg :partition :reader kafka-revoked-partition)
   (offset :initarg :offset :reader kafka-revoked-offset))
  (:report (lambda (c s)
             (format s "Kafka: ~a" (mq-protocol:mq-error-message c))))
  (:documentation
   "ACK or NACK of a message whose partition a rebalance gave away since it
was fetched. Nothing is settled or committed: the partition's new owner may
have committed past the message, and it delivers the message again. The
CONTINUE restart returns from ACK / NACK. librdkafka's offsets store refuses
an unassigned partition the same way (Local: Erroneous state)."))

(defun %enter (session)
  "Count a call that uses SESSION's handles, per thread too. Signals
MQ-CONNECTION-ERROR when SESSION is closed."
  (unless (bt2:with-lock-held ((%session-state-lock session))
            (unless (%session-closed session)
              (incf (gethash (bt2:current-thread) (%session-user-threads session) 0))
              (incf (%session-users session))))
    (error 'mq-protocol:mq-connection-error :message "the Kafka backend is closed")))

(defun %leave (session)
  "End a call %ENTER counted. When CLOSE ran inside a call, the last call
to end, on whichever thread, releases the handles (%RELEASE-HANDLES)."
  (let ((deferred
          (bt2:with-lock-held ((%session-state-lock session))
            (let ((threads (%session-user-threads session))
                  (self (bt2:current-thread)))
              (when (zerop (decf (gethash self threads)))
                (remhash self threads))
              (decf (%session-users session))
              (bt2:condition-broadcast (%session-state-cv session))
              (when (zerop (%session-users session))
                (shiftf (%session-deferred-close session) nil))))))
    (when deferred
      (%release-handles session :abort (getf deferred :abort)))))

(defmacro %using-handles ((session) &body body)
  "Run BODY as a call that uses SESSION's librdkafka handles: CLOSE destroys
them only once no such call runs, and a CLOSE from inside such a call (a
handler of a condition BODY signals) leaves them to the call's end. Signals
MQ-CONNECTION-ERROR when SESSION is closed. An MQ-ERROR from BODY is
signalled again once BODY is done, so its handlers run with no lock held."
  (let ((s (gensym "SESSION"))
        (err (gensym "ERR"))
        (e (gensym "E"))
        (vals (gensym "VALUES")))
    `(let ((,s ,session)
           (,err nil)
           (,vals '()))
       (%enter ,s)
       (unwind-protect
           (setf ,vals (multiple-value-list
                        (handler-case (progn ,@body)
                          (mq-protocol:mq-error (,e) (setf ,err ,e) nil))))
         (%leave ,s))
       (if ,err
           (error ,err)
           (values-list ,vals)))))

(defvar *sessions* (make-hash-table)
  "Session id → RDKAFKA-SESSION with a consumer: the id is the rebalance
callback's opaque.")

(defvar *sessions-lock* (bt2:make-lock :name "rdkafka-sessions"))

(defvar *next-session-id* 0)

(defun %register-session (session)
  "Give SESSION an id in *SESSIONS*. → the id."
  (bt2:with-lock-held (*sessions-lock*)
    (let ((id (incf *next-session-id*)))
      (setf (gethash id *sessions*) session
            (%session-id session) id))))

(defun %session-by-id (id)
  (bt2:with-lock-held (*sessions-lock*)
    (values (gethash id *sessions*))))

(defun %forget-session (session)
  (bt2:with-lock-held (*sessions-lock*)
    (when (%session-id session)
      (remhash (%session-id session) *sessions*))))

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

(defun %make-handle (type properties &key rebalance-opaque)
  "rd_kafka_new of TYPE with PROPERTIES, an alist of strings. With
REBALANCE-OPAQUE (an integer), the consumer's rebalances go through
%REBALANCE-CB, which gets it as its opaque."
  (let ((conf (%rd-conf-new))
        (errstr (cffi:foreign-alloc :char :count 512)))
    (unwind-protect
        (progn
          (loop for (name . value) in properties
                do (%conf-set conf name value errstr))
          (when rebalance-opaque
            (%rd-conf-set-opaque conf (cffi:make-pointer rebalance-opaque))
            (%rd-conf-set-rebalance-cb conf (cffi:callback %rebalance-cb)))
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
  "SESSION's producer handle, made on first use. Read under the producer
lock too, so another thread never sees the handle before it is complete."
  (bt2:with-lock-held ((%session-producer-lock session))
    (or (%session-producer session)
        (setf (%session-producer session)
              (%make-handle +rd-kafka-producer+
                            (%properties session
                                         '("client.id" . "cl-stack-mq-backend-kafka")
                                         '("acks" . "all")
                                         '("linger.ms" . "5")
                                         '("allow.auto.create.topics" . "true")))))))

(defun %consumer (session)
  "SESSION's consumer handle, made on first use (under the session lock)."
  (or (%session-consumer session)
      (let ((id (%register-session session))
            (rk nil))
        (unwind-protect
            (progn
              (setf rk (%make-handle +rd-kafka-consumer+
                                     (%properties session
                                                  '("client.id" . "cl-stack-mq-backend-kafka")
                                                  (cons "group.id" (%session-group-id session))
                                                  '("enable.auto.commit" . "false")
                                                  '("auto.offset.reset" . "earliest")
                                                  '("enable.partition.eof" . "false")
                                                  '("allow.auto.create.topics" . "true"))
                                     :rebalance-opaque id))
              ;; Serve rebalances and errors from rd_kafka_consumer_poll.
              (%rd-poll-set-consumer rk)
              (setf (%session-consumer session) rk))
          (unless rk
            (%forget-session session))))))

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
                ;; The local queue may be full: serve delivery reports and
                ;; retry, until another thread closes the backend.
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
                             (when (%session-closed session)
                               (error 'mq-protocol:mq-connection-error
                                      :message "the Kafka backend is closed"))
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
  "The headers of RKMESSAGE as an alist. Names and values are UTF-8
strings, or octets when they are not UTF-8 (another producer may send any
bytes), so a header never fails the poll."
  (cffi:with-foreign-object (hdrsp :pointer)
    (setf (cffi:mem-ref hdrsp :pointer) (cffi:null-pointer))
    (when (zerop (%rd-message-headers rkmessage hdrsp))
      (let ((hdrs (cffi:mem-ref hdrsp :pointer)))
        (unless (cffi:null-pointer-p hdrs)
          (loop for i from 0 below (%rd-header-cnt hdrs)
                collect (cffi:with-foreign-objects ((namep :pointer) (valuep :pointer)
                                                    (sizep :size))
                          (%rd-header-get-all hdrs i namep valuep sizep)
                          (let ((name (cffi:mem-ref namep :pointer))
                                (value (cffi:mem-ref valuep :pointer)))
                            (cons (%from-octets
                                   (%foreign-octets name (loop for n from 0
                                                               until (zerop (cffi:mem-aref name :uint8 n))
                                                               finally (return n))))
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
offset); its delivery tag maps to META and the partition's assignment epoch
for ACK / NACK, and the session holds META until the message is settled.
The message is read first: the tag is registered only once it is built."
  (cffi:with-foreign-slots ((payload len key key-len) rkmessage (:struct rd-kafka-message))
    (let ((message (mq-protocol:make-mq-message
                    :key (and (not (cffi:null-pointer-p key)) (plusp key-len)
                              (%from-octets (%foreign-octets key key-len)))
                    :payload (if (cffi:null-pointer-p payload) #() (%foreign-octets payload len))
                    :headers (%read-headers rkmessage)
                    :topic (first meta)
                    :redelivered-p (and (gethash meta (%session-redeliver session)) t)))
          (tag (incf (%session-next-tag session))))
      (setf (mq-protocol:mq-message-delivery-tag message) tag
            (gethash tag (%session-inflight session))
            (append meta (list (%partition-epoch session (first meta) (second meta))))
            (gethash meta (%session-held session)) tag)
      message)))

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

(defun %tp-slots (tp)
  "(topic partition offset err) of an rd_kafka_topic_partition_t."
  (cffi:with-foreign-slots ((topic partition offset err) tp (:struct rd-kafka-topic-partition))
    (list (cffi:foreign-string-to-lisp topic :encoding :utf-8) partition offset err)))

(defun %partition-list (partitions)
  "The entries (%TP-SLOTS) of an rd_kafka_topic_partition_list_t."
  (cffi:with-foreign-slots ((cnt elems) partitions (:struct rd-kafka-topic-partition-list))
    (loop for i below cnt
          collect (%tp-slots (cffi:mem-aptr elems '(:struct rd-kafka-topic-partition) i)))))

(defun %group-committed (rk keys)
  "The group's committed offsets of KEYS ((topic partition) ...), asked of
the coordinator: an alist key → offset (RD_KAFKA_OFFSET_INVALID: none). A
partition the coordinator did not answer for is left out."
  (let ((list (%rd-tpl-new (length keys))))
    (unwind-protect
        (progn
          (loop for (topic partition) in keys
                do (%rd-tpl-add list topic partition))
          (when (zerop (%rd-committed rk list 10000))
            (loop for (topic partition offset err) in (%partition-list list)
                  when (zerop err)
                    collect (cons (list topic partition) offset))))
      (%rd-tpl-destroy list))))

(defun %partition-epoch (session topic partition)
  (gethash (list topic partition) (%session-epochs session) 0))

(defun %revoked-entry-p (session entry)
  "Was ENTRY's partition (an inflight entry: topic partition offset epoch)
revoked from this member since the message was fetched?"
  (destructuring-bind (topic partition offset epoch) entry
    (declare (ignore offset))
    (/= epoch (%partition-epoch session topic partition))))

(defun %revoke-for-good (session topic partition)
  "TOPIC / PARTITION left this member: another member may consume it and
commit past the messages this backend holds. Bump its epoch, so ACK and
NACK of those messages signal KAFKA-PARTITION-REVOKED and commit nothing,
and stop holding them, so a fetch of the same offsets after the partition
comes back hands them out again. Forget its buffered messages (never handed
out) and its settle state: when it comes back, it is consumed afresh from
the group's commit. Its pause is kept: librdkafka keeps the pause across
assignments, and polling TOPIC resumes it."
  (let ((key (list topic partition))
        (inflight (%session-inflight session)))
    (flet ((of-partition-p (meta)
             (and (equal (first meta) topic) (eql (second meta) partition))))
      (incf (gethash key (%session-epochs session) 0))
      (remhash key (%session-ownership session))
      (dolist (table (list (%session-held session) (%session-redeliver session)))
        (let ((drop '()))
          (maphash (lambda (meta value)
                     (declare (ignore value))
                     (when (of-partition-p meta) (push meta drop)))
                   table)
          (dolist (meta drop) (remhash meta table))))
      (setf (gethash topic (%session-buffered session))
            (remove-if (lambda (m)
                         (let ((tag (mq-protocol:mq-message-delivery-tag m)))
                           (when (of-partition-p (gethash tag inflight))
                             (remhash tag inflight)
                             t)))
                       (gethash topic (%session-buffered session))))
      (remhash key (%session-settled session))
      (remhash key (%session-requeued session))
      (remhash key (%session-committed session))
      (remhash key (%session-own-commits session)))))

(defun %revoke-all-for-good (session)
  "Every partition this member owns or has :PENDING left it for good
(%REVOKE-FOR-GOOD): the group no longer knows this member, or a rebalance
failed."
  (dolist (key (loop for key being the hash-keys of (%session-ownership session)
                     collect key))
    (%revoke-for-good session (first key) (second key))))

(defun %retained-p (session key group)
  "Is the group's commit of KEY (topic partition) unchanged since this
member last knew it (the known commit: at the assignment, or this member's
own commit since)? GROUP: the group's committed offsets now
(%GROUP-COMMITTED). An unknown value on either side: not retained."
  (let ((entry (assoc key group :test #'equal))
        (known (gethash key (%session-committed session))))
    (and entry known (eql (cdr entry) known))))

(defun %on-assign (session rk keys)
  "The group assigns KEYS ((topic partition) ...). Ask the group for their
commits (rd_kafka_committed). A partition this member had until the eager
revocation just before keeps its held messages when the group's commit is
the known one (%RETAINED-P); every other :PENDING partition left for good.
Then all of KEYS are :OWNED, and the group's commit is their known commit
(none when the group did not answer for one)."
  (let* ((ownership (%session-ownership session))
         (known (%session-committed session))
         (pending (loop for key being the hash-keys of ownership using (hash-value state)
                        when (eq state :pending) collect key))
         (group (and keys (%group-committed rk keys))))
    (dolist (key pending)
      (unless (and (member key keys :test #'equal)
                   (%retained-p session key group))
        (%revoke-for-good session (first key) (second key))))
    (dolist (key keys)
      (setf (gethash key ownership) :owned)
      (remhash key (%session-own-commits session))
      (let ((entry (assoc key group :test #'equal)))
        (if entry
            (setf (gethash key known) (cdr entry))
            (remhash key known))))))

(defun %apply-rebalance (rk err partitions cooperative)
  "Hand a rebalance back to librdkafka: assign or unassign PARTITIONS, the
whole assignment (eager) or incrementally (COOPERATIVE). Any other ERR, a
rebalance failure, clears the assignment."
  (cond
   ((and cooperative
         (member err (list +rd-kafka-resp-err-assign-partitions+
                           +rd-kafka-resp-err-revoke-partitions+)))
    (let* ((assign (= err +rd-kafka-resp-err-assign-partitions+))
           (e (if assign
                  (%rd-incremental-assign rk partitions)
                  (%rd-incremental-unassign rk partitions))))
      (unless (cffi:null-pointer-p e)
        (let ((text (%rd-error-string e)))
          (%rd-error-destroy e)
          (%kafka-error "rd_kafka_incremental_~:[un~;~]assign: ~a" assign text)))))
   (t
    (let ((rc (%rd-assign rk (if (= err +rd-kafka-resp-err-assign-partitions+)
                                 partitions
                                 (cffi:null-pointer)))))
      (unless (zerop rc)
        (%kafka-error "rd_kafka_assign: ~a" (%rd-err2str rc)))))))

(defun %on-rebalance (session rk err partitions)
  "The consumer's rebalance callback (%REBALANCE-CB), served inside
rd_kafka_consumer_poll on the polling thread (which holds the poll lock,
not the session lock) and inside rd_kafka_consumer_close. Under the session
lock, so no commit runs meanwhile: record which partitions this member
owns, then assign or revoke. An assignment learns the group's commits
(%ON-ASSIGN) and commits what was settled on every owned partition before
rd_kafka_assign / incremental_assign, so librdkafka fetches from past the
acks. A revocation commits what was settled first (a commit refused during
the rebalance is retried here). An eager revocation marks its partitions
:PENDING, and the assignment that follows decides. A cooperative or lost
revocation, or a rebalance failure, is for good (%REVOKE-FOR-GOOD). Nothing
unwinds into librdkafka: an error is kept for the poll to signal, and
librdkafka gets its assign call regardless."
  (let ((cooperative (equal (%rd-rebalance-protocol rk) "COOPERATIVE"))
        (applied nil))
    (flet ((apply-rebalance ()
             (unless applied
               (setf applied t)
               (%apply-rebalance rk err partitions cooperative))))
      (handler-case
          (if (null session)
              (apply-rebalance)
              (bt2:with-recursive-lock-held ((%session-lock session))
                (let ((keys (mapcar (lambda (entry) (subseq entry 0 2))
                                    (%partition-list partitions)))
                      (ownership (%session-ownership session)))
                  (cond
                    ((= err +rd-kafka-resp-err-assign-partitions+)
                     (%on-assign session rk keys)
                     (%commit-owned session)
                     (apply-rebalance))
                    ((and (= err +rd-kafka-resp-err-revoke-partitions+)
                          (zerop (%rd-assignment-lost rk)))
                     (%commit-owned session)
                     (dolist (key keys)
                       (cond
                         (cooperative (%revoke-for-good session (first key) (second key)))
                         ((gethash key ownership) (setf (gethash key ownership) :pending))))
                     (apply-rebalance))
                    ((= err +rd-kafka-resp-err-revoke-partitions+)
                     (dolist (key keys)
                       (%revoke-for-good session (first key) (second key)))
                     (apply-rebalance))
                    (t
                     (%revoke-all-for-good session)
                     (apply-rebalance))))))
        (error (e)
          (when session
            (bt2:with-recursive-lock-held ((%session-lock session))
              (setf (%session-callback-error session)
                    (if (typep e 'mq-protocol:mq-error)
                        e
                        (make-condition 'mq-protocol:mq-error
                                        :message (format nil "Kafka rebalance: ~a" e))))))
          (unless applied
            (handler-case (apply-rebalance)
              (mq-protocol:mq-error () nil))))))))

(cffi:defcallback %rebalance-cb :void
    ((rk :pointer) (err :int) (partitions :pointer) (opaque :pointer))
  (%on-rebalance (%session-by-id (cffi:pointer-address opaque)) rk err partitions))

(defun %take-in (session topic rkmessage)
  "Register a consumed RKMESSAGE, under the session lock. → its new
MQ-MESSAGE when it is of TOPIC. NIL for a consumer event, a message this
backend holds or has settled, one below the partition's known commit (a
fetch that started before this member's own commit), one of a partition
this member does not own (fetched before a revocation took effect), or one
of another topic: that one is buffered for its topic, and its partition
paused unless a thread polls that topic. The consumer event
MAX_POLL_EXCEEDED (librdkafka left the group for this member) revokes
every partition for good."
  ;; Non-zero ERR: a consumer event (rebalance in progress, unknown topic
  ;; before auto-create, ...) that librdkafka retries by itself; it carries
  ;; no message.
  (let ((err (cffi:foreign-slot-value rkmessage '(:struct rd-kafka-message) 'err)))
    (cond
      ((= err +rd-kafka-resp-err-max-poll-exceeded+)
       (%revoke-all-for-good session)
       nil)
      ((zerop err)
       (let* ((meta (%rkmessage-meta rkmessage))
              (of (first meta))
              (key (list of (second meta))))
         (cond
           ((gethash meta (%session-held session)) nil)
           ((%settled-p session meta) nil)
           ((not (eq (gethash key (%session-ownership session)) :owned))
            nil)
           ((< (third meta) (gethash key (%session-own-commits session) -1))
            nil)
           (t
            (%unrequeue session meta)
            (let ((message (%message session rkmessage meta)))
              (cond
                ((equal of topic) message)
                (t
                 (%buffer session of message)
                 (unless (plusp (gethash of (%session-polling session) 0))
                   (%pause-partition session of (second meta)))
                 nil))))))))))

(defun %fetch-one (session topic wait-ms)
  "One rd_kafka_consumer_poll of up to WAIT-MS ms, holding the poll lock
but not the session lock, so ACK and NACK on other threads go on meanwhile;
its message is registered (%TAKE-IN) before the next fetch. → the new
MQ-MESSAGE of TOPIC, or NIL. Signals an error the rebalance callback caught
during the poll (the message, if any, is kept for the next poll)."
  (let ((message nil)
        (err nil))
    (bt2:with-lock-held ((%session-poll-lock session))
      (let ((rkmessage (%rd-consumer-poll (%session-consumer session) wait-ms)))
        (unless (cffi:null-pointer-p rkmessage)
          (unwind-protect
              (bt2:with-recursive-lock-held ((%session-lock session))
                (setf message (%take-in session topic rkmessage)))
            (%rd-message-destroy rkmessage)))
        (bt2:with-recursive-lock-held ((%session-lock session))
          (setf err (shiftf (%session-callback-error session) nil))
          (when (and err message)
            (%hold-back session topic (list message))))))
    (when err
      (error err))
    message))

(defun %drop-revoked (session messages)
  "MESSAGES without those whose partition was revoked from this member since
they were fetched; those were never handed out, and are forgotten."
  (remove-if (lambda (m)
               (let* ((tag (mq-protocol:mq-message-delivery-tag m))
                      (entry (gethash tag (%session-inflight session))))
                 (when (or (null entry) (%revoked-entry-p session entry))
                   (remhash tag (%session-inflight session))
                   t)))
             messages))

(defun %hold-back (session topic messages)
  "Put MESSAGES (oldest first), fetched for a poll that did not return them,
back at the head of TOPIC's buffer."
  (when messages
    (setf (gethash topic (%session-buffered session))
          (append messages (gethash topic (%session-buffered session))))))

(defun %poll (session topic &key (n 1) timeout)
  "Up to N messages of TOPIC. TIMEOUT seconds (NIL: one 100 ms poll).
The consumer is subscribed to every topic polled so far, so a message of
another topic can arrive: it is buffered for that topic, oldest first, and
its partition paused until that topic is polled (unless another thread
polls it meanwhile). A message this backend still holds (handed out and not
settled, or buffered), or has settled past the group's commit, is not
handed out again when a rebalance or a seek fetches it a second time.
Each slice of at most 200 ms is one rd_kafka_consumer_poll without the
session lock (%FETCH-ONE), so ACK, NACK and PUBLISH on other threads never
wait for this poll, and CLOSE stops it at the next slice
(MQ-CONNECTION-ERROR). A message whose partition is revoked from this
member before the poll returns is not returned; one fetched by a poll that
signals is kept for the next poll of TOPIC."
  (let ((deadline (+ (get-internal-real-time)
                     (round (* (or timeout 0) internal-time-units-per-second))))
        ;; OUT is newest first until the final NREVERSE.
        (out '())
        (counted nil)
        (done nil))
    (unwind-protect
        (progn
          (%using-handles (session)
            (bt2:with-recursive-lock-held ((%session-lock session))
              (%ensure-subscribed session topic)
              (%resume-topic session topic)
              (incf (gethash topic (%session-polling session) 0))
              (setf counted t)))
          (loop
            (%using-handles (session)
              ;; Buffered for TOPIC, possibly by another thread's poll.
              (bt2:with-recursive-lock-held ((%session-lock session))
                (setf out (revappend (%take-buffered session topic (- n (length out))) out)))
              (when (or (>= (length out) n)
                        (and out (null timeout)))
                (return))
              (let* ((left (- deadline (get-internal-real-time)))
                     (wait-ms (if timeout
                                  (max 0 (min 200 (round (* 1000 left) internal-time-units-per-second)))
                                  100))
                     (message (%fetch-one session topic wait-ms)))
                (when message
                  (push message out))))
            (when (or (null timeout)
                      (>= (get-internal-real-time) deadline))
              (return))
            ;; Let another thread's poll of this backend take the next slice.
            (bt2:thread-yield))
          (setf done t))
      (bt2:with-recursive-lock-held ((%session-lock session))
        (when counted
          (when (zerop (decf (gethash topic (%session-polling session))))
            (remhash topic (%session-polling session))))
        (setf out (%drop-revoked session out))
        (unless done
          (%hold-back session topic (reverse out))
          (setf out '()))))
    (nreverse out)))

(defun %one-partition (topic partition offset)
  (let* ((list (%rd-tpl-new 1))
         (tp (%rd-tpl-add list topic partition)))
    (setf (cffi:foreign-slot-value tp '(:struct rd-kafka-topic-partition) 'offset) offset)
    list))

(defun %take-inflight (session message)
  "Forget MESSAGE (or a delivery tag): the session no longer holds it. → its
inflight entry (topic partition offset epoch)."
  (let* ((tag (if (integerp message) message (mq-protocol:mq-message-delivery-tag message)))
         (entry (gethash tag (%session-inflight session))))
    (unless entry
      (%kafka-error "unknown or already settled delivery tag ~s" tag))
    (remhash tag (%session-inflight session))
    ;; After a revocation for good the offset may be held again, by a
    ;; message fetched since: that one stays held.
    (let ((meta (subseq entry 0 3)))
      (when (eql (gethash meta (%session-held session)) tag)
        (remhash meta (%session-held session))))
    entry))

(defun %commit (session topic partition next)
  "Commit NEXT (the offset the group resumes at) for TOPIC / PARTITION,
synchronously. → librdkafka's error code: 0 when committed."
  (let ((list (%one-partition topic partition next)))
    (unwind-protect (%rd-commit (%consumer session) list 0)
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

(defun %known-commit (session key)
  "KEY's known commit as an offset, or NIL: none, or unknown."
  (let ((known (gethash key (%session-committed session))))
    (and known (>= known 0) known)))

(defun %assignment-lost-code-p (rc)
  "Does a commit refused with RC mean the group no longer knows this
member?"
  (or (= rc +rd-kafka-resp-err-unknown-member-id+)
      (= rc +rd-kafka-resp-err-fenced-instance-id+)))

(defun %rebalance-refusal-p (rc)
  "Was a commit refused with RC only because a rebalance is under way?"
  (or (= rc +rd-kafka-resp-err-rebalance-in-progress+)
      (= rc +rd-kafka-resp-err-illegal-generation+)))

(defun %commit-settled (session topic partition)
  "Commit TOPIC / PARTITION as far as its settled messages reach
(%COMMIT-TARGET), if this member owns it. → the offset committed (NIL:
none), and the error code of a commit librdkafka refused (NIL: none).
Committed: the target is the known commit and this member's own commit,
the settled offsets below it are dropped. Refused because a rebalance is
under way (REBALANCE_IN_PROGRESS, or ILLEGAL_GENERATION while librdkafka
still counts the assignment): the offsets stay settled, and the next
commit of the partition (an ack, or the rebalance callback) takes them.
Refused because the group no longer knows this member (UNKNOWN_MEMBER_ID,
FENCED_INSTANCE_ID, or ILLEGAL_GENERATION with the assignment lost): every
partition is revoked for good, and the code returned is UNKNOWN_MEMBER_ID
for an ILLEGAL_GENERATION."
  (let* ((key (list topic partition))
         (settled (gethash key (%session-settled session))))
    (when (and settled (eq (gethash key (%session-ownership session)) :owned))
      (let ((target (%commit-target settled
                                    (%unsettled-offsets session topic partition)
                                    (%known-commit session key))))
        (when target
          (let ((rc (%commit session topic partition target)))
            (when (and (= rc +rd-kafka-resp-err-illegal-generation+)
                       (/= 0 (%rd-assignment-lost (%consumer session))))
              (setf rc +rd-kafka-resp-err-unknown-member-id+))
            (cond
              ((zerop rc)
               (setf (gethash key (%session-own-commits session)) target
                     (gethash key (%session-committed session)) target
                     (gethash key (%session-settled session))
                     (remove-if (lambda (o) (< o target)) settled))
               target)
              ((%assignment-lost-code-p rc)
               (%revoke-all-for-good session)
               (values nil rc))
              (t (values nil rc)))))))))

(defun %commit-owned (session)
  "%COMMIT-SETTLED on every partition this member owns (the rebalance
callback: a commit refused during the rebalance is retried here)."
  (dolist (key (loop for key being the hash-keys of (%session-ownership session)
                       using (hash-value state)
                     when (eq state :owned) collect key))
    (%commit-settled session (first key) (second key))))

(defun %settle (session tag entry)
  "Settle the message of delivery TAG, whose inflight ENTRY (topic
partition offset epoch) %TAKE-INFLIGHT took, and commit as far as the
settled messages reach (%COMMIT-SETTLED). A message settled while an
earlier one is unsettled is committed once that one is settled; if this
member leaves first, the group's next member gets both again (at least
once). The commit never moves back. While an eager rebalance has the
partition revoked (:PENDING), or a commit is refused because a rebalance
is under way, the commit waits for the rebalance callback or the next ack.
→ NIL when settled, else what to signal once no lock is held:
(:REVOKED entry) when the commit found the assignment lost (the tag stays,
so acking it again says so too), or (:FAILED entry text) when librdkafka
refused the commit otherwise: the settle is undone and the tag restored,
so the caller may retry."
  (destructuring-bind (topic partition offset epoch) entry
    (declare (ignore epoch))
    (let* ((key (list topic partition))
           (meta (list topic partition offset))
           (redeliver (gethash meta (%session-redeliver session))))
      (remhash meta (%session-redeliver session))
      (push offset (gethash key (%session-settled session)))
      (multiple-value-bind (target rc) (%commit-settled session topic partition)
        (declare (ignore target))
        (cond
          ((or (null rc) (%rebalance-refusal-p rc)) nil)
          ((%assignment-lost-code-p rc)
           (setf (gethash tag (%session-inflight session)) entry)
           (list :revoked entry))
          (t
           (setf (gethash key (%session-settled session))
                 (remove offset (gethash key (%session-settled session)) :count 1)
                 (gethash tag (%session-inflight session)) entry
                 (gethash meta (%session-held session)) tag)
           (when redeliver
             (setf (gethash meta (%session-redeliver session)) t))
           (list :failed entry (format nil "rd_kafka_commit ~a[~d]: ~a"
                                       topic partition (%rd-err2str rc)))))))))

(defun %ack (session message)
  "Settle the message (%SETTLE): the group's commit moves past it once
every earlier message of its partition that this backend fetched is
settled too, so acks in any order never move the commit back. A message
whose partition was revoked from this member since it was fetched, or
whose commit finds that the group no longer knows this member, is not
settled and commits nothing: KAFKA-PARTITION-REVOKED, signalled with no
lock held (CONTINUE returns). A commit librdkafka refuses otherwise
signals MQ-ERROR with the message still unsettled, so ACK may be retried."
  (%signal-outcome
   (%using-handles (session)
     (bt2:with-recursive-lock-held ((%session-lock session))
       (let* ((tag (%delivery-tag message))
              (entry (%take-inflight session tag)))
         (if (%revoked-entry-p session entry)
             (list :revoked entry)
             (%settle session tag entry))))))
  message)

(defun %drop-buffered-after (session topic partition offset)
  "Forget the buffered messages of TOPIC / PARTITION past OFFSET: a seek to
OFFSET fetches them again, after the message at OFFSET."
  (flet ((later-p (m)
           (destructuring-bind (tp p o &rest epoch)
               (gethash (mq-protocol:mq-message-delivery-tag m) (%session-inflight session))
             (declare (ignore epoch))
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

(defun %requeue (session topic partition offset)
  "Seek TOPIC / PARTITION back to OFFSET, so the message there comes again,
marked redelivered, followed by the later messages of its partition that
this backend has not handed out (buffered ones are dropped and fetched
again, in order). Later messages still held, or already settled, are not
repeated. If an earlier message of the partition was requeued and not
fetched again yet, the seek goes back to that one. While an eager
rebalance has the partition revoked (:PENDING) there is no seek: when it
comes back it is fetched from the group's commit, which is not past the
requeued offsets."
  (let* ((key (list topic partition))
         (requeued (cons offset (gethash key (%session-requeued session))))
         (seek-to (reduce #'min requeued)))
    (setf (gethash key (%session-requeued session)) requeued)
    (%drop-buffered-after session topic partition seek-to)
    (when (member partition (gethash topic (%session-paused session)))
      ;; Fetch from the seek position now; a message of a topic that is not
      ;; being polled is buffered and paused again.
      (%partitions-call session #'%rd-resume-partitions "resume" topic (list partition))
      (setf (gethash topic (%session-paused session))
            (remove partition (gethash topic (%session-paused session)))))
    (when (eq (gethash key (%session-ownership session)) :owned)
      (let ((list (%one-partition topic partition seek-to)))
        (unwind-protect
            (let ((err (%rd-seek-partitions (%consumer session) list 5000)))
              (unless (cffi:null-pointer-p err)
                (let ((text (%rd-error-string err)))
                  (%rd-error-destroy err)
                  (%kafka-error "rd_kafka_seek_partitions ~a[~d]@~d: ~a"
                                topic partition seek-to text))))
          (%rd-tpl-destroy list))))
    (setf (gethash (list topic partition offset) (%session-redeliver session)) t)))

(defun %signal-revoked (entry)
  "Signal KAFKA-PARTITION-REVOKED for ENTRY (topic partition offset epoch).
CONTINUE returns NIL."
  (destructuring-bind (topic partition offset epoch) entry
    (declare (ignore epoch))
    (restart-case
        (error 'kafka-partition-revoked
               :topic topic :partition partition :offset offset
               :message (format nil "partition ~a[~d] was revoked from this member ~
                                     after the message at offset ~d was fetched: not ~
                                     settled, the partition's new owner delivers it"
                                topic partition offset))
      (continue ()
        :report "Return without settling the message."
        nil))))

(defun %delivery-tag (message)
  "MESSAGE's delivery tag (MESSAGE may be the tag itself)."
  (if (integerp message) message (mq-protocol:mq-message-delivery-tag message)))

(defun %signal-outcome (outcome)
  "Signal what %SETTLE (or a revoked entry) left for ACK / NACK to signal
once no lock is held: (:REVOKED entry) → KAFKA-PARTITION-REVOKED (CONTINUE
returns NIL); (:FAILED entry text) → MQ-ERROR. NIL: nothing."
  (case (first outcome)
    (:revoked (%signal-revoked (second outcome)))
    (:failed (%kafka-error "~a" (third outcome)))))

(defun %nack (session message &key (requeue t))
  "REQUEUE: seek the partition back to the message (%REQUEUE), so it is
delivered again, marked redelivered. Otherwise settle it like ACK
(%SETTLE). A message whose partition was revoked from this member since it
was fetched signals KAFKA-PARTITION-REVOKED instead, with no lock held
(CONTINUE returns): the partition's new owner delivers it."
  (%signal-outcome
   (%using-handles (session)
     (bt2:with-recursive-lock-held ((%session-lock session))
       (let* ((tag (%delivery-tag message))
              (entry (%take-inflight session tag)))
         (cond
           ((%revoked-entry-p session entry) (list :revoked entry))
           (requeue
            (destructuring-bind (topic partition offset epoch) entry
              (declare (ignore epoch))
              (%requeue session topic partition offset))
            nil)
           (t (%settle session tag entry)))))))
  message)

(defun %flush (session timeout)
  "Wait up to TIMEOUT seconds for queued messages to be delivered, in slices
of at most 200 ms, so a CLOSE on another thread waits at most one slice.
→ T when the queue drained; NIL on timeout, or once the backend is closed."
  (let ((rk (bt2:with-lock-held ((%session-producer-lock session))
              (%session-producer session)))
        (deadline (+ (get-internal-real-time)
                     (round (* timeout internal-time-units-per-second)))))
    (or (null rk)
        (loop
          (let ((left-ms (max 0 (round (* 1000 (- deadline (get-internal-real-time)))
                                       internal-time-units-per-second))))
            (when (zerop (%rd-flush rk (min 200 left-ms)))
              (return t))
            (when (or (zerop left-ms) (%session-closed session))
              (return nil)))))))

(defun %release-handles (session &key abort)
  "Leave the group, flush (unless ABORT) and destroy SESSION's handles,
once it is closed and no call uses them. → T when there were handles to
release."
  (let ((consumer (%session-consumer session))
        (producer (%session-producer session)))
    (setf (%session-producer session) nil)
    (unwind-protect
        (when consumer
          ;; Leaves the group; the last revocation runs %ON-REBALANCE here,
          ;; and its commits use the consumer handle, still in its slot.
          ;; From then on the callback no longer finds this session.
          (unwind-protect (%rd-consumer-close consumer)
            (%forget-session session)
            (setf (%session-consumer session) nil))
          (%rd-destroy consumer))
      (%forget-session session)
      (when producer
        (unless abort (%rd-flush producer 10000))
        (%rd-destroy producer)))
    (and (or consumer producer) t)))

(defun %close-session (session &key abort)
  "Close SESSION once no call uses its handles (%USING-HANDLES: a poll
stops at its next slice, a flush within 200 ms), then release them
(%RELEASE-HANDLES). Called on a thread that is itself inside a call (from
a handler of a condition the call signals, possibly with the session's
locks held), it waits for nothing: the last call to end, on whichever
thread, releases the handles. Calls from then on signal
MQ-CONNECTION-ERROR. → T when there were handles to release; NIL when
there were none, or SESSION was closed already."
  (let ((now
          (bt2:with-lock-held ((%session-state-lock session))
            (unless (%session-closed session)
              (setf (%session-closed session) t)
              (cond
                ((plusp (gethash (bt2:current-thread) (%session-user-threads session) 0))
                 (setf (%session-deferred-close session) (list :abort abort))
                 :deferred)
                (t
                 (loop while (plusp (%session-users session))
                       do (bt2:condition-wait (%session-state-cv session)
                                              (%session-state-lock session)))
                 :now))))))
    (case now
      (:now (%release-handles session :abort abort))
      (:deferred (and (or (%session-consumer session) (%session-producer session)) t))
      (t nil))))

(defun %topic-name (destination)
  (etypecase destination
    (string destination)
    (symbol (string-downcase (symbol-name destination)))))

(defun make-rdkafka-driver (&key (bootstrap-servers "localhost:9092") group-id config)
  "The librdkafka driver plist (:publish :poll :ack :nack :flush :close) for
MAKE-KAFKA-BACKEND. CONFIG is an alist of librdkafka properties
(\"security.protocol\" . \"SASL_SSL\") applied to both handles; GROUP-ID
defaults to a fresh one. Each operation may be called from any thread."
  (let ((session (make-rdkafka-session
                  :bootstrap bootstrap-servers
                  :group-id (or group-id
                                (format nil "cl-stack-~36r-~36r" (get-universal-time)
                                        (random (expt 2 40) (make-random-state t))))
                  :config config)))
    (list :publish (lambda (backend destination message)
                     (declare (ignore backend))
                     (%using-handles (session)
                       ;; The destination names the topic; a message polled
                       ;; from another topic keeps its own.
                       (%produce session
                                 (if destination
                                     (%topic-name destination)
                                     (mq-protocol:mq-message-topic message))
                                 message)))
          :poll (lambda (backend destination &rest keys)
                  (declare (ignore backend))
                  (apply #'%poll session (%topic-name destination) keys))
          :ack (lambda (backend message)
                 (declare (ignore backend))
                 (%ack session message))
          :nack (lambda (backend message &rest keys)
                  (declare (ignore backend))
                  (apply #'%nack session message keys))
          :flush (lambda (backend &optional (timeout 10))
                   (declare (ignore backend))
                   (%using-handles (session)
                     (%flush session timeout)))
          :close (lambda (backend &key abort)
                   (declare (ignore backend))
                   (%close-session session :abort abort))
          :group-id (%session-group-id session))))

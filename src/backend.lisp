(in-package #:mq-backend-kafka)

;;; The backend calls its DRIVER, a plist of functions. By default that is
;;; the librdkafka driver (driver.lisp); tests inject their own.

(defclass kafka-backend (mq-protocol:mq-backend)
  ((bootstrap-servers :initarg :bootstrap-servers
                      :accessor kafka-bootstrap-servers
                      :initform "localhost:9092")
   (cffi-library-path :initarg :cffi-library-path
                      :accessor kafka-cffi-library-path
                      :initform nil)
   (driver :initarg :driver :accessor kafka-driver :initform nil))
  (:documentation
   "mq-protocol backend for Kafka. DRIVER is a plist of function designators
(:publish :poll :ack :nack, optionally :subscribe :flush :close). By default
it is the librdkafka driver (MAKE-RDKAFKA-DRIVER); without librdkafka the
generic functions signal MQ-ERROR \"librdkafka not loaded\"."))

(defun %librdkafka-usable-p (path)
  "Is librdkafka loaded? With PATH, a library file to use when none is
loaded yet: a PATH that does not exist means no driver."
  (cond
   ((null path) (librdkafka-loaded-p))
   ((not (probe-file path)) nil)
   (t (unless (librdkafka-loaded-p)
        (cffi:load-foreign-library (truename path)))
      (librdkafka-loaded-p))))

(defun make-kafka-backend (&key (bootstrap-servers "localhost:9092")
                                cffi-library-path
                                driver group-id config)
  "A Kafka backend (confluent_kafka Producer + Consumer). Without DRIVER it
uses librdkafka (loaded with the system, from the native overlay):
GROUP-ID names the consumer group (default: a fresh one), CONFIG is an
alist of extra librdkafka properties. Nothing connects until first use;
CL:CLOSE flushes and releases the handles. CFFI-LIBRARY-PATH names a
librdkafka to load when the overlay's is missing."
  (make-instance 'kafka-backend
                 :bootstrap-servers bootstrap-servers
                 :cffi-library-path cffi-library-path
                 :driver (or driver
                             (and (%librdkafka-usable-p cffi-library-path)
                                  (make-rdkafka-driver :bootstrap-servers bootstrap-servers
                                                       :group-id group-id
                                                       :config config)))))

(defun kafka-group-id (backend)
  "The consumer group of BACKEND's librdkafka driver, or NIL."
  (getf (kafka-driver backend) :group-id))

(defmethod close ((backend kafka-backend) &key abort)
  "Flush (unless ABORT) and release the driver's handles. → T if it held any."
  (let ((fn (getf (kafka-driver backend) :close)))
    (and fn (funcall fn backend :abort abort))))

(defun kafka-driver-loaded-p (backend)
  (and (kafka-driver backend) t))

(defmethod mq-protocol:mq-backend-capabilities ((backend kafka-backend))
  (mq-protocol:make-mq-capabilities
   :publish-p t
   ;; Push subscribe needs a driver that has it; the librdkafka driver pulls.
   :subscribe-p (and (getf (kafka-driver backend) :subscribe) t)
   :poll-p t
   :ack-p t
   :nack-p t
   :transaction-p nil
   :delivery-semantics :at-least-once))

(defun %resolve-fn (designator)
  (cond
    ((null designator) nil)
    ((functionp designator) designator)
    ((and (symbolp designator) (fboundp designator))
     (fdefinition designator))
    ((and (consp designator) (fboundp (car designator)))
     (fdefinition (car designator)))
    (t
     (error 'mq-protocol:mq-error
            :message (format nil "invalid kafka driver fn ~s" designator)))))

(defun %driver-fn (backend op)
  (let* ((driver (kafka-driver backend))
         (fn (and driver (%resolve-fn (getf driver op)))))
    (or fn
        (restart-case
            (error 'mq-protocol:mq-error
                   :backend backend
                   :message (if driver
                                (format nil "the Kafka driver does not support ~(~a~)" op)
                                "librdkafka not loaded"))
          (use-value (value)
            :report "Use a supplied driver function or result"
            value)
          (retry ()
            :report "Retry after installing a driver"
            (%driver-fn backend op))))))

(defun %call-driver (backend op &rest args)
  (let ((fn (%driver-fn backend op)))
    (if (functionp fn)
        (apply fn backend args)
        fn)))

(defun kafka-flush (backend &key (timeout 10))
  "Wait up to TIMEOUT seconds until every published message is delivered.
→ T when the producer queue drained."
  (%call-driver backend :flush timeout))

(defmethod mq-protocol:publish ((backend kafka-backend) destination message &key)
  (%call-driver backend :publish destination message))

(defmethod mq-protocol:subscribe ((backend kafka-backend) destination handler &key)
  (%call-driver backend :subscribe destination handler))

(defmethod mq-protocol:poll-messages ((backend kafka-backend) destination
                                      &key (n 1) timeout)
  (%call-driver backend :poll destination :n n :timeout timeout))

(defmethod mq-protocol:ack ((backend kafka-backend) message &key)
  (%call-driver backend :ack message))

(defmethod mq-protocol:nack ((backend kafka-backend) message &key (requeue t))
  (%call-driver backend :nack message :requeue requeue))

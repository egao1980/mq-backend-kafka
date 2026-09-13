(in-package #:mq-backend-kafka)

;;; Skeleton. Real librdkafka CFFI is a wave-1 follow-up (native overlay +
;;; absolute-path LOAD-FOREIGN-LIBRARY). This file never calls
;;; LOAD-FOREIGN-LIBRARY — missing .so must not break CI.

(defclass kafka-backend (mq-protocol:mq-backend)
  ((bootstrap-servers :initarg :bootstrap-servers
                      :accessor kafka-bootstrap-servers
                      :initform "localhost:9092")
   (cffi-library-path :initarg :cffi-library-path
                      :accessor kafka-cffi-library-path
                      :initform nil)
   (driver :initarg :driver :accessor kafka-driver :initform nil))
  (:documentation
   "mq-protocol backend for Kafka. DRIVER is a plist of function
designators (:publish :subscribe :poll :ack :nack). Without a driver,
GFs signal MQ-ERROR \"librdkafka not loaded\"."))

(defun make-kafka-backend (&key (bootstrap-servers "localhost:9092")
                             cffi-library-path
                             driver)
  (make-instance 'kafka-backend
                 :bootstrap-servers bootstrap-servers
                 :cffi-library-path cffi-library-path
                 :driver driver))

(defun kafka-driver-loaded-p (backend)
  (and (kafka-driver backend) t))

(defmethod mq-protocol:mq-backend-capabilities ((backend kafka-backend))
  (mq-protocol:make-mq-capabilities
   :publish-p t
   :subscribe-p t
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
                   :message "librdkafka not loaded")
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

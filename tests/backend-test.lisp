(in-package #:mq-backend-kafka/tests)

(deftest missing-driver-signals
  (let ((b (mq-backend-kafka:make-kafka-backend
            :bootstrap-servers "localhost:9092"
            :cffi-library-path "/no/such/librdkafka.so")))
    (ok (equal "localhost:9092" (mq-backend-kafka:kafka-bootstrap-servers b)))
    (ok (equal "/no/such/librdkafka.so"
               (mq-backend-kafka:kafka-cffi-library-path b)))
    (ng (mq-backend-kafka:kafka-driver-loaded-p b))
    (ok (signals (mq-protocol:publish
                  b "t" (mq-protocol:make-mq-message :payload #(1)))
                 'mq-protocol:mq-error))
    (ok (signals (mq-protocol:poll-messages b "t")
                 'mq-protocol:mq-error))))

(deftest missing-driver-message
  (let ((b (mq-backend-kafka:make-kafka-backend :cffi-library-path "/no/such/librdkafka.so"))
        (msg nil))
    (handler-case (mq-protocol:publish
                   b "t" (mq-protocol:make-mq-message :payload #(1)))
      (mq-protocol:mq-error (c)
        (setf msg (mq-protocol:mq-error-message c))))
    (ok (search "librdkafka not loaded" msg))))

(deftest fake-driver-publish-poll
  (let* ((published nil)
         (acked nil)
         (driver
          (list :publish
                (lambda (backend dest message)
                  (declare (ignore backend))
                  (push (list dest message) published)
                  message)
                :poll
                (lambda (backend dest &key n timeout)
                  (declare (ignore backend dest n timeout))
                  (mapcar #'second (reverse published)))
                :ack
                (lambda (backend message)
                  (declare (ignore backend))
                  (push message acked)
                  message)
                :subscribe
                (lambda (backend dest handler)
                  (declare (ignore backend dest handler))
                  :subscribed)
                :nack
                (lambda (backend message &key requeue)
                  (declare (ignore backend requeue))
                  message)))
         (b (mq-backend-kafka:make-kafka-backend
             :bootstrap-servers "redpanda:9092"
             :driver driver))
         (msg (mq-protocol:make-mq-message :key "k" :payload #(9 8 7))))
    (ok (mq-backend-kafka:kafka-driver-loaded-p b))
    (ok (eq msg (mq-protocol:publish b "orders" msg)))
    (let ((got (mq-protocol:poll-messages b "orders")))
      (ok (= 1 (length got)))
      (ok (eq msg (first got)))
      (ok (equalp #(9 8 7) (mq-protocol:mq-message-payload (first got)))))
    (ok (eq msg (mq-protocol:ack b msg)))
    (ok (eq msg (first acked)))
    (ok (eq :subscribed (mq-protocol:subscribe b "orders" #'identity)))))

(deftest missing-driver-use-value
  (let ((b (mq-backend-kafka:make-kafka-backend :cffi-library-path "/no/such/librdkafka.so"))
        (got :unset))
    (handler-bind ((mq-protocol:mq-error
                    (lambda (c)
                      (use-value :injected c))))
      (setf got (mq-protocol:publish
                 b "t" (mq-protocol:make-mq-message :payload #(1)))))
    (ok (eq :injected got))))

(deftest kafka-capabilities
  (let ((caps (mq-protocol:mq-backend-capabilities
               (mq-backend-kafka:make-kafka-backend))))
    (ok (mq-protocol:mq-capability-supported-p caps :publish))
    (ok (mq-protocol:mq-capability-supported-p caps :poll))
    (ok (eq :at-least-once
            (mq-protocol:mq-capability-delivery-semantics caps)))))

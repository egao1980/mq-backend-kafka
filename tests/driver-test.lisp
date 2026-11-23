(in-package #:mq-backend-kafka/tests)

;;; Evidence for egao1980/mq-backend-kafka#1: librdkafka overlays + the
;;; CFFI driver used by default, consumer groups, offset commit via ack.
;;;
;;; Spec:
;;;  - (asdf:load-system "mq-backend-kafka") loads librdkafka (native
;;;    overlay, absolute-path load at ASDF load), so rd_kafka_version
;;;    resolves with no extra call;
;;;  - make-kafka-backend without :driver has a working driver;
;;;  - :group-id names the consumer group, ack commits the offset, an
;;;    unacked message is redelivered to the next member of the group
;;;    (at-least-once, as mq-backend-capabilities says);
;;;  - cl:close releases the native handles.
;;;
;;; Live tests need a Kafka broker and skip without it:
;;;   docker run -d --name kafka-test -p 127.0.0.1:19094:9092 \
;;;     -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
;;;     -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
;;;     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19094 \
;;;     -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
;;;     -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
;;;     -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
;;;     -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
;;;     -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka-native:3.9.0
;;; Override with MQ_KAFKA_TEST_BOOTSTRAP (host:port).

(defun %bootstrap () 
  (let ((v (uiop:getenv "MQ_KAFKA_TEST_BOOTSTRAP")))
    (if (and v (plusp (length v))) v "127.0.0.1:19094")))

(defun %broker-up-p ()
  (let* ((bs (%bootstrap))
         (colon (position #\: bs :from-end t))
         (s (handler-case
                (usocket:socket-connect (subseq bs 0 colon)
                                        (parse-integer bs :start (1+ colon))
                                        :timeout 2)
              (error () nil))))
    (when s (usocket:socket-close s) t)))

(defmacro with-broker (() &body body)
  `(if (%broker-up-p)
       (progn ,@body)
       (skip (format nil "no Kafka at ~A (see tests/driver-test.lisp)" (%bootstrap)))))

(defun %backend (&rest args)
  "MAKE-KAFKA-BACKEND through APPLY: an unknown keyword fails this test at
run time instead of warning at compile time."
  (apply #'mq-backend-kafka:make-kafka-backend args))

(defun %name (tag)
  (format nil "evidence-~A-~D-~D" tag (get-universal-time) (random 1000000)))

(defun %octets (string) (map '(vector (unsigned-byte 8)) #'char-code string))

(defun %payloads (messages)
  (mapcar (lambda (m) (map 'string #'code-char (mq-protocol:mq-message-payload m)))
          messages))

(deftest librdkafka-loaded-by-asdf-load
  (ok (cffi:foreign-symbol-pointer "rd_kafka_version")
      "rd_kafka_version resolves after (asdf:load-system \"mq-backend-kafka\")"))

(deftest default-backend-has-a-driver
  (let ((b (mq-backend-kafka:make-kafka-backend :bootstrap-servers (%bootstrap))))
    (unwind-protect
         (ok (mq-backend-kafka:kafka-driver-loaded-p b)
             "make-kafka-backend without :driver uses the librdkafka driver")
      (ignore-errors (close b)))))

(deftest publish-poll-round-trip
  (with-broker ()
    (let ((topic (%name "rt"))
          (b (%backend :bootstrap-servers (%bootstrap) :group-id (%name "g"))))
      (unwind-protect
           (progn
             (mq-protocol:publish b topic (mq-protocol:make-mq-message
                                           :key "k1" :payload "order-1"))
             (let ((msgs (mq-protocol:poll-messages b topic :n 1 :timeout 20)))
               (ok (equal '("order-1") (%payloads msgs))
                   (format nil "polled back the published payload (got ~S)"
                           (%payloads msgs)))))
        (close b)))))

(deftest ack-commits-offset-unacked-is-redelivered
  (with-broker ()
    (let ((topic (%name "grp"))
          (group (%name "g")))
      ;; m1, m2 on a fresh topic.
      (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "p"))))
        (unwind-protect
             (dolist (s '("m1" "m2"))
               (mq-protocol:publish p topic (mq-protocol:make-mq-message :payload s)))
          (close p)))
      ;; Member 1 reads m1 and acks it.
      (let ((b1 (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
             (let ((msgs (mq-protocol:poll-messages b1 topic :n 1 :timeout 20)))
               (ok (equal '("m1") (%payloads msgs)) "member 1 gets m1")
               (when msgs (mq-protocol:ack b1 (first msgs))))
          (close b1)))
      ;; Member 2 resumes after the committed m1 and reads m2, unacked.
      (let ((b2 (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
             (ok (equal '("m2") (%payloads (mq-protocol:poll-messages
                                            b2 topic :n 1 :timeout 20)))
                 "member 2 resumes at m2: the ack committed m1")
          (close b2)))
      ;; Member 3: m2 was never acked, so it comes back.
      (let ((b3 (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
             (ok (equal '("m2") (%payloads (mq-protocol:poll-messages
                                            b3 topic :n 1 :timeout 20)))
                 "member 3 gets m2 again: unacked is redelivered")
          (close b3))))))

(in-package #:mq-backend-kafka/tests)

;;; The librdkafka driver beyond the evidence tests: what needs no broker
;;; (config errors, closed backends, octet decoding, group ids), then keys,
;;; headers, nack with requeue and flush against the Kafka of driver-test.lisp.

(deftest librdkafka-version-string
  (let ((v (mq-backend-kafka:librdkafka-version)))
    (ok (and (stringp v) (eql 0 (search "2." v)))
        (format nil "librdkafka ~A is loaded" v))))

(deftest group-id-default-and-explicit
  (let ((a (%backend :bootstrap-servers (%bootstrap)))
        (b (%backend :bootstrap-servers (%bootstrap)))
        (c (%backend :bootstrap-servers (%bootstrap) :group-id "orders-workers")))
    (ok (stringp (mq-backend-kafka:kafka-group-id a)))
    (ok (string/= (mq-backend-kafka:kafka-group-id a) (mq-backend-kafka:kafka-group-id b))
        "each backend gets its own group by default")
    (ok (equal "orders-workers" (mq-backend-kafka:kafka-group-id c)))
    (ok (null (close a)) "closing a backend that never connected releases nothing")))

(deftest bad-config-signals-mq-error
  (let ((b (%backend :bootstrap-servers (%bootstrap)
                     :config '(("no.such.property" . "1")))))
    (unwind-protect
         (let ((e (handler-case
                      (progn (mq-protocol:publish b "t" (mq-protocol:make-mq-message :payload "x"))
                             nil)
                    (mq-protocol:mq-error (e) e))))
           (ok e "an unknown librdkafka property is an mq-error")
           (ok (and e (search "no.such.property" (princ-to-string e)))))
      (close b))))

(deftest closed-backend-signals-connection-error
  (let ((b (%backend :bootstrap-servers (%bootstrap))))
    (close b)
    (ok (signals (mq-protocol:publish b "t" (mq-protocol:make-mq-message :payload "x"))
                 'mq-protocol:mq-connection-error))))

(deftest binary-keys-stay-octets
  (ok (equal "ключ" (mq-backend-kafka::%from-octets
                      (mq-backend-kafka::%to-octets "ключ"))))
  (ok (equalp #(255 0 254) (mq-backend-kafka::%from-octets
                            (coerce #(255 0 254) '(vector (unsigned-byte 8)))))
      "invalid UTF-8 comes back as octets"))

(deftest unknown-delivery-tag-signals
  (let ((b (%backend :bootstrap-servers (%bootstrap))))
    (unwind-protect
         (ok (signals (mq-protocol:ack b 424242) 'mq-protocol:mq-error))
      (close b))))

(deftest librdkafka-driver-has-no-push-subscribe
  (let ((b (%backend :bootstrap-servers (%bootstrap))))
    (ok (not (mq-protocol:mq-capability-supported-p
              (mq-protocol:mq-backend-capabilities b) :subscribe))
        "capabilities say pull only")
    (let ((e (handler-case (progn (mq-protocol:subscribe b "t" #'identity) nil)
               (mq-protocol:mq-error (e) e))))
      (ok (and e (search "does not support subscribe" (princ-to-string e)))
          (format nil "~A" e)))))

(deftest keys-headers-and-order
  (with-broker ()
    (let ((topic (%name "kh"))
          (b (%backend :bootstrap-servers (%bootstrap))))
      (unwind-protect
           (progn
             (dotimes (i 5)
               (mq-protocol:publish b topic (mq-protocol:make-mq-message
                                             :key "ключ-1" :payload (format nil "m~D" i)
                                             :headers (list (cons "seq" (princ-to-string i))
                                                            (cons "src" "cl")))))
             (ok (mq-backend-kafka:kafka-flush b :timeout 10) "flush drains the producer")
             (let ((msgs (mq-protocol:poll-messages b topic :n 5 :timeout 20)))
               (ok (equal '("m0" "m1" "m2" "m3" "m4") (%payloads msgs)) "one key keeps order")
               (ok (every (lambda (m) (equal "ключ-1" (mq-protocol:mq-message-key m))) msgs)
                   "UTF-8 key round-trips")
               (ok (equal '("0" "1" "2" "3" "4")
                          (mapcar (lambda (m) (cdr (assoc "seq" (mq-protocol:mq-message-headers m)
                                                          :test #'equal)))
                                  msgs))
                   "headers round-trip")
               (ok (every (lambda (m) (equal topic (mq-protocol:mq-message-topic m))) msgs))
               (dolist (m msgs) (mq-protocol:ack b m))))
        (close b)))))

(deftest nack-requeue-redelivers
  (with-broker ()
    (let ((topic (%name "nack"))
          (b (%backend :bootstrap-servers (%bootstrap))))
      (unwind-protect
           (progn
             (mq-protocol:publish b topic (mq-protocol:make-mq-message :payload "again"))
             (let ((first (first (mq-protocol:poll-messages b topic :timeout 20))))
               (ok (and first (not (mq-protocol:mq-message-redelivered-p first))))
               (when first
                 (mq-protocol:nack b first :requeue t)
                 (let ((second (first (mq-protocol:poll-messages b topic :timeout 20))))
                   (ok (equal '("again") (%payloads (and second (list second))))
                       "nack :requeue t delivers it again")
                   (ok (and second (mq-protocol:mq-message-redelivered-p second))
                       "marked redelivered")
                   (when second (mq-protocol:ack b second))))))
        (close b)))))

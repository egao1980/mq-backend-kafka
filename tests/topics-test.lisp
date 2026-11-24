(in-package #:mq-backend-kafka/tests)

;;; Evidence against PR #2 (fix for #1): POLL-MESSAGES is "pull up to N
;;; messages from DESTINATION" (mq-protocol). The driver's %POLL adds the
;;; topic to the consumer's subscription and then hands out whatever
;;; rd_kafka_consumer_poll returns: messages of every topic the backend
;;; was ever polled for. Polling topic B returns topic A's messages.

(deftest poll-returns-only-the-requested-topic
  (with-broker ()
    (let ((topic-a (%name "a"))
          (topic-b (%name "b")))
      (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "p"))))
        (unwind-protect
             (progn
               (dolist (s '("a1" "a2" "a3"))
                 (mq-protocol:publish p topic-a (mq-protocol:make-mq-message :payload s)))
               (mq-protocol:publish p topic-b (mq-protocol:make-mq-message :payload "b1")))
          ;; CLOSE flushes the producer.
          (close p)))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id (%name "g"))))
        (unwind-protect
             (let ((from-a (mq-protocol:poll-messages c topic-a :n 1 :timeout 20)))
               (ok (equal '("a1") (%payloads from-a)) "topic A gives a1")
               (let* ((from-b (mq-protocol:poll-messages c topic-b :n 4 :timeout 10))
                      (topics (mapcar #'mq-protocol:mq-message-topic from-b)))
                 (ok (and from-b (every (lambda (tp) (equal tp topic-b)) topics))
                     (format nil "poll-messages on B returns B's messages only (payloads ~S)"
                             (%payloads from-b)))))
          (close c))))))

(deftest switching-topics-keeps-order-and-repeats-nothing
  ;; a1 is held (not acked) while B is polled: adding B to the subscription
  ;; must not bring a1 back, A's messages fetched meanwhile wait for A in
  ;; order, and B's poll sees B only.
  (with-broker ()
    (let ((topic-a (%name "sa"))
          (topic-b (%name "sb")))
      (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "sp"))))
        (unwind-protect
            (progn
              (dolist (s '("a1" "a2" "a3"))
                (mq-protocol:publish p topic-a (mq-protocol:make-mq-message :payload s)))
              (dolist (s '("b1" "b2"))
                (mq-protocol:publish p topic-b (mq-protocol:make-mq-message :payload s))))
          (close p)))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id (%name "sg"))))
        (unwind-protect
            (let* ((a1 (mq-protocol:poll-messages c topic-a :n 1 :timeout 20))
                   (bs (mq-protocol:poll-messages c topic-b :n 2 :timeout 20))
                   (more-b (mq-protocol:poll-messages c topic-b :n 1 :timeout 2))
                   (rest-a (mq-protocol:poll-messages c topic-a :n 2 :timeout 20))
                   (again-a (mq-protocol:poll-messages c topic-a :n 1 :timeout 3)))
              (ok (equal '("a1") (%payloads a1)) "A gives a1")
              (ok (equal '("b1" "b2") (%payloads bs))
                  (format nil "B gives b1 b2 only (got ~S)" (%payloads bs)))
              (ok (null more-b) (format nil "nothing else on B (got ~S)" (%payloads more-b)))
              (ok (equal '("a2" "a3") (%payloads rest-a))
                  (format nil "back on A: a2 a3 in order, a1 not repeated (got ~S)"
                          (%payloads rest-a)))
              (ok (null again-a)
                  (format nil "a1 is held, not handed out again (got ~S)" (%payloads again-a)))
              (ok (notany #'mq-protocol:mq-message-redelivered-p (append a1 bs rest-a))
                  "nothing is marked redelivered")
              (dolist (m (append a1 bs rest-a)) (mq-protocol:ack c m)))
          (close c))))))

(deftest nack-after-switching-topics-redelivers-in-order
  ;; A's partition is paused (B is polled) when a1 is nacked: a1 comes
  ;; back first, then a2 and a3, once A is polled again.
  (with-broker ()
    (let ((topic-a (%name "na"))
          (topic-b (%name "nb")))
      (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "np"))))
        (unwind-protect
            (progn
              (dolist (s '("a1" "a2" "a3"))
                (mq-protocol:publish p topic-a (mq-protocol:make-mq-message :payload s)))
              (mq-protocol:publish p topic-b (mq-protocol:make-mq-message :payload "b1")))
          (close p)))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id (%name "ng"))))
        (unwind-protect
            (let* ((a1 (first (mq-protocol:poll-messages c topic-a :n 1 :timeout 20)))
                   (bs (mq-protocol:poll-messages c topic-b :n 1 :timeout 20)))
              (ok (equal '("b1") (%payloads bs)) "B gives b1")
              (mq-protocol:nack c a1 :requeue t)
              (let ((as (mq-protocol:poll-messages c topic-a :n 3 :timeout 20)))
                (ok (equal '("a1" "a2" "a3") (%payloads as))
                    (format nil "a1 again, then a2 a3 (got ~S)" (%payloads as)))
                (ok (and as (mq-protocol:mq-message-redelivered-p (first as))
                         (notany #'mq-protocol:mq-message-redelivered-p (rest as)))
                    "only a1 is marked redelivered")
                (dolist (m (append bs as)) (mq-protocol:ack c m))))
          (close c))))))

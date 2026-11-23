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

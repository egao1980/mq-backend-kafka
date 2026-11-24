(in-package #:mq-backend-kafka/tests)

;;; Round-3 evidence against PR #2: one thread's POLL-MESSAGES :TIMEOUT holds
;;; the session lock for the whole timeout, so ACK and PUBLISH from any
;;; other thread wait until that poll returns.
;;;
;;; The usual worker shape (confluent-kafka: a consumer thread polls, worker
;;; threads commit, a request thread produces) stalls: every ack and every
;;; publish waits up to the poller's timeout. librdkafka handles are
;;; thread-safe, and in confluent-kafka-python Producer.produce and
;;; Consumer.commit do not wait for another thread's Consumer.poll.
;;; mq-backend-amqp's poll-messages takes its lock per attempt and sleeps
;;; outside it.

(defun %ms-since (start)
  (round (* 1000 (- (get-internal-real-time) start)) internal-time-units-per-second))

(deftest ack-and-publish-do-not-wait-for-another-threads-poll
  (with-broker ()
    (let ((topic (%name "r3-lock"))
          (other (%name "r3-lock-out"))
          (b (%backend :bootstrap-servers (%bootstrap) :group-id (%name "r3-lockg"))))
      (flet ((timed-while-polling (fn)
               ;; Milliseconds FN takes while another thread runs a
               ;; poll-messages :timeout 4 on the (now empty) topic.
               (let ((poller (bt2:make-thread
                              (lambda () (mq-protocol:poll-messages b topic :timeout 4))
                              :name "poller")))
                 (sleep 0.3)
                 (let ((start (get-internal-real-time)))
                   (funcall fn)
                   (prog1 (%ms-since start)
                     (bt2:join-thread poller))))))
        (unwind-protect
            (progn
              (mq-protocol:publish b topic (mq-protocol:make-mq-message :payload "m1"))
              (mq-backend-kafka:kafka-flush b)
              (let ((m1 (first (mq-protocol:poll-messages b topic :timeout 20))))
                (ok m1 "m1 polled: the consumer is assigned")
                (let ((publish-ms (timed-while-polling
                                   (lambda ()
                                     (mq-protocol:publish
                                      b other (mq-protocol:make-mq-message :payload "x")))))
                      (ack-ms (timed-while-polling (lambda () (mq-protocol:ack b m1)))))
                  (ok (< publish-ms 1000)
                      (format nil "publish from another thread took ~D ms while a poll :timeout 4 ran"
                              publish-ms))
                  (ok (< ack-ms 1000)
                      (format nil "ack from another thread took ~D ms while a poll :timeout 4 ran"
                              ack-ms)))))
          (close b))))))

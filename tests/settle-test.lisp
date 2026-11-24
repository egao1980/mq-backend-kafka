(in-package #:mq-backend-kafka/tests)

;;; Round 3 (#1): settlement in any order. ACK and NACK settle one message,
;;; as in RabbitMQ and the in-memory backend; the group's commit moves to
;;; the earliest message still unsettled, never back.

(deftest-parametrize commit-target-table
    ((settled unsettled committed want)
     ;; ack a2 while a1 (offset 0) is out: nothing to commit yet.
     ('(1) '(0) nil nil)
     ;; then ack a1: both settled, a3 not fetched: resume at 2.
     ('(0 1) '() nil 2)
     ;; ack a1 while a2 is out: resume at a2.
     ('(0) '(1) nil 1)
     ;; the commit never moves back.
     ('(3) '() 5 nil)
     ;; a later ack past the last commit moves it on.
     ('(5 6) '() 5 7)
     ;; settled offsets past an unsettled one wait for it.
     ('(4 6 7) '(5) 4 5))
  (ok (eql want (mq-backend-kafka::%commit-target settled unsettled committed))
      (format nil "settled ~S unsettled ~S committed ~S → ~S" settled unsettled committed want)))

(defun %publish-each (topic payloads)
  (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "r3p"))))
    (unwind-protect
        (dolist (s payloads)
          (mq-protocol:publish p topic (mq-protocol:make-mq-message :payload s)))
      (close p))))

(deftest live-ack-later-only-then-leave-redelivers-both
  ;; a2 acked, a1 never settled: the commit stays before a1, so the next
  ;; member gets a1 and a2 again (at least once).
  (with-broker ()
    (let ((topic (%name "r3-al"))
          (group (%name "r3-alg")))
      (%publish-each topic '("a1" "a2" "a3"))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let ((got (mq-protocol:poll-messages c topic :n 2 :timeout 20)))
              (ok (equal '("a1" "a2") (%payloads got)))
              (when (= 2 (length got))
                (mq-protocol:ack c (second got))))
          (close c)))
      (let ((c2 (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let ((next (mq-protocol:poll-messages c2 topic :n 3 :timeout 15)))
              (ok (equal '("a1" "a2" "a3") (%payloads next))
                  (format nil "a1 was never settled: the next member starts there (got ~S)"
                          (%payloads next)))
              (dolist (m next) (mq-protocol:ack c2 m)))
          (close c2))))))

(deftest live-two-nacks-in-a-row-redeliver-both
  ;; a1 then a2 nacked with requeue. Before round 3 the second seek went
  ;; to a2 and skipped a1, which was requeued but not fetched again.
  (with-broker ()
    (let ((topic (%name "r3-nn")))
      (%publish-each topic '("a1" "a2" "a3"))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id (%name "r3-nng"))))
        (unwind-protect
            (let ((got (mq-protocol:poll-messages c topic :n 2 :timeout 20)))
              (ok (equal '("a1" "a2") (%payloads got)))
              (when (= 2 (length got))
                (mq-protocol:nack c (first got) :requeue t)
                (mq-protocol:nack c (second got) :requeue t)
                (let ((again (mq-protocol:poll-messages c topic :n 3 :timeout 10)))
                  (ok (equal '("a1" "a2" "a3") (%payloads again))
                      (format nil "a1, a2 again, then a3 (got ~S)" (%payloads again)))
                  (ok (equal '(t t nil) (mapcar (lambda (m)
                                                  (and (mq-protocol:mq-message-redelivered-p m) t))
                                                again))
                      "a1 and a2 are marked redelivered")
                  (dolist (m again) (mq-protocol:ack c m)))))
          (close c))))))

(deftest live-nack-without-requeue-out-of-order
  ;; a2 dropped (nack :requeue nil), then a1 acked: the next member
  ;; resumes at a3.
  (with-broker ()
    (let ((topic (%name "r3-nd"))
          (group (%name "r3-ndg")))
      (%publish-each topic '("a1" "a2" "a3"))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let ((got (mq-protocol:poll-messages c topic :n 2 :timeout 20)))
              (when (= 2 (length got))
                (mq-protocol:nack c (second got) :requeue nil)
                (mq-protocol:ack c (first got))))
          (close c)))
      (let ((c2 (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let ((next (mq-protocol:poll-messages c2 topic :n 2 :timeout 10)))
              (ok (equal '("a3") (%payloads next))
                  (format nil "resumes at a3 (got ~S)" (%payloads next)))
              (dolist (m next) (mq-protocol:ack c2 m)))
          (close c2))))))

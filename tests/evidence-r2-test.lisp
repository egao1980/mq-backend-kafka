(in-package #:mq-backend-kafka/tests)

;;; Round-2 evidence against PR #2: acks and nacks out of order, as a worker
;;; pool (or a Python ThreadPoolExecutor port) settles them. One partition
;;; (the broker's default for an auto-created topic), a1 a2 a3.
;;;
;;; README, ack: "Commits the message's offset + 1 (synchronous). The
;;; group's next member resumes after it. As in Kafka, this also settles
;;; earlier messages of the same partition." %ACK commits the acked
;;; message's own offset + 1 whatever was committed before, so acking a2
;;; and then a1 moves the group's offset back from 2 to 1.
;;;
;;; README, nack :requeue t: "it comes again, marked redelivered-p, then the
;;; later messages of its partition that were not handed out yet." %NACK
;;; seeks back to the nacked offset, and %HELD no longer has the acked a2,
;;; so a2 comes again too.

(defun %publish-all (topic payloads)
  (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "r2p"))))
    (unwind-protect
        (dolist (s payloads)
          (mq-protocol:publish p topic (mq-protocol:make-mq-message :payload s)))
      (close p))))

(deftest out-of-order-acks-never-move-the-commit-back
  (with-broker ()
    (let ((topic (%name "r2-oa"))
          (group (%name "r2-oag")))
      (%publish-all topic '("a1" "a2" "a3"))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let ((got (mq-protocol:poll-messages c topic :n 2 :timeout 20)))
              (ok (equal '("a1" "a2") (%payloads got)) "a1 a2")
              (when (= 2 (length got))
                (mq-protocol:ack c (second got))
                (mq-protocol:ack c (first got))))
          (close c)))
      (let ((c2 (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let ((next (mq-protocol:poll-messages c2 topic :n 2 :timeout 10)))
              (ok (equal '("a3") (%payloads next))
                  (format nil "a2 and a1 were acked: the next member resumes at a3 (got ~S)"
                          (%payloads next)))
              (dolist (m next) (mq-protocol:ack c2 m)))
          (close c2))))))

(deftest nack-after-acking-a-later-message-redelivers-only-the-nacked-one
  (with-broker ()
    (let ((topic (%name "r2-on")))
      (%publish-all topic '("a1" "a2" "a3"))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id (%name "r2-ong"))))
        (unwind-protect
            (let ((got (mq-protocol:poll-messages c topic :n 2 :timeout 20)))
              (ok (equal '("a1" "a2") (%payloads got)) "a1 a2")
              (when (= 2 (length got))
                ;; a2 is done, a1 failed.
                (mq-protocol:ack c (second got))
                (mq-protocol:nack c (first got) :requeue t)
                (let ((again (mq-protocol:poll-messages c topic :n 3 :timeout 5)))
                  (ok (equal '("a1" "a3") (%payloads again))
                      (format nil "a1 again, then a3; a2 was acked (got ~S)" (%payloads again)))
                  (dolist (m again) (mq-protocol:ack c m)))))
          (close c))))))

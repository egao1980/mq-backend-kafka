(in-package #:mq-backend-kafka/tests)

;;; #4: an ack of a message whose partition was revoked from this member
;;; must not commit: another member owns the partition and may have
;;; committed past it. The late ack signals KAFKA-PARTITION-REVOKED (CONTINUE
;;; returns) and the group's commit stays where the new owner put it.
;;; #3: cooperative-sticky through :config; CLOSE hands the partition over
;;; promptly.
;;;
;;; The range assignor gives a partition to the member whose id sorts first,
;;; and a member id starts with the client.id: "a-..." takes it from "z-...".

(defun %member (group client-id &rest config)
  (%backend :bootstrap-servers (%bootstrap) :group-id group
            :config (cons (cons "client.id" client-id) config)))

(defun %next-member-gets (topic group &key (timeout 8))
  "Payloads a fresh member of GROUP polls from TOPIC within TIMEOUT."
  (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id group)))
    (unwind-protect
        (%payloads (mq-protocol:poll-messages c topic :n 5 :timeout timeout))
      (close c))))

(deftest late-ack-after-revocation-commits-nothing
  (with-broker ()
    (let* ((topic (%name "rv"))
           (group (%name "rvg"))
           (late (%member group "z-late"))
           (owner nil))
      (%publish-each topic '("b1" "b2" "b3" "b4"))
      (unwind-protect
          (let ((held (mq-protocol:poll-messages late topic :n 4 :timeout 20)))
            (ok (equal '("b1" "b2" "b3" "b4") (%payloads held)) "member z holds b1..b4")
            (setf owner (%member group "a-owner"))
            ;; z keeps polling, so it serves the rebalance that gives the
            ;; partition to a.
            (let* ((poller (bt2:make-thread
                            (lambda () (mq-protocol:poll-messages late topic :timeout 12))
                            :name "late poller"))
                   (got (mq-protocol:poll-messages owner topic :n 4 :timeout 30)))
              (bt2:join-thread poller)
              (ok (equal '("b1" "b2" "b3" "b4") (%payloads got))
                  (format nil "member a owns the partition now and gets b1..b4 (got ~S)"
                          (%payloads got)))
              (dolist (m got) (mq-protocol:ack owner m)))
            (when (= 4 (length held))
              (ok (signals (mq-protocol:ack late (first held))
                           'mq-backend-kafka:kafka-partition-revoked)
                  "a late ack of b1 signals kafka-partition-revoked")
              (ok (signals (mq-protocol:nack late (second held) :requeue nil)
                           'mq-backend-kafka:kafka-partition-revoked)
                  "so does a late nack :requeue nil of b2")
              (ok (signals (mq-protocol:nack late (third held) :requeue t)
                           'mq-backend-kafka:kafka-partition-revoked)
                  "and a late nack :requeue t of b3")
              (ok (eq (fourth held)
                      (handler-bind ((mq-backend-kafka:kafka-partition-revoked #'continue))
                        (mq-protocol:ack late (fourth held))))
                  "CONTINUE returns from the ack of b4")
              (ok (signals (mq-protocol:ack late (first held)) 'mq-protocol:mq-error)
                  "b1's tag is gone after the refusal")))
        (close late)
        (when owner (close owner)))
      ;; a committed past b4; the late acks did not move it back.
      (let ((next (%next-member-gets topic group)))
        (ok (null next)
            (format nil "the group resumes after b4: nothing again (got ~S)" next))))))

(deftest cooperative-revocation-keeps-the-other-partitions
  ;; cooperative-sticky: member a joins and takes topic A's partition from
  ;; z. z's ack of its A message commits nothing; z keeps topic B's
  ;; partition, and its B ack commits.
  (with-broker ()
    (let* ((ta (%name "cva"))
           (tb (%name "cvb"))
           (group (%name "cvg"))
           (coop '("partition.assignment.strategy" . "cooperative-sticky"))
           (z (%member group "z-coop" coop))
           (a nil))
      (%publish-each ta '("a1"))
      (%publish-each tb '("b1"))
      (unwind-protect
          (let ((a1 (first (mq-protocol:poll-messages z ta :timeout 20)))
                (b1 (first (mq-protocol:poll-messages z tb :timeout 20))))
            (ok (and a1 b1) "z holds a1 and b1")
            (setf a (%member group "a-coop" coop))
            (let* ((poller (bt2:make-thread
                            (lambda () (mq-protocol:poll-messages z tb :timeout 15))
                            :name "z poller"))
                   (got (mq-protocol:poll-messages a ta :n 1 :timeout 30)))
              (bt2:join-thread poller)
              (ok (equal '("a1") (%payloads got))
                  (format nil "a takes topic A's partition and gets a1 (got ~S)" (%payloads got)))
              (dolist (m got) (mq-protocol:ack a m)))
            (when a1
              (ok (signals (mq-protocol:ack z a1) 'mq-backend-kafka:kafka-partition-revoked)
                  "z's late ack of a1 signals kafka-partition-revoked"))
            (when b1
              (ok (eq b1 (mq-protocol:ack z b1)) "z still owns topic B: the ack of b1 commits")))
        (close z)
        (when a (close a)))
      (ok (null (%next-member-gets ta group)) "topic A resumes after a1")
      (ok (null (%next-member-gets tb group)) "topic B resumes after b1"))))

(deftest cooperative-sticky-close-hands-over-promptly
  ;; #3: member 1 acks m1 and closes; member 2 of the same group resumes at
  ;; m2 at once, not after member 1's session timeout.
  (with-broker ()
    (let ((topic (%name "coop"))
          (group (%name "coopg"))
          (config '(("partition.assignment.strategy" . "cooperative-sticky"))))
      (%publish-each topic '("m1" "m2"))
      (let ((b1 (%backend :bootstrap-servers (%bootstrap) :group-id group :config config)))
        (unwind-protect
            (let ((msgs (mq-protocol:poll-messages b1 topic :n 1 :timeout 20)))
              (ok (equal '("m1") (%payloads msgs)) "member 1 gets m1")
              (when msgs (mq-protocol:ack b1 (first msgs))))
          (close b1)))
      (let ((b2 (%backend :bootstrap-servers (%bootstrap) :group-id group :config config))
            (start (get-internal-real-time)))
        (unwind-protect
            (let* ((got (mq-protocol:poll-messages b2 topic :n 1 :timeout 30))
                   (ms (%ms-since start)))
              (ok (equal '("m2") (%payloads got))
                  (format nil "member 2 resumes at m2 (got ~S)" (%payloads got)))
              (ok (< ms 10000) (format nil "member 2 got it in ~D ms" ms)))
          (close b2))))))

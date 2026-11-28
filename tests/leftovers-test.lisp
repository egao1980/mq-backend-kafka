(in-package #:mq-backend-kafka/tests)

;;; mq-backend-kafka#8: leftovers of the 0.2.2 adversarial check. These run
;;; without a broker (white box, librdkafka loaded).

;;; --- A second CLOSE waits for the release -------------------------------

(deftest a-second-close-waits-for-a-deferred-release
  (let* ((s (mq-backend-kafka::make-rdkafka-session :bootstrap "127.0.0.1:1"))
         (inside (bt2:make-semaphore :name "inside"))
         (leave (bt2:make-semaphore :name "leave"))
         (left nil)
         (second-returned-at nil)
         (left-at nil))
    (let ((caller (bt2:make-thread
                   (lambda ()
                     (mq-backend-kafka::%enter s)
                     ;; CLOSE from inside a call: deferred to the call's end.
                     (mq-backend-kafka::%close-session s)
                     (bt2:signal-semaphore inside)
                     (bt2:wait-on-semaphore leave)
                     (setf left-at (get-internal-real-time)
                           left t)
                     (mq-backend-kafka::%leave s))
                   :name "caller")))
      (bt2:wait-on-semaphore inside)
      (let ((second (bt2:make-thread
                     (lambda ()
                       (mq-backend-kafka::%close-session s)
                       (setf second-returned-at (get-internal-real-time)))
                     :name "second close")))
        (sleep 0.3)
        (ok (null second-returned-at) "the second close is still waiting while the call runs")
        (bt2:signal-semaphore leave)
        (bt2:join-thread caller)
        (bt2:join-thread second)
        (ok (and left second-returned-at (>= second-returned-at left-at))
            "the second close returns after the deferred release")))))

(deftest a-close-inside-a-call-after-a-close-does-not-wait
  ;; The thread inside the call would wait for itself.
  (let ((s (mq-backend-kafka::make-rdkafka-session :bootstrap "127.0.0.1:1"))
        (done nil))
    (let ((th (bt2:make-thread
               (lambda ()
                 (mq-backend-kafka::%enter s)
                 (mq-backend-kafka::%close-session s)
                 (mq-backend-kafka::%close-session s)
                 (setf done t)
                 (mq-backend-kafka::%leave s))
               :name "twice")))
      (loop repeat 100 until done do (sleep 0.02))
      (ok done "a second close inside the call returns at once")
      (bt2:join-thread th))))

;;; --- KIP-848: UNKNOWN_MEMBER_ID does not revoke everything --------------

(defun %settle-refused-with (rc &rest config)
  "Settle offset 5 of t[0] on a session (CONFIG) whose commit is refused
with RC. → (values outcome ownership-state settled-offsets)."
  (let ((s (mq-backend-kafka::make-rdkafka-session :bootstrap "127.0.0.1:1" :config config))
        (key (list "t" 0)))
    (setf (gethash key (mq-backend-kafka::%session-ownership s)) :owned)
    (%with-redefined (mq-backend-kafka::%commit (lambda (session topic partition next)
                                                  (declare (ignore session topic partition next))
                                                  rc))
      (let ((outcome (mq-backend-kafka::%settle s 1 (list "t" 0 5 0))))
        (values outcome
                (gethash key (mq-backend-kafka::%session-ownership s))
                (gethash key (mq-backend-kafka::%session-settled s)))))))

(deftest unknown-member-id-under-the-consumer-protocol-keeps-the-assignment
  (multiple-value-bind (outcome state settled)
      (%settle-refused-with mq-backend-kafka::+rd-kafka-resp-err-unknown-member-id+
                            '("group.protocol" . "consumer"))
    (ok (null outcome) (format nil "ack returns (got ~S)" outcome))
    (ok (eq :owned state) (format nil "t[0] stays owned (got ~S)" state))
    (ok (equal '(5) settled) (format nil "offset 5 stays settled for the next commit (got ~S)" settled))))

(deftest unknown-member-id-under-the-classic-protocol-revokes
  (multiple-value-bind (outcome state)
      (%settle-refused-with mq-backend-kafka::+rd-kafka-resp-err-unknown-member-id+)
    (ok (eq :revoked (first outcome)) (format nil "kafka-partition-revoked (got ~S)" outcome))
    (ok (null state) "t[0] revoked for good")))

;;; --- The assignment's rd_kafka_committed: shorter, and retried ----------

(deftest group-committed-retries-a-coordinator-wait
  (let ((calls '()))
    (%with-redefined (mq-backend-kafka::%rd-committed
                      (lambda (rk list timeout-ms)
                        (declare (ignore rk list))
                        (push timeout-ms calls)
                        (if (= 1 (length calls))
                            mq-backend-kafka::+rd-kafka-resp-err-wait-coord+
                            0)))
      (let ((got (mq-backend-kafka::%group-committed (cffi:null-pointer) '(("t" 0)))))
        (ok (equal '(("t" 0)) (mapcar #'car got))
            (format nil "the second attempt answers for t[0] (got ~S)" got))
        (ok (= 2 (length calls)) (format nil "two attempts (got ~S)" (reverse calls)))
        (ok (<= (reduce #'+ calls) 5000)
            (format nil "at most 5 s in all under the session lock (got ~S ms)" (reverse calls)))))))

(deftest group-committed-does-not-retry-a-hard-error
  (let ((calls 0))
    (%with-redefined (mq-backend-kafka::%rd-committed
                      (lambda (rk list timeout-ms)
                        (declare (ignore rk list timeout-ms))
                        (incf calls)
                        mq-backend-kafka::+rd-kafka-resp-err-unknown-member-id+))
      (ok (null (mq-backend-kafka::%group-committed (cffi:null-pointer) '(("t" 0)))))
      (ok (= 1 calls) (format nil "one attempt (got ~D)" calls)))))

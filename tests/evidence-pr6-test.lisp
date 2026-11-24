(in-package #:mq-backend-kafka/tests)

;;; Adversarial evidence against PR #6 (0.2.1, fixes #3 #4 #5), with a live
;;; broker. The default assignor (range,roundrobin) is eager: adding a topic
;;; to the subscription, or a member joining, revokes every partition and
;;; assigns them again. PR #6: "An eager revoke-then-assign that gives a
;;; partition straight back keeps the held messages, when the group's
;;; committed offset (rd_kafka_committed) is unchanged. Acks made between
;;; the revoke and the assign commit when the assign comes."

(defun %session-of (client-id)
  "White box: the librdkafka driver session whose :config sets CLIENT-ID."
  (bt2:with-lock-held (mq-backend-kafka::*sessions-lock*)
    (loop for s being the hash-values of mq-backend-kafka::*sessions*
          when (equal client-id
                      (cdr (assoc "client.id" (mq-backend-kafka::%session-config s)
                                  :test #'string=)))
            return s)))

(defun %ownership (client-id topic partition)
  (let ((s (%session-of client-id)))
    (and s
         (bt2:with-recursive-lock-held ((mq-backend-kafka::%session-lock s))
           (gethash (list topic partition) (mq-backend-kafka::%session-ownership s))))))

(defun %ack-outcome (backend message)
  "Ack MESSAGE: :ACKED, :REVOKED (kafka-partition-revoked) or the other
MQ-ERROR's text."
  (handler-case (progn (mq-protocol:ack backend message) :acked)
    (mq-backend-kafka:kafka-partition-revoked () :revoked)
    (mq-protocol:mq-error (e) (princ-to-string e))))

(deftest eager-rebalance-with-an-earlier-commit-keeps-held-messages
  ;; The group has a commit from an earlier member (a0 acked: 1). A new
  ;; member holds a1, has not committed itself, and adds topic B to its
  ;; subscription. The eager rebalance gives A's partition straight back
  ;; and the group's commit stays 1, yet %RETAINED-P compares it with this
  ;; member's own last commit (none: RD_KAFKA_OFFSET_INVALID) and revokes
  ;; the partition for good.
  (with-broker ()
    (let ((ta (%name "kra"))
          (tb (%name "krb"))
          (group (%name "krg")))
      (%publish-each ta '("a0" "a1" "a2"))
      (%publish-each tb '("b1"))
      (let ((earlier (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let ((got (mq-protocol:poll-messages earlier ta :n 1 :timeout 20)))
              (ok (equal '("a0") (%payloads got)) "an earlier member gets a0 and acks it")
              (dolist (m got) (mq-protocol:ack earlier m)))
          (close earlier)))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let* ((a1 (mq-protocol:poll-messages c ta :n 1 :timeout 20))
                   (bs (mq-protocol:poll-messages c tb :n 1 :timeout 20)))
              (ok (equal '("a1") (%payloads a1)) "this member holds a1")
              (ok (equal '("b1") (%payloads bs)) "polling B (a rebalance) gives b1")
              (when a1
                (let ((outcome (%ack-outcome c (first a1))))
                  (ok (eq :acked outcome)
                      (format nil "the ack of a1 commits: A's partition came straight back (got ~S)"
                              outcome))))
              (let ((rest (mq-protocol:poll-messages c ta :n 2 :timeout 5)))
                (ok (equal '("a2") (%payloads rest))
                    (format nil "A goes on with a2; a1 is not handed out again (got ~S)"
                            (%payloads rest)))
                (dolist (m (append bs rest)) (%ack-outcome c m))))
          (close c))))))

(deftest member-joining-with-an-earlier-commit-keeps-held-messages
  ;; The same with a second member: the group's commit is 1 (an earlier
  ;; member acked a0). z holds a1; y joins, and the range assignor gives
  ;; the partition straight back to z ("a-..." sorts first). The group's
  ;; commit stays 1.
  (with-broker ()
    (let* ((topic (%name "krj"))
           (group (%name "krjg"))
           (z (%member group "a-keep"))
           (y nil)
           (z-got :none))
      (%publish-each topic '("a0" "a1"))
      (let ((earlier (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (dolist (m (mq-protocol:poll-messages earlier topic :n 1 :timeout 20))
              (mq-protocol:ack earlier m))
          (close earlier)))
      (unwind-protect
          (let ((a1 (mq-protocol:poll-messages z topic :n 1 :timeout 20)))
            (ok (equal '("a1") (%payloads a1)) "z holds a1")
            (setf y (%member group "z-join"))
            (let ((zp (bt2:make-thread
                       (lambda () (setf z-got (mq-protocol:poll-messages z topic :n 1 :timeout 10)))
                       :name "z poller")))
              (ok (null (mq-protocol:poll-messages y topic :timeout 8))
                  "y joins and gets nothing: the partition stays with z")
              (bt2:join-thread zp))
            (ok (null z-got)
                (format nil "z's poll during the rebalance hands out nothing: a1 is held (got ~S)"
                        (%payloads z-got)))
            (when a1
              (let ((outcome (%ack-outcome z (first a1))))
                (ok (eq :acked outcome)
                    (format nil "z's ack of a1 commits: the partition never left z (got ~S)"
                            outcome)))))
        (close z)
        (when y (close y))))))

(deftest acked-message-is-not-handed-out-again-after-a-retained-rebalance
  ;; Fresh group. a2 is buffered (A's partition paused) while B is polled.
  ;; a1's ack commits 1. Polling C rebalances: A's partition comes back
  ;; (retained: the group's commit is this member's 1) and its fetch
  ;; restarts at the commit, 1; the pause is kept. Then a2 is handed out
  ;; from the buffer and acked: the commit moves to 2 and SETTLED drops
  ;; offset 1. When A's partition resumes, the fetch from 1 returns a2,
  ;; which is neither held nor settled any more.
  (with-broker ()
    (let ((ta (%name "kpa"))
          (tb (%name "kpb"))
          (tc (%name "kpc"))
          (group (%name "kpg")))
      (%publish-each ta '("a1" "a2"))
      (%publish-each tb '("b1"))
      (%publish-each tc '("c1"))
      (let ((c (%backend :bootstrap-servers (%bootstrap) :group-id group)))
        (unwind-protect
            (let* ((a1 (mq-protocol:poll-messages c ta :n 1 :timeout 20))
                   (bs (mq-protocol:poll-messages c tb :n 2 :timeout 5)))
              (ok (equal '("a1") (%payloads a1)) "A gives a1")
              (ok (equal '("b1") (%payloads bs)) "B gives b1 (a2 waits for A)")
              (dolist (m a1) (mq-protocol:ack c m))
              (let* ((cs (mq-protocol:poll-messages c tc :n 2 :timeout 5))
                     (a2 (mq-protocol:poll-messages c ta :n 1 :timeout 20)))
                (ok (equal '("c1") (%payloads cs)) "C gives c1")
                (ok (equal '("a2") (%payloads a2)) "A gives a2")
                (dolist (m a2) (mq-protocol:ack c m))
                (let ((again (mq-protocol:poll-messages c ta :n 2 :timeout 5)))
                  (ok (null again)
                      (format nil "a2 was acked and committed: A has nothing more (got ~S)"
                              (%payloads again)))
                  (dolist (m (append bs cs again)) (%ack-outcome c m)))))
          (close c))))))

(deftest acks-while-revoked-are-not-handed-out-again-on-the-assignment
  ;; z holds a1 a2 of T. y owns topic U. w joins with T; y does not poll,
  ;; so the eager rebalance waits for y while T's partition is revoked
  ;; from z (:pending). z acks a1 and a2 meanwhile. Then y polls, and the
  ;; assignment gives T's partition back to z (range: "a-..." sorts
  ;; first). The callback assigns (librdkafka asks the coordinator for
  ;; the committed offset: none yet), then commits 2 and drops a1 a2 from
  ;; SETTLED; the fetch from the earliest offset returns a1 a2 again.
  (with-broker ()
    (let* ((tt (%name "kwt"))
           (tu (%name "kwu"))
           (group (%name "kwg"))
           (zid (%name "a-z"))
           (yid (%name "m-y"))
           (wid (%name "w-w"))
           (z (%member group zid))
           (y (%member group yid))
           (w nil)
           (z-got '())
           (outcomes '())
           (state nil))
      (%publish-each tt '("a1" "a2"))
      (%publish-each tu '("u1"))
      (unwind-protect
          (let ((held (mq-protocol:poll-messages z tt :n 2 :timeout 20)))
            (ok (equal '("a1" "a2") (%payloads held)) "z holds a1 a2")
            ;; y joins with U; z polls meanwhile and serves that rebalance.
            (let ((zp (bt2:make-thread (lambda () (mq-protocol:poll-messages z tt :timeout 8))
                                       :name "z poller 1")))
              (ok (equal '("u1") (%payloads (mq-protocol:poll-messages y tu :n 1 :timeout 20)))
                  "y owns U and gets u1")
              (bt2:join-thread zp))
            ;; w joins with T. z polls; y does not.
            (setf w (%member group wid))
            (let ((zp (bt2:make-thread
                       (lambda () (setf z-got (mq-protocol:poll-messages z tt :n 2 :timeout 25)))
                       :name "z poller 2"))
                  (wp (bt2:make-thread (lambda () (mq-protocol:poll-messages w tt :timeout 25))
                                       :name "w poller")))
              (loop repeat 150
                    until (eq :pending (%ownership zid tt 0))
                    do (sleep 0.1))
              (setf state (%ownership zid tt 0))
              (ok (eq :pending state)
                  (format nil "T's partition is revoked from z, the rebalance waits for y (~S)" state))
              (setf outcomes (mapcar (lambda (m) (%ack-outcome z m)) held))
              (ok (equal '(:acked :acked) outcomes)
                  (format nil "z acks a1 a2 while revoked (~S)" outcomes))
              (mq-protocol:poll-messages y tu :timeout 5)
              (bt2:join-thread zp)
              (bt2:join-thread wp))
            (ok (eq :owned (%ownership zid tt 0)) "T's partition came back to z")
            (ok (null z-got)
                (format nil "z's poll after the assignment repeats nothing: a1 a2 were acked (got ~S)"
                        (%payloads z-got))))
        (close z)
        (close y)
        (when w (close w)))
      (let ((next (%next-member-gets tt group)))
        (ok (null next) (format nil "the group's commit is past a2 (got ~S)" next))))))

(deftest late-ack-after-max-poll-interval-signals-revoked
  ;; z holds m1 m2. a joins; the rebalance queues an (eager, not lost)
  ;; revocation for z's next poll, and z does not poll. After
  ;; max.poll.interval.ms (6 s) librdkafka leaves the group for z (its
  ;; lost revocation is skipped: a rebalance is already in progress), so a
  ;; takes the partition, gets m1 m2 and acks them. z, still not polling,
  ;; acks m1 late. PR #6: an ack of a message fetched before its partition
  ;; left this member settles nothing and signals kafka-partition-revoked.
  ;; The epoch only moves when a poll serves a revocation for good: %ACK
  ;; settles m1 and calls rd_kafka_commit, and after a poll the partition
  ;; is only :PENDING.
  (with-broker ()
    (let* ((topic (%name "kmp"))
           (group (%name "kmpg"))
           (fast '(("session.timeout.ms" . "6000")
                   ("max.poll.interval.ms" . "6000")
                   ("heartbeat.interval.ms" . "1000")))
           (z (apply #'%member group "z-mp" fast))
           (a nil))
      (%publish-each topic '("m1" "m2"))
      (unwind-protect
          (let ((held (mq-protocol:poll-messages z topic :n 2 :timeout 20)))
            (ok (equal '("m1" "m2") (%payloads held)) "z holds m1 m2")
            (setf a (%member group "a-mp"))
            (let ((got (mq-protocol:poll-messages a topic :n 2 :timeout 40)))
              (ok (equal '("m1" "m2") (%payloads got))
                  (format nil "a gets m1 m2 once z's poll interval ran out (got ~S)"
                          (%payloads got)))
              (dolist (m got) (mq-protocol:ack a m)))
            (when (= 2 (length held))
              (let ((late (%ack-outcome z (first held)))
                    (retry (%ack-outcome z (first held))))
                (ok (eq :revoked late)
                    (format nil "z's late ack of m1 signals kafka-partition-revoked (got ~S)" late))
                (ok (eq :revoked retry)
                    (format nil "acking m1 again still says revoked (got ~S)" retry)))
              (mq-protocol:poll-messages z topic :timeout 2)
              (let ((state (%ownership "z-mp" topic 0)))
                (ok (null state)
                    (format nil "after a poll, z no longer counts the partition as its own (~S)"
                            state)))
              (let ((after (%ack-outcome z (second held))))
                (ok (eq :revoked after)
                    (format nil "after a poll, z's ack of m2 signals kafka-partition-revoked (got ~S)"
                            after)))))
        (close z)
        (when a (close a)))
      (let ((next (%next-member-gets topic group)))
        (ok (null next) (format nil "a's commit after m2 stands (got ~S)" next))))))

(deftest-parametrize churn-hands-out-every-message
    ((config)
     ('())
     ('(("partition.assignment.strategy" . "cooperative-sticky"))))
  ;; Held check, no break expected: z runs a worker pool (a poller, two
  ;; ackers acking in any order) for 20 s while 30 messages are published
  ;; one by one. Member "a-..." (sorts first: it takes the partition under
  ;; range) joins after 3 s for 6 s, another after 12 s for 4 s, each with
  ;; its own pool. Every message must reach some caller, and the group's
  ;; commit must end after the last one.
  (with-broker ()
    (let* ((topic (%name "kch"))
           (group (%name "kchg"))
           (total 30)
           (all (loop for i below total collect (format nil "p~D" i)))
           (lock (bt2:make-lock :name "churn"))
           (delivered '())
           (outcomes '())
           (errors '()))
      (labels ((note (kind value)
                 (bt2:with-lock-held (lock)
                   (ecase kind
                     (:delivered (push value delivered))
                     (:outcome (push value outcomes))
                     (:error (push value errors)))))
               (pool (client-id seconds)
                 (let* ((b (apply #'%member group client-id config))
                        (q-lock (bt2:make-lock :name "churn queue"))
                        (queue '())
                        (polling t)
                        (deadline (+ (get-internal-real-time)
                                     (* seconds internal-time-units-per-second)))
                        (ackers
                         (loop repeat 2
                               collect (bt2:make-thread
                                        (lambda ()
                                          (handler-case
                                              (loop
                                                (let ((m (bt2:with-lock-held (q-lock) (pop queue))))
                                                  (cond
                                                   (m (sleep (random 0.05))
                                                      (note :outcome (%ack-outcome b m)))
                                                   ((not polling) (return))
                                                   (t (sleep 0.01)))))
                                            (error (e) (note :error e))))
                                        :name "churn acker"))))
                   (unwind-protect
                       (handler-case
                           (loop while (< (get-internal-real-time) deadline)
                                 do (dolist (m (mq-protocol:poll-messages b topic :n 3 :timeout 0.5))
                                      (note :delivered (map 'string #'code-char
                                                            (mq-protocol:mq-message-payload m)))
                                      (bt2:with-lock-held (q-lock)
                                        (setf queue (append queue (list m))))))
                         (error (e) (note :error e)))
                     (setf polling nil)
                     (mapc #'bt2:join-thread ackers)
                     (close b)))))
        (let ((publisher (bt2:make-thread
                          (lambda ()
                            (let ((p (%backend :bootstrap-servers (%bootstrap)
                                               :group-id (%name "kchp"))))
                              (unwind-protect
                                  (dolist (s all)
                                    (mq-protocol:publish p topic (mq-protocol:make-mq-message
                                                                  :payload s))
                                    (mq-backend-kafka:kafka-flush p)
                                    (sleep 0.4))
                                (close p))))
                          :name "churn publisher"))
              (z (bt2:make-thread (lambda () (pool (%name "z-ch") 20)) :name "z pool")))
          (sleep 3)
          (pool (%name "a-ch") 6)
          (sleep 3)
          (pool (%name "a-ch2") 4)
          (bt2:join-thread publisher)
          (bt2:join-thread z)))
      (let* ((c (%backend :bootstrap-servers (%bootstrap) :group-id group))
             (rest (unwind-protect
                       (%payloads (mq-protocol:poll-messages c topic :n total :timeout 8))
                     (close c)))
             (lost (set-difference all (union delivered rest :test #'string=) :test #'string=)))
        (ok (null errors) (format nil "no thread failed (~{~A~^; ~})" errors))
        (ok (null lost) (format nil "every message reached a caller (lost ~S)" lost))
        (ok (null rest) (format nil "the group's commit ends after p29 (left ~S)" rest))
        (ok t (format nil "~D deliveries of ~D messages; ~D acks, ~D revoked, other ~S"
                      (length delivered) total
                      (count :acked outcomes) (count :revoked outcomes)
                      (remove-if #'symbolp outcomes)))))))

(deftest cooperative-acks-during-rebalances-are-not-refused
  ;; cooperative-sticky keeps z's partition through rebalances, so z
  ;; commits while the group rebalances. A commit the broker refuses then
  ;; (ILLEGAL_GENERATION, REBALANCE_IN_PROGRESS) makes the ack signal a
  ;; plain mq-error after %TAKE-INFLIGHT dropped the delivery tag and
  ;; %SETTLE recorded the offset as settled: the ack cannot be retried,
  ;; and the offset is committed only if a later ack of the partition
  ;; commits. Members join and leave eight times while z acks about 200
  ;; messages as they come.
  (with-broker ()
    (let* ((topic (%name "kco"))
           (group (%name "kcog"))
           (coop '("partition.assignment.strategy" . "cooperative-sticky"))
           (z (%member group (%name "a-kco") coop))
           (lock (bt2:make-lock :name "kco"))
           (outcomes '())
           (refused '())
           (errors '())
           (running t))
      (%publish-each topic '("w0"))
      (unwind-protect
          (let ((worker
                 (bt2:make-thread
                  (lambda ()
                    (handler-case
                        (loop while running
                              do (dolist (m (mq-protocol:poll-messages z topic :n 5 :timeout 0.2))
                                   (let ((outcome (%ack-outcome z m)))
                                     (bt2:with-lock-held (lock)
                                       (push outcome outcomes)
                                       (unless (member outcome '(:acked :revoked))
                                         (push (list outcome (%ack-outcome z m)) refused))))))
                      (error (e) (bt2:with-lock-held (lock) (push e errors)))))
                  :name "z worker"))
                (publisher
                 (bt2:make-thread
                  (lambda ()
                    (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "kcop"))))
                      (unwind-protect
                          (loop for i from 1 to 200
                                do (mq-protocol:publish p topic (mq-protocol:make-mq-message
                                                                 :payload (format nil "w~D" i)))
                                   (mq-backend-kafka:kafka-flush p)
                                   (sleep 0.05))
                        (close p))))
                  :name "kco publisher")))
            (loop repeat 8
                  do (let ((j (%member group (%name "z-kcoj") coop)))
                       (unwind-protect (mq-protocol:poll-messages j topic :timeout 1.5)
                         (close j))))
            (bt2:join-thread publisher)
            (sleep 2)
            (setf running nil)
            (bt2:join-thread worker))
        (close z))
      (ok (null errors) (format nil "the worker did not fail (~{~A~^; ~})" errors))
      (ok (null refused)
          (format nil "no ack is refused while the group rebalances (~D of ~D refused~@[, e.g. ~S~])"
                  (length refused) (length outcomes) (first refused))))))

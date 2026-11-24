(in-package #:mq-backend-kafka/tests)

;;; #5 beyond the round-3 evidence: one backend used from several threads.
;;; CLOSE stops another thread's long poll at its next slice, and a worker
;;; pool (one poller, ackers in any order, a publisher) settles every
;;; message once: the group's next member gets nothing again.

(deftest close-stops-a-poll-on-another-thread
  (with-broker ()
    (let* ((topic (%name "cl"))
           (b (%backend :bootstrap-servers (%bootstrap) :group-id (%name "clg")))
           (result nil))
      (%publish-each topic '("x"))
      (ok (mq-protocol:poll-messages b topic :timeout 20) "x polled: the consumer is assigned")
      (let ((poller (bt2:make-thread
                     (lambda ()
                       (setf result
                             (handler-case (progn (mq-protocol:poll-messages b topic :timeout 30)
                                                  :returned)
                               (mq-protocol:mq-connection-error () :closed))))
                     :name "poller")))
        (sleep 0.5)
        (let ((start (get-internal-real-time)))
          (close b)
          (let ((close-ms (%ms-since start)))
            (bt2:join-thread poller)
            (let ((both-ms (%ms-since start)))
              (ok (< close-ms 3000) (format nil "close returned in ~D ms" close-ms))
              (ok (< both-ms 3000) (format nil "the poll ended ~D ms after close began" both-ms))
              (ok (eq :closed result)
                  (format nil "the poll signals mq-connection-error (got ~S)" result)))))))))

(deftest worker-pool-settles-every-message-once
  (with-broker ()
    (let* ((topic (%name "wp"))
           (group (%name "wpg"))
           (b (%backend :bootstrap-servers (%bootstrap) :group-id group))
           (total 40)
           (lock (bt2:make-lock :name "queue"))
           (queue '())
           (seen '())
           (acked 0)
           (errors '())
           (polling t))
      (mq-protocol:publish b topic (mq-protocol:make-mq-message :payload "p0"))
      (mq-backend-kafka:kafka-flush b)
      (flet ((guard (fn)
               (lambda ()
                 (handler-case (funcall fn)
                   (error (e) (bt2:with-lock-held (lock) (push e errors)))))))
        (let ((publisher (bt2:make-thread
                          (guard (lambda ()
                                   (loop for i from 1 below total
                                         do (mq-protocol:publish
                                             b topic (mq-protocol:make-mq-message
                                                      :payload (format nil "p~D" i)))
                                            (sleep 0.02))
                                   (mq-backend-kafka:kafka-flush b)))
                          :name "publisher"))
              (workers (loop for w below 4
                             collect (bt2:make-thread
                                      (guard (lambda ()
                                               (loop
                                                 (let ((m (bt2:with-lock-held (lock) (pop queue))))
                                                   (cond
                                                     (m (sleep (random 0.03))
                                                        (mq-protocol:ack b m)
                                                        (bt2:with-lock-held (lock) (incf acked)))
                                                     ((not polling) (return))
                                                     (t (sleep 0.01)))))))
                                      :name "acker")))
              (deadline (+ (get-internal-real-time) (* 60 internal-time-units-per-second))))
          (unwind-protect
              (loop while (and (< (length seen) total)
                               (< (get-internal-real-time) deadline))
                    do (dolist (m (mq-protocol:poll-messages b topic :n 5 :timeout 1))
                         (bt2:with-lock-held (lock)
                           (push (map 'string #'code-char (mq-protocol:mq-message-payload m)) seen)
                           (setf queue (append queue (list m))))))
            (setf polling nil)
            (bt2:join-thread publisher)
            (mapc #'bt2:join-thread workers))
          (ok (null errors) (format nil "no thread failed (~{~A~^; ~})" errors))
          (ok (= total (length seen) (length (remove-duplicates seen :test #'string=)))
              (format nil "~D distinct messages polled, ~D in all" total (length seen)))
          (ok (= total acked) (format nil "~D acked" acked))))
      (close b)
      (let ((next (%next-member-gets topic group)))
        (ok (null next) (format nil "every ack was committed: nothing again (got ~S)" next))))))

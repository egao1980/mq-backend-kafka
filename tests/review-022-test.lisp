(in-package #:mq-backend-kafka/tests)

;;; The adversarial check of the 0.2.2 candidate, with a live broker:
;;; - a commit past the log end (librdkafka resets to the earliest offset)
;;;   must not make the driver drop the messages below it;
;;; - CLOSE from a handler of a condition signalled inside a poll, with
;;;   the session's locks held and an ack waiting for them on another
;;;   thread, must not deadlock;
;;; - a header name that is not UTF-8 must not fail the poll or wedge the
;;;   partition's commit.

(defun %bad-name-headers (&rest ignore)
  "An rd_kafka_headers_t with one header whose name is the byte #xFF."
  (declare (ignore ignore))
  (let ((hdrs (mq-backend-kafka::%rd-headers-new 1)))
    (cffi:with-foreign-objects ((name :uint8 2) (v :uint8 1))
      (setf (cffi:mem-aref name :uint8 0) #xff
            (cffi:mem-aref name :uint8 1) 0
            (cffi:mem-aref v :uint8 0) 65)
      (mq-backend-kafka::%rd-header-add hdrs name 1 v 1))
    hdrs))

(defmacro %with-redefined ((name function) &body body)
  "Run BODY with the global function NAME replaced by FUNCTION."
  (let ((orig (gensym "ORIG")))
    `(let ((,orig (fdefinition ',name)))
       (unwind-protect
           (progn (setf (fdefinition ',name) ,function) ,@body)
         (setf (fdefinition ',name) ,orig)))))

(defun %publish-bad-header (topic payload)
  (let ((p (%backend :bootstrap-servers (%bootstrap) :group-id (%name "advp"))))
    (unwind-protect
        (%with-redefined (mq-backend-kafka::%make-headers #'%bad-name-headers)
          (mq-protocol:publish p topic (mq-protocol:make-mq-message :payload payload))
          (mq-backend-kafka:kafka-flush p))
      (close p))))

(defun %group-commit-of (session topic)
  (bt2:with-recursive-lock-held ((mq-backend-kafka::%session-lock session))
    (cdr (first (mq-backend-kafka::%group-committed
                 (mq-backend-kafka::%session-consumer session) (list (list topic 0)))))))

(deftest header-name-not-utf-8-is-delivered-and-committed
  (with-broker ()
    (let ((topic (%name "advph"))
          (group (%name "advphg")))
      (%publish-bad-header topic "p0")
      (%publish-each topic '("g1" "g2"))
      (let ((c (%member group "adv-poison")))
        (unwind-protect
            (let ((got (handler-case (mq-protocol:poll-messages c topic :n 3 :timeout 20)
                         (error (e) (list :error (type-of e))))))
              (ok (equal '("p0" "g1" "g2") (and (every #'mq-protocol:mq-message-p got)
                                                (%payloads got)))
                  (format nil "the poll delivers p0 g1 g2 (got ~S)" got))
              (when (every #'mq-protocol:mq-message-p got)
                (ok (equalp '((#(255) . "A")) (mq-protocol:mq-message-headers (first got)))
                    "a header name that is not UTF-8 comes as octets")
                (dolist (m got) (mq-protocol:ack c m))
                (let ((commit (%group-commit-of (%session-of "adv-poison") topic)))
                  (ok (eql 3 commit) (format nil "the acks commit 3 (got ~S)" commit)))))
          (close c))))))

(define-condition %test-signal (error) ())

(deftest close-in-a-handler-inside-a-poll-with-an-ack-waiting
  (with-broker ()
    (let* ((topic (%name "advdl"))
           (group (%name "advdlg"))
           (phase :start)
           (a-result :running)
           (b-result :running))
      (%publish-each topic '("g0" "g1"))
      (let* ((c (%member group "adv-dl"))
             (g0 (mq-protocol:poll-messages c topic :n 1 :timeout 20))
             (s (%session-of "adv-dl")))
        (ok (equal '("g0") (%payloads g0)) "g0 held")
        (%with-redefined (mq-backend-kafka::%read-headers
                          (lambda (rkmessage)
                            (declare (ignore rkmessage))
                            ;; Inside %TAKE-IN: poll lock and session lock held.
                            (error '%test-signal)))
          (let ((a (bt2:make-thread
                    (lambda ()
                      (setf a-result
                            (block call
                              (handler-bind ((%test-signal
                                               (lambda (e)
                                                 (declare (ignore e))
                                                 (setf phase :in-handler)
                                                 (loop repeat 100 until (eq phase :b-waiting)
                                                       do (sleep 0.05))
                                                 (sleep 1)
                                                 (return-from call (list :closed (close c))))))
                                (mq-protocol:poll-messages c topic :n 1 :timeout 20)
                                :polled))))
                    :name "closer in a poll handler"))
                (b nil))
            (loop repeat 300 until (eq phase :in-handler) do (sleep 0.1))
            (ok (eq :in-handler phase) "the poll's handler runs")
            (setf b (bt2:make-thread
                     (lambda ()
                       (setf phase :b-waiting)
                       (setf b-result
                             (handler-case (progn (mq-protocol:ack c (first g0)) :acked)
                               (error (e) (list :error (princ-to-string e))))))
                     :name "acker"))
            (loop repeat 150 while (or (bt2:thread-alive-p a) (bt2:thread-alive-p b))
                  do (sleep 0.1))
            (ok (not (bt2:thread-alive-p a))
                (format nil "close in the handler returned (got ~S)" a-result))
            (ok (not (bt2:thread-alive-p b))
                (format nil "the waiting ack returned (got ~S)" b-result))
            (ok (equal '(:closed t) a-result) (format nil "close → T (got ~S)" a-result))
            (ok (eq :acked b-result)
                (format nil "the ack that waited ran before the handles were released (got ~S)"
                        b-result))
            (ok (and s (null (mq-backend-kafka::%session-consumer s)))
                "the handles are released once the last call ended")))))))

(deftest commit-past-the-log-end-drops-nothing
  (with-broker ()
    (let ((topic (%name "advend"))
          (group (%name "advendg")))
      (%publish-each topic '("x0"))
      (let ((c0 (%member group "adv-end0")))
        (unwind-protect
            (progn
              (ok (equal '("x0") (%payloads (mq-protocol:poll-messages c0 topic :n 1 :timeout 20)))
                  "c0 gets x0")
              (let ((s (%session-of "adv-end0")))
                (ok (zerop (bt2:with-recursive-lock-held ((mq-backend-kafka::%session-lock s))
                             (mq-backend-kafka::%commit s topic 0 100)))
                    "the group's commit is 100, past the log end")))
          (close c0)))
      (%publish-each topic '("y1" "y2"))
      (let ((c1 (%member group "adv-end1")))
        (unwind-protect
            (let ((got (%payloads (mq-protocol:poll-messages c1 topic :n 5 :timeout 12))))
              (ok (equal '("x0" "y1" "y2") got)
                  (format nil "the reset to the earliest offset delivers x0 y1 y2 (got ~S)" got)))
          (close c1))))))

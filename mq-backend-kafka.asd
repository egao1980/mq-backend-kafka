(defsystem "mq-backend-kafka"
  :version "0.1.0"
  :description "librdkafka overlay skeleton for mq-protocol (driver-injected; no CFFI load)"
  :author "egao1980"
  :license "MIT"
  :depends-on ("mq-protocol")
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "backend"))
  :in-order-to ((test-op (test-op "mq-backend-kafka/tests"))))

(defsystem "mq-backend-kafka/tests"
  :depends-on ("mq-backend-kafka" "rove")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))

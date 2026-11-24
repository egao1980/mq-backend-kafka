(defsystem "mq-backend-kafka"
  :version "0.2.0"
  :description "Kafka backend for mq-protocol: librdkafka CFFI driver with consumer groups, ack = offset commit; librdkafka ships as native overlays"
  :author "egao1980"
  :license "MIT"
  :defsystem-depends-on ("asdf-native")
  :depends-on ("mq-protocol" "cffi" "encoding-protocol" "bordeaux-threads")
  :serial t
  :pathname "src"
  :components ((:file "package")
               ;; Windows: the DLLs librdkafka.dll links, loaded first from the
               ;; same overlay directory so the loader resolves them by name.
               (:native-library "vcruntime140" :if-feature :windows :optional t)
               (:native-library "msvcp140" :if-feature :windows :optional t)
               (:native-library "z" :if-feature :windows :optional t)
               (:native-library "zstd" :if-feature :windows :optional t)
               (:native-library "libcrypto-3-x64" :if-feature :windows :optional t)
               (:native-library "libssl-3-x64" :if-feature :windows :optional t)
               (:native-library "libcurl" :if-feature :windows :optional t)
               ;; $MQ_BACKEND_KAFKA_NATIVE, native/ (overlay), lib/<os>-<arch>/
               ;; (checkout, scripts/fetch-librdkafka.sh), else the system
               ;; librdkafka; missing → a warning, and backends have no driver.
               (:native-library "rdkafka" :versions ("1") :names (:windows ("librdkafka.dll"))
                :env "MQ_BACKEND_KAFKA_NATIVE" :system t :optional t)
               (:file "ffi")
               (:file "driver")
               (:file "backend"))
  :in-order-to ((test-op (test-op "mq-backend-kafka/tests")))
  ;; Overlays: lib/<os>-<arch>/ per platform (linux amd64 / arm64, darwin
  ;; arm64, windows amd64), staged by scripts/fetch-librdkafka.sh.
  :properties (:cl-repo (:provides ("mq-backend-kafka"))))

(defsystem "mq-backend-kafka/tests"
  :depends-on ("mq-backend-kafka" "rove" "cffi" "usocket")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test")
               (:file "driver-test")
               (:file "live-test")
               (:file "topics-test")
               (:file "evidence-r2-test")
               (:file "settle-test")
               (:file "evidence-r3-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))

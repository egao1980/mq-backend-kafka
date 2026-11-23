# mq-backend-kafka

[`mq-protocol`](https://github.com/egao1980/mq-protocol) backend for **Apache Kafka** over [librdkafka](https://github.com/confluentinc/librdkafka), through CFFI. librdkafka ships with the package as native overlays and loads with the system: `(asdf:load-system "mq-backend-kafka")` is all a consumer does.

```lisp
(asdf:load-system "mq-backend-kafka")

;; confluent_kafka: Producer / Consumer({"bootstrap.servers": ..., "group.id": "workers"})
(let ((b (mq-backend-kafka:make-kafka-backend :bootstrap-servers "localhost:9092"
                                              :group-id "workers")))
  (unwind-protect
       (progn
         (stack-mq:publish b "orders" (stack-mq:make-mq-message :key "customer-7"
                                                                :payload "order-1"
                                                                :headers '(("source" . "web"))))
         (dolist (m (stack-mq:poll-messages b "orders" :n 10 :timeout 5))
           (process m)
           (stack-mq:ack b m)))          ; commits the offset
    (close b)))                          ; flush, leave the group, free the handles
```

## Semantics

| | |
|---|---|
| `make-kafka-backend` | `:bootstrap-servers`, `:group-id` (default: a fresh group per backend), `:config` (alist of librdkafka properties, e.g. `("security.protocol" . "SASL_SSL")`), `:driver` (inject your own plist), `:cffi-library-path` (a librdkafka to load when the overlay's is missing). Nothing connects until first use. |
| `publish dest msg` | Produce to topic `dest` (else the message's topic). Key and header values are UTF-8 when they are strings. Asynchronous, as in librdkafka; `kafka-flush` waits for delivery, `close` flushes. |
| `poll-messages topic &key n timeout` | The consumer joins the group and subscribes to `topic` (a rebalance follows the first poll). Waits up to `timeout` seconds for `n` messages; without `timeout` it makes one 100 ms poll. A message carries key, payload, headers, topic and a delivery tag. |
| `ack msg` | Commits the message's offset + 1 (synchronous). The group's next member resumes after it. As in Kafka, this also settles earlier messages of the same partition. |
| `nack msg :requeue t` | Seeks the partition back to the message: it, and what followed it, comes again, marked `redelivered-p`. `:requeue nil` commits past it. |
| delivery | at-least-once: `enable.auto.commit` is off, so a message that was never acked is delivered again to the next consumer of the group. |
| `close` | Leaves the group (`rd_kafka_consumer_close`), flushes up to 10 s (not with `:abort t`), destroys both handles. Later calls signal `mq-connection-error`. |

`subscribe` (push) is not offered by the librdkafka driver: `mq-backend-capabilities` reports `subscribe-p nil`, and `subscribe` signals `mq-error` with the usual `use-value` / `retry` restarts. librdkafka errors (configuration, commit, seek) are `mq-error`s with librdkafka's text. Without librdkafka every operation signals `mq-error` "librdkafka not loaded".

`librdkafka-version` / `librdkafka-loaded-p` report the loaded library.

## Native library

The `(:native-library "rdkafka")` component ([asdf-native](https://github.com/egao1980/asdf-native)) loads, by absolute path: `$MQ_BACKEND_KAFKA_NATIVE`, then `native/` (the overlay of an installed package), then `lib/<os>-<arch>/` (a checkout), else the system librdkafka. On Windows the DLLs librdkafka.dll links are loaded first from the same directory. No `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH`.

Overlays: linux amd64 / arm64, darwin arm64, windows amd64. `scripts/fetch-librdkafka.sh [os-arch … | all]` stages them from librdkafka's release builds (the `librdkafka.redist` package Confluent publishes, pinned by version and SHA-256; **librdkafka 2.16.0**). On Linux and macOS the dependencies (OpenSSL, zlib, zstd, lz4, …) are linked statically; linux-amd64 uses the CentOS 8 build, which needs only glibc 2.28+. Licenses: `NOTICE`, and `LICENSES.librdkafka.txt` in each overlay. Publish: `.github/workflows/publish-oci.yml` (tag `v*` or dispatch).

## Tests

`cl-repo test` after `scripts/fetch-librdkafka.sh`. The live tests skip without a broker on `127.0.0.1:19094` (`MQ_KAFKA_TEST_BOOTSTRAP`); `tests/driver-test.lisp` has the `docker run` line for `apache/kafka-native:3.9.0`.

## License

MIT — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

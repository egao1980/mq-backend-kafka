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
| `poll-messages topic &key n timeout` | Returns messages of `topic` only. The consumer joins the group and adds `topic` to its subscription (a rebalance follows). Waits up to `timeout` seconds for `n` messages; without `timeout` it makes one 100 ms poll. A message of another subscribed topic that arrives meanwhile is buffered for that topic and its partition paused until that topic is polled. A message the backend still holds (handed out and not settled) is never handed out twice, even when a rebalance fetches it again. A message carries key, payload, headers, topic and a delivery tag. Header names and values are UTF-8 strings, or octets when they are not UTF-8. |
| `ack msg` | Settles that message only, as in RabbitMQ and the in-memory backend, so a worker pool can ack in any order. The group's offset is committed (synchronously) up to the earliest message of the partition this backend fetched and has not settled, or past the last settled one when none is left, and it never moves back. A message acked while an earlier one is still out is committed once that one is settled; if the member leaves first, the next member of the group gets both again (at least once). A commit the broker refuses because a rebalance is under way (`REBALANCE_IN_PROGRESS`, `ILLEGAL_GENERATION`, common with `cooperative-sticky`) still counts: `ack` returns, and the offset is committed in the next rebalance callback (before the revocation, and at the assignment) or by the next ack of the partition. Any other refused commit signals `mq-error` with the message still unsettled, so `ack` can be retried. |
| `nack msg :requeue t` | Seeks the partition back to the message: it comes again, marked `redelivered-p`, then the later messages of its partition that were not handed out yet. Later messages still held, or already settled, are not repeated; two nacks in a row seek back to the earlier one. `:requeue nil` settles it like `ack`. |
| rebalances | The driver assigns and revokes in its own rebalance callback, for eager assignors (librdkafka's default, range / roundrobin) and for `cooperative-sticky` (`:config '(("partition.assignment.strategy" . "cooperative-sticky"))`). Commits go only to partitions this member owns. `ack` or `nack` of a message whose partition was given to another member since it was fetched settles nothing and commits nothing: it signals `kafka-partition-revoked` (an `mq-error`; the `continue` restart returns), and the partition's new owner delivers the message. librdkafka's offsets store refuses an unassigned partition the same way. At each assignment the driver asks the group for the committed offsets of the assigned partitions (`rd_kafka_committed`) and keeps them as the known commit, moved by its own commits. An eager rebalance that gives a partition straight back, with the group's commit still the known one, keeps the messages the backend holds. What was settled is committed before `rd_kafka_assign` / `incremental_assign`, so the fetch starts past the acks; a re-fetched message below this member's own commit since the assignment is not handed out again. A commit refused with `UNKNOWN_MEMBER_ID` or `FENCED_INSTANCE_ID` (or `ILLEGAL_GENERATION` once librdkafka counts the assignment lost), or a `max.poll.interval.ms` timeout (`MAX_POLL_EXCEEDED`), means the assignment is lost: every partition is revoked, and that `ack` (and later ones of earlier messages) signals `kafka-partition-revoked`. |
| threads | Every operation may be called from any thread. `poll-messages` takes the backend's lock only for its bookkeeping, never across librdkafka's blocking poll, so `ack`, `nack`, `publish` and `kafka-flush` on other threads do not wait for a poll. Two polls of one backend take turns per slice of at most 200 ms. |
| delivery | at-least-once: `enable.auto.commit` is off, so a message that was never acked is delivered again to the next consumer of the group. |
| `close` | Waits for calls in progress on other threads (a poll stops within one 200 ms slice and signals `mq-connection-error`), then leaves the group (`rd_kafka_consumer_close`), flushes up to 10 s (not with `:abort t`) and destroys both handles. Later calls signal `mq-connection-error`. `close` from a handler of a condition signalled inside a call on the same thread waits for nothing: the last call in progress, on whichever thread, releases the handles when it ends. |

`subscribe` (push) is not offered by the librdkafka driver: `mq-backend-capabilities` reports `subscribe-p nil`, and `subscribe` signals `mq-error` with the usual `use-value` / `retry` restarts. librdkafka errors (configuration, commit, seek) are `mq-error`s with librdkafka's text. Without librdkafka every operation signals `mq-error` "librdkafka not loaded".

`librdkafka-version` / `librdkafka-loaded-p` report the loaded library.

## Native library

The `(:native-library "rdkafka")` component ([asdf-native](https://github.com/egao1980/asdf-native)) loads, by absolute path: `$MQ_BACKEND_KAFKA_NATIVE`, then `native/` (the overlay of an installed package), then `lib/<os>-<arch>/` (a checkout), else the system librdkafka. On Windows the DLLs librdkafka.dll links are loaded first from the same directory. No `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH`.

Overlays: linux amd64 / arm64, darwin arm64, windows amd64. `scripts/fetch-librdkafka.sh [os-arch … | all]` stages them from librdkafka's release builds (the `librdkafka.redist` package Confluent publishes, pinned by version and SHA-256; **librdkafka 2.16.0**). On Linux and macOS the dependencies (OpenSSL, zlib, zstd, lz4, …) are linked statically; linux-amd64 uses the CentOS 8 build, which needs only glibc 2.28+. Licenses: `NOTICE`, and `LICENSES.librdkafka.txt` in each overlay. Publish: `.github/workflows/publish-oci.yml` (tag `v*` or dispatch).

## Tests

`cl-repo test` after `scripts/fetch-librdkafka.sh`. The live tests skip without a broker on `127.0.0.1:19094` (`MQ_KAFKA_TEST_BOOTSTRAP`); `tests/driver-test.lisp` has the `docker run` line for `apache/kafka-native:3.9.0`.

## License

MIT — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

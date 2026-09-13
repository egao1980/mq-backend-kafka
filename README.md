# mq-backend-kafka

[`mq-protocol`](https://github.com/egao1980/mq-protocol) backend skeleton for **Apache Kafka** via [librdkafka](https://github.com/confluentinc/librdkafka). Wave 1 ships the CLOS class and a **driver-injection** seam so tests never `dlopen` a missing `.so`.

```lisp
(asdf:load-system "mq-backend-kafka")

(mq-backend-kafka:make-kafka-backend
 :bootstrap-servers "localhost:9092"
 :cffi-library-path nil)   ; reserved for the overlay path

;; Without a driver, GFs signal MQ-ERROR "librdkafka not loaded"
;; (restarts: USE-VALUE, RETRY). Tests inject a plist of functions:
(let ((b (mq-backend-kafka:make-kafka-backend
          :driver (list :publish (lambda (backend dest msg) msg)
                        :poll (lambda (backend dest &key n timeout) nil)))))
  (stack-mq:publish b "t" (stack-mq:make-mq-message :payload #(1))))
```

Slots: `bootstrap-servers`, `cffi-library-path`, `driver`. **Do not** call `cffi:load-foreign-library` from this repo until the overlay exists — a missing library breaks CI.

## Native overlay (wave-1 follow-up)

Same pattern as [`llama-cpp`](https://github.com/egao1980/llama-cpp) / `vllm-cpp`:

1. Publish natives from the **owning** repo with `publish-oci.yml` (`:cl-repo` overlays in the `.asd`: `:cffi-libraries ("rdkafka")`, per-platform `lib/<os>-<arch>/`).
2. At ASDF load, **absolute-path** `cffi:load-foreign-library` of the staged `librdkafka` (probe overlay dir, `truename`, load that path). Push the dir onto `cffi:*foreign-library-directories*`. Never `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH`.
3. Consumers `(asdf:load-system "mq-backend-kafka")` — no `load-*` / `ensure-*` helper.
4. Parity canary vs dockerized Redpanda.

This checkout does **not** copy native binaries.

## License

MIT — see [LICENSE](LICENSE).

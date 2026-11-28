(in-package #:mq-backend-kafka)

;;; librdkafka C API, the part the driver uses. The library itself is loaded
;;; by the (:native-library "rdkafka") component before this file: the
;;; overlay's native/, lib/<os>-<arch>/ in a checkout, else the system one.

(defconstant +rd-kafka-producer+ 0)
(defconstant +rd-kafka-consumer+ 1)
(defconstant +rd-kafka-partition-ua+ -1)
(defconstant +rd-kafka-msg-f-copy+ 2)
(defconstant +rd-kafka-vtype-topic+ 1)
(defconstant +rd-kafka-vtype-value+ 4)
(defconstant +rd-kafka-vtype-key+ 5)
(defconstant +rd-kafka-vtype-msgflags+ 7)
(defconstant +rd-kafka-vtype-headers+ 10)
(defconstant +rd-kafka-resp-err-queue-full+ -184)
(defconstant +rd-kafka-resp-err-timed-out+ -185)
(defconstant +rd-kafka-resp-err-partition-eof+ -191)

;;; rd_kafka_vu_t: an int vtype, padding, then a 64-octet union.
(defconstant +rd-kafka-vu-size+ 72)
(defconstant +rd-kafka-vu-union+ 8)

(cffi:defcstruct rd-kafka-message
  (err :int)
  (rkt :pointer)
  (partition :int32)
  (payload :pointer)
  (len :size)
  (key :pointer)
  (key-len :size)
  (offset :int64)
  (private :pointer))

(cffi:defcstruct rd-kafka-topic-partition
  (topic :pointer)
  (partition :int32)
  (offset :int64)
  (metadata :pointer)
  (metadata-size :size)
  (opaque :pointer)
  (err :int)
  (private :pointer))

(cffi:defcfun ("rd_kafka_version" %rd-version) :int)
(cffi:defcfun ("rd_kafka_version_str" %rd-version-str) :string)
(cffi:defcfun ("rd_kafka_err2str" %rd-err2str) :string (err :int))

(cffi:defcfun ("rd_kafka_conf_new" %rd-conf-new) :pointer)
(cffi:defcfun ("rd_kafka_conf_set" %rd-conf-set) :int
  (conf :pointer) (name :string) (value :string)
  (errstr :pointer) (errstr-size :size))
(cffi:defcfun ("rd_kafka_conf_destroy" %rd-conf-destroy) :void (conf :pointer))

(cffi:defcfun ("rd_kafka_new" %rd-new) :pointer
  (type :int) (conf :pointer) (errstr :pointer) (errstr-size :size))
(cffi:defcfun ("rd_kafka_destroy" %rd-destroy) :void (rk :pointer))
(cffi:defcfun ("rd_kafka_poll" %rd-poll) :int (rk :pointer) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_flush" %rd-flush) :int (rk :pointer) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_poll_set_consumer" %rd-poll-set-consumer) :int (rk :pointer))

(cffi:defcfun ("rd_kafka_produceva" %rd-produceva) :pointer
  (rk :pointer) (vus :pointer) (cnt :size))
(cffi:defcfun ("rd_kafka_error_code" %rd-error-code) :int (error :pointer))
(cffi:defcfun ("rd_kafka_error_string" %rd-error-string) :string (error :pointer))
(cffi:defcfun ("rd_kafka_error_destroy" %rd-error-destroy) :void (error :pointer))

(cffi:defcfun ("rd_kafka_headers_new" %rd-headers-new) :pointer (initial-count :size))
(cffi:defcfun ("rd_kafka_header_add" %rd-header-add) :int
  (hdrs :pointer) (name :pointer) (name-size :long)
  (value :pointer) (value-size :long))
(cffi:defcfun ("rd_kafka_headers_destroy" %rd-headers-destroy) :void (hdrs :pointer))
(cffi:defcfun ("rd_kafka_message_headers" %rd-message-headers) :int
  (rkmessage :pointer) (hdrsp :pointer))
(cffi:defcfun ("rd_kafka_header_cnt" %rd-header-cnt) :size (hdrs :pointer))
(cffi:defcfun ("rd_kafka_header_get_all" %rd-header-get-all) :int
  (hdrs :pointer) (idx :size) (namep :pointer) (valuep :pointer) (sizep :pointer))

(cffi:defcfun ("rd_kafka_topic_name" %rd-topic-name) :string (rkt :pointer))
(cffi:defcfun ("rd_kafka_topic_partition_list_new" %rd-tpl-new) :pointer (size :int))
(cffi:defcfun ("rd_kafka_topic_partition_list_add" %rd-tpl-add) :pointer
  (rktparlist :pointer) (topic :string) (partition :int32))
(cffi:defcfun ("rd_kafka_topic_partition_list_destroy" %rd-tpl-destroy) :void
  (rktparlist :pointer))

(cffi:defcfun ("rd_kafka_subscribe" %rd-subscribe) :int (rk :pointer) (topics :pointer))
(cffi:defcfun ("rd_kafka_consumer_poll" %rd-consumer-poll) :pointer
  (rk :pointer) (timeout-ms :int))
(cffi:defcfun ("rd_kafka_message_destroy" %rd-message-destroy) :void (rkmessage :pointer))
(cffi:defcfun ("rd_kafka_commit" %rd-commit) :int
  (rk :pointer) (offsets :pointer) (async :int))
(cffi:defcfun ("rd_kafka_seek_partitions" %rd-seek-partitions) :pointer
  (rk :pointer) (partitions :pointer) (timeout-ms :int))

(cffi:defcfun ("rd_kafka_pause_partitions" %rd-pause-partitions) :int
  (rk :pointer) (partitions :pointer))

(cffi:defcfun ("rd_kafka_resume_partitions" %rd-resume-partitions) :int
  (rk :pointer) (partitions :pointer))
(cffi:defcfun ("rd_kafka_consumer_close" %rd-consumer-close) :int (rk :pointer))

;;; Consumer group rebalances: the driver's own rebalance callback assigns
;;; and revokes, so it knows which partitions this member owns.
(defconstant +rd-kafka-resp-err-revoke-partitions+ -174)

(defconstant +rd-kafka-resp-err-assign-partitions+ -175)

(defconstant +rd-kafka-offset-invalid+ -1001)

;;; Commit refusals: a rebalance is under way (retry once it is done), or
;;; the group no longer knows this member (its assignment is lost).
(defconstant +rd-kafka-resp-err-illegal-generation+ 22)

(defconstant +rd-kafka-resp-err-unknown-member-id+ 25)

(defconstant +rd-kafka-resp-err-rebalance-in-progress+ 27)

(defconstant +rd-kafka-resp-err-fenced-instance-id+ 82)

;;; A consumer event: max.poll.interval.ms ran out, librdkafka left the group.
(defconstant +rd-kafka-resp-err-max-poll-exceeded+ -147)

;;; rd_kafka_committed failures worth one more try: the coordinator is not
;;; known yet, moving, or loading its offsets, or the request timed out.
(defconstant +rd-kafka-resp-err-wait-coord+ -180)

(defconstant +rd-kafka-resp-err-transport+ -195)

(defconstant +rd-kafka-resp-err-coordinator-load-in-progress+ 14)

(defconstant +rd-kafka-resp-err-coordinator-not-available+ 15)

(defconstant +rd-kafka-resp-err-not-coordinator+ 16)

(cffi:defcstruct rd-kafka-topic-partition-list
  (cnt :int)
  (size :int)
  (elems :pointer))

(cffi:defcfun ("rd_kafka_conf_set_rebalance_cb" %rd-conf-set-rebalance-cb) :void
  (conf :pointer) (rebalance-cb :pointer))

(cffi:defcfun ("rd_kafka_conf_set_opaque" %rd-conf-set-opaque) :void
  (conf :pointer) (opaque :pointer))

(cffi:defcfun ("rd_kafka_assign" %rd-assign) :int (rk :pointer) (partitions :pointer))

(cffi:defcfun ("rd_kafka_incremental_assign" %rd-incremental-assign) :pointer
  (rk :pointer) (partitions :pointer))

(cffi:defcfun ("rd_kafka_incremental_unassign" %rd-incremental-unassign) :pointer
  (rk :pointer) (partitions :pointer))

(cffi:defcfun ("rd_kafka_rebalance_protocol" %rd-rebalance-protocol) :string (rk :pointer))

(cffi:defcfun ("rd_kafka_assignment_lost" %rd-assignment-lost) :int (rk :pointer))

(cffi:defcfun ("rd_kafka_committed" %rd-committed) :int
  (rk :pointer) (partitions :pointer) (timeout-ms :int))

(defun librdkafka-loaded-p ()
  "True when librdkafka's symbols resolve in this image."
  (and (cffi:foreign-symbol-pointer "rd_kafka_version") t))

(defun librdkafka-version ()
  "The loaded librdkafka's version string (\"2.16.0\"), or NIL."
  (and (librdkafka-loaded-p) (%rd-version-str)))

# Messaging

The messaging crate defines broker-neutral envelopes, serialization boundaries, producer and
consumer contracts, delivery ownership, acknowledgements, retry decisions, dead-letter data, and
capability discovery.

No payload codec or broker is mandatory. MessageCodec converts typed values to a media type and
opaque bytes. MessageMetadata accepts binary values and requires portable namespaced keys such as
myapp.tenant or plenora.trace.traceparent. Debug output redacts payload bytes, typed payloads, and
metadata values. The portable map is fail-closed at 64 entries, 128 bytes per key, 8 KiB per value,
and 32 KiB combined key/value bytes.

Worker-bound messages use canonical metadata keys `plenora.message.id`,
`plenora.trace.correlation_id`, and optional `plenora.message.causation_id`. The first two are
required UUIDs for `MetadataMessageDecoder`; invalid identity metadata is rejected before handler
execution.

Delivery owns its adapter acknowledgement capability. Calling ack or nack consumes the delivery,
which makes a second acknowledgement impossible through the safe API.

RetryPolicy receives the current one-based attempt and a classified error. The standard
ExponentialBackoff policy caps delays, attempts, and an optional elapsed-time budget. Seeded jitter
is deterministic. OutcomeUnknown remains distinct from confirmed publication and is never
converted into success or retry unless the application explicitly opts into retry.

BrokerCapabilities makes durable consumption, replay, ordering, native dead-letter support, and
exactly-once claims discoverable without promising features that an adapter cannot provide.

`DeadLetterSink` turns any dedicated `MessageProducer` into portable dead-letter routing. It adds
bounded reason, attempt, failure-time, and deterministic `<message-id>.dlq` metadata without
changing the payload. The distinct DLQ ID prevents JetStream stream-wide de-duplication from
mistaking the DLQ record for the original publication. `Confirmed` and `OutcomeUnknown` remain
separate all the way back to the consumer adapter.

`plenora-runtime-nats` implements the contracts with a fixed-subject JetStream producer and a
durable pull consumer. Production-oriented configuration requires TLS by default and binds to
pre-provisioned infrastructure; local plaintext and resource creation are explicit opt-ins.
Publication is confirmed only after the JetStream acknowledgement resolves. Ambiguous post-send
failures return `OutcomeUnknown`. Positive delivery acknowledgement uses server-confirmed double
ACK; retryable work maps to NAK, `RetryAfter` preserves its delay in a native delayed NAK,
permanent rejection maps to TERM, and shutdown maps to the configured delayed NAK.

JetStream reports no native DLQ capability. The Apalis broker runner emulates it explicitly using
a fixed-subject producer dedicated to a provisioned DLQ subject. It sends TERM for the original
delivery only after confirmed DLQ publication. Missing configuration, publication failure, or
`OutcomeUnknown` leaves the original unacknowledged and eligible for redelivery; a TERM failure
after confirmed publication is reported separately.

Operational and replay consumers require finite nonzero `max_deliver`, `max_ack_pending`, and local
payload-byte limits. Binding checks the existing durable's acknowledgement timeout, delivery and
pending-ack bounds, filter, ACK policy, and delivery policy before consumption starts. Oversized
incoming messages are terminated without exposing their bytes to a handler.

Binary metadata is encoded reversibly into reserved NATS headers. Replay configuration names both
the operational and replay durable and rejects equality, so replay cannot move or overwrite the
operational consumer cursor. The adapter advertises
only durable-consumer and replay capabilities. Connection events update health/readiness;
controlled reconnect preserves the durable consumer, and graceful drain is completed by polling
consumer streams until they end after `begin_drain`.

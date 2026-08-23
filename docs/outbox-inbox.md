# Outbox, inbox, and idempotency

The first milestone provides persistence-neutral traits and deterministic fake stores. A business
write and its outbox append must ultimately share one transaction; processing and inbox recording
must likewise be atomic before acknowledgement.

OutboxRelay claims one bounded batch and publishes through MessageProducer. Confirmed publication
is marked published. A producer error is recorded according to policy and returned with its source.
OutcomeUnknown is retained for reconciliation by default; retrying it requires an explicit opt-in
because the remote effect may already have happened. A run that returns early releases every claim
it never attempted as `NotAttempted`/Retry, so a mid-batch failure cannot park later entries until a
store-specific lease expires; release itself is best effort, an entry whose release also fails is
counted in `unreleased_claims`, and a run cancelled by dropping its future releases nothing. Those
entries remain the store's claim-lease or crash-recovery responsibility.

The first store trait intentionally does not prescribe a lease model. Concrete adapters must define
claim expiry and crash recovery. InboxDeduplicator is a convenience helper, not an atomicity
boundary: the inbox check, business effect, and processed record must share the consumer's durable
transaction.

Idempotency begin is atomic and distinguishes execute, in-progress, conflict, and stored-result
states. The initial portable decision does not carry stored result bytes.

Concrete database persistence is deferred until plenora-database-tools is stable enough for an
adapter contract.

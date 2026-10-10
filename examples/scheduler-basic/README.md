# Scheduler di base

Questo esempio costruisce uno scheduler limitato, registra un piccolo comando ed esegue il dispatch
della sua occorrenza one-shot. Un consumer Plenora sostituisce `ExampleDispatcher` con un
dispatcher di proprietà dell'applicazione che pubblica su NATS o chiama un altro confine limitato
del runtime. L'id deterministico dell'occorrenza deve diventare la chiave di idempotenza a valle.

Eseguirlo con:

```text
cargo run -p plenora-runtime-example-scheduler-basic
```

# Wheel nativa sperimentale

plenora_runtime contiene Client, AsyncClient, ChainError e FUNCTIONS importato da Rust.
Le funzioni accettano keyword uguali ai campi di Request. plan e JSON grezzo:
i membri duplicati devono arrivare al parser Rust senza passare per un dizionario.

PyO3 rilascia il GIL; AsyncClient attende il metodo nativo su un worker Python.
Cancellare l'attesa non cancella il run. Le factory remote usano il client Rust NATS.
Gli iteratori eventi e la qualifica completa P1.5 restano aperti.

Build: `maturin build --manifest-path crates/plenora-runtime-chain-python/Cargo.toml`.
I test in tests/test_installed.py importano una wheel installata; discovery,
validazione e rifiuti locali non attestano i controlli su PostgreSQL/NATS reali.
Limiti: [ADR 0010](../../docs/adr/0010-scripted-chain-facades.md).

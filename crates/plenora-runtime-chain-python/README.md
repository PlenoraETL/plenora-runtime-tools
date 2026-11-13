# Wheel nativa sperimentale

plenora_runtime contiene Client, AsyncClient, ChainError e FUNCTIONS importato da Rust.
Le funzioni accettano keyword uguali ai campi di Request. plan e JSON grezzo:
i membri duplicati devono arrivare al parser Rust senza passare per un dizionario.

PyO3 rilascia il GIL; AsyncClient attende il metodo nativo su un worker Python.
Cancellare l'attesa non cancella il run. Le factory remote usano il client Rust NATS.
`Client.iter_events(run_id, cursor=0, limit=256)` e
`AsyncClient.iter_events(...)` enumerano le pagine disponibili usando EventCursor
Rust. Una pagina vuota termina l'enumerazione, senza attestare fine run. Il cursor
e la sequenza dell'ultimo evento consumato; per una nuova enumerazione usare
quella sequenza. Gli errori non avanzano il cursore della pagina.
`await AsyncClient.create(config)` valida e costruisce fuori dall'event loop;
`AsyncClient(config)` differisce la costruzione al primo await. Il config viene
consumato in quel momento: non modificarlo durante l'inizializzazione. Anche
costruzione, migrate e connessione espongono ChainError, con codice canonico,
category/phase/remote_effect/effect_scope/retry/run_id/domain_code e risultato
originale separato da str/repr. Le proiezioni sono implementate solo in Rust.
`resume(..., recovery="observe"|"continue"|"reconcile")` usa le stesse autorita
CLI/Rust; omissione equivale a reconcile. Il criterio 6 resta il passo successivo.
Dettagli e limiti in [interfaccia](../../docs/chain-interface.md).

Build: `maturin build --manifest-path crates/plenora-runtime-chain-python/Cargo.toml`.
I test in tests/test_installed.py importano una wheel installata; discovery,
validazione e rifiuti locali non attestano i controlli su PostgreSQL/NATS reali.
Limiti: [ADR 0010](../../docs/adr/0010-scripted-chain-facades.md).

Il job PostgreSQL installa wheel e binario e prova cancel/resume/eventi in un
database separato dalle prove di riduzione permanente della retention. Il test
e saltato se non sono disponibili URL PostgreSQL e binario installato: non vale
come qualifica locale del backend quando saltato.

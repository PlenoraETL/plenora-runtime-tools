# Contratti catene Rust

Tipi pubblici e validazione statica offline della bozza
[CHAIN-PLAN-2.0](../../docs/specs/CHAIN-PLAN-2.0.md). Il workspace include il
crate tramite `crates/*`. Dipende solo da serde/JSON, regex, SHA-256 e chrono
senza clock; non ha engine, resolver, trasporto, Python o database.

`parse_plan`, `parse_registry`, `parse_result`, `parse_event` e `parse_error`
sono gli ingressi wire chiusi: massimo 1 MiB e profondita 64, UTF-8 esatto,
duplicati rifiutati e nessuna normalizzazione dei numeri. I tipi pubblici sono
DTO costruibili e serializzabili, senza `Deserialize`: i DTO wire sono privati.
La costruzione Rust non costituisce ammissione; `validate_*` ricontrolla anche
la struttura e i limiti. `Debug` e `Rejection` non contengono dati del documento.

`validate_plan(&plan, &registry)` verifica il registro fornito indipendentemente,
restituisce ordine, digest, barriere schema dinamico e verdetto fidelity.
`reference_only` non e un token di esecuzione: payload, autorizzazione reale,
schema effettivo, conservazione Arrow, prenotazioni e discovery immutata devono
essere provati dall'host prima di invoke. `reported` non autorizza perdite e il
consenso DB non si propaga ad altri componenti. Non esiste un callback invoke.

`canonical_plan` e `plan_digest` coprono l'intero piano; `consent_input_digest`
lega binding e manifest iniziali. `validate_result` riconcilia esito e piano;
`validate_event` controlla il documento, non la sequenza storica nello store.

I tipi sono riproducibili con `python scripts/generate_chain_types.py` seguito
da `cargo fmt --all`. La generazione e uno strumento di sviluppo offline,
non una dipendenza di compilazione o esecuzione. Gli schemi locali sono inclusi
nel binario: il valutatore accetta solo il loro vocabolario e non carica URI.

Prove: `cargo test -p plenora-runtime-chain-contracts` esegue tutte le fixture enumerate nel manifest,
confronta i codici dell'oracolo e verifica roundtrip, limiti e regressioni oltre
le fixture. `python -m unittest discover -s scripts -p 'test_chain_contract.py'`
verifica separatamente la referenza Python sugli stessi attesi.

# plenora-runtime-tools

Fondamenta Rust riusabili per servizi e worker asincroni dell'ecosistema Plenora.
Il progetto è agnostico rispetto all'infrastruttura ai propri confini pubblici: le tecnologie
concrete di HTTP, worker, broker e persistenza stanno dietro gli adapter.

## Stato

Il repository contiene i contratti core della versione 0.1, un registro e un dispatcher generici
e limitati di capability, un testkit deterministico, fake dell'outbox, gli adapter Apalis e
NATS JetStream, e le fondamenta indipendenti dal backend per HTTP, osservabilità, schedulazione,
contenimento dei sottoprocessi e controllo operativo. Le API pubbliche non sono ancora stabili.
La specifica eseguibile è
[`plenora-runtime-tools_Agent_Implementation_Spec_v0.1.md`](plenora-runtime-tools_Agent_Implementation_Spec_v0.1.md).

## Workspace

I crate core stanno sotto `crates/`, le integrazioni concrete sotto `adapters/` e i consumer
eseguibili sotto `examples/`. Leggere [`docs/architecture.md`](docs/architecture.md) e
[`docs/boundaries.md`](docs/boundaries.md) prima di aggiungere dipendenze. La composizione HTTP e
la responsabilità della telemetria sono descritte in [`docs/http.md`](docs/http.md) e
[`docs/observability.md`](docs/observability.md). Stato, avanzamento, scadenze e cancellazione
per task sono specificati in [`docs/task-lifecycle.md`](docs/task-lifecycle.md); la capacità dei
worker e gli heartbeat di istanza sono descritti in [`docs/worker.md`](docs/worker.md).
La pressione generica sulla memoria di processo e l'ammissione reversibile sono descritte in
[`docs/resources.md`](docs/resources.md). La schedulazione limitata one-shot, a intervallo, cron
con fuso orario, con pausa/ripresa/avvio manuale e i cursori di ripartenza sono descritti in
[`docs/scheduler.md`](docs/scheduler.md).
Gli adapter di integrazione di proprietà dell'applicazione e il routing generico versionato sono
descritti in [`docs/capabilities.md`](docs/capabilities.md); il modello di adapter verificato in
compilazione e i test di contratto richiesti stanno in
[`docs/integrating-a-library.md`](docs/integrating-a-library.md). Il dimensionamento della capacità
dei worker, l'avvio, i segnali di runtime e le operazioni di arresto sono trattati in
[`docs/operations.md`](docs/operations.md).
I gate di qualificazione attuali e i rischi di sicurezza residui sono tracciati in
[`docs/qualification.md`](docs/qualification.md) e
[`docs/security-review.md`](docs/security-review.md).
Il pin immutabile dei contratti comuni, il confine di richiesta implementato e le lacune che
restano rispetto a Runtime Binding 1.0 sono tracciati in
[`docs/contract-alignment.md`](docs/contract-alignment.md) e
[`contracts/source.json`](contracts/source.json).
La copertura limitata di carico, cancellazione, ciclo di vita e soak su JetStream reale è
descritta in [`docs/stability.md`](docs/stability.md). La qualificazione di coverage e fuzz,
compresi il gate del 90% sulle righe applicato e i sei target di fuzz, è documentata in
[`docs/testing.md`](docs/testing.md).
La stessa guida ai test documenta la sonda sintetica di ritenzione della memoria limitata da
Docker e il suo controllo con leak intenzionale.
Il contenimento generico dei processi figli è documentato in
[`docs/subprocess-execution.md`](docs/subprocess-execution.md); nessuna libreria concreta viene
forzata in quella modalità prima che ne siano misurati il comportamento reale di memoria e di
cancellazione. La scoperta operativa senza payload e il controllo HTTP autorizzato sono descritti
in [`docs/control-plane.md`](docs/control-plane.md).

## Validazione locale

La toolchain di sviluppo fissata è Rust 1.98.1 e la MSRV è Rust 1.98
([ADR 0007](docs/adr/0007-toolchain-1-98-1-e-msrv-1-98.md)). Quando Rust non è
installato sull'host, usare l'helper Docker da PowerShell:

```powershell
./scripts/cargo-docker.ps1 fmt --all --check
./scripts/cargo-docker.ps1 check --workspace --all-targets
./scripts/cargo-docker.ps1 clippy --workspace --all-targets --all-features
./scripts/cargo-docker.ps1 test --workspace --all-features
```

L'harness di integrazione NATS usa un container effimero solo su loopback ed emette i dati di
connessione in JSON. Vedere [`tests/architecture/README.md`](tests/architecture/README.md).

Le composizioni eseguibili stanno sotto `examples/`: `worker-basic` mostra l'esecuzione limitata
indipendente dal motore e la classificazione dei retry; `worker-nats` collega NATS JetStream a un
registro generico di capability congelato tramite Apalis, con task di handler ammessi
dinamicamente, concorrenza massima esplicita, configurazione sicura rispetto alla redazione, TLS,
ACK/NAK, instradamento dead-letter confermato, health/readiness HTTP e un unico percorso di drain
coordinato;
`capability-adapter` è un modello verificato in compilazione per collegare qualunque libreria Rust
di proprietà dell'applicazione; `scheduler-basic` mostra il dispatch programmato limitato e
l'identità deterministica delle occorrenze; `runtime-benchmark` emette JSON riproducibile per il
coordinamento limitato di worker e scheduler;
`http-service` e `http-worker` mostrano composizioni più piccole solo HTTP. Un
test di accettazione deterministico fra crate esercita la stessa forma di processo senza
infrastruttura esterna.

## Licenza

Proprietaria, source available: il codice è visibile pubblicamente solo a fini di revisione, e la
visibilità non concede alcuna licenza. Vedere [`LICENSE`](LICENSE).

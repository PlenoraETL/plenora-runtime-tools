# Qualifica di stabilità

I test di stabilità affiancano alla correttezza funzionale un carico sostenuto limitato e
transizioni ripetute del ciclo di vita. I controlli di accettazione deterministici verificano
invarianti, non job al secondo; un microbenchmark separato in modalità release registra il
throughput di coordinamento senza trasformare numeri legati all'hardware in soglie di correttezza.

## Scenari sempre attivi

La suite normale del workspace esegue questi scenari su Windows, Linux e macOS:

- 2.048 messaggi del broker con `max_in_flight=32`; esattamente 32 handler diventano attivi, 2.016
  restano in coda, ogni messaggio riceve un solo ACK e non resta alcun task attivo né consegna;
- 64 task attivi contemporaneamente ricevono richieste di cancellazione concorrenti e rilasciano ogni
  voce del registro dei task e ogni permesso di esecuzione;
- 100 executor di worker costruiti in modo indipendente elaborano e svuotano ciascuno otto task
  concorrenti senza stato residuo;
- 1.000 cicli di pressione di memoria e recupero lasciano aperta l'ammissione reversibile del
  worker, e un fallimento del campionamento resta chiuso fino al recupero confermato;
- un arretrato dello scheduler di 10.001 occorrenze avanza per tick limitati senza materializzare una
  coda, mentre i percorsi di crash prima dell'effetto, esito sconosciuto, timeout e riavvio dal
  cursore restano deterministici;
- il consumer di capability in forma di processo continua a coprire dispatch generico, readiness
  HTTP, ACK e shutdown coordinato.

Ogni scenario ha un timeout a orologio. Le dimensioni di code, storico, registro, task, messaggi e
payload restano limitate in modo esplicito durante il test.

## Soak su JetStream reale

La suite NATS, ignorata per default, gira in CI nel container Docker effimero e pinnato già
esistente. Pubblica e conferma 256 messaggi con identificativi univoci, forza una riconnessione del
client a metà osservando `NotReady` e poi `Ready`, riceve ogni ACK con conferma del server, e
rifiuta perdite, duplicati, sequenze fuori intervallo o drain incompleto. L'intero scenario ha un
timeout di 60 secondi.

I test di stabilità in-process si eseguono direttamente:

```text
cargo test -p plenora-runtime-worker --test stability --locked -- --nocapture
cargo test -p plenora-runtime-apalis --test stability --locked -- --nocapture
cargo test -p plenora-runtime-resources --test pressure --locked -- --nocapture
cargo test -p plenora-runtime-scheduler --test scheduler --locked -- --nocapture
```

Il soak reale si esegue solo contro l'harness Docker in loopback:

```powershell
$status = ./scripts/nats-docker.ps1 Start | ConvertFrom-Json
try {
    $env:PLENORA_NATS_URL = $status.client_url
    cargo test -p plenora-runtime-nats --test real_nats --locked -- --ignored --nocapture
}
finally {
    ./scripts/nats-docker.ps1 Cleanup -RunId $status.run_id
}
```

Questi test rilevano regressioni di limitatezza, ciclo di vita, duplicati/perdite e recupero. Non
sostituiscono un soak lungo, simile alla produzione, sul consumer Plenora finale con i suoi veri
adapter di fondazione e la sua persistenza.

## Soak sintetico della memoria

`tests/memory` esercita un singolo worker Rust a lunga vita con allocazioni sintetiche effettive.
Copre plateau dell'allocatore, frammentazione, concorrenza massima configurata, errore dell'handler,
cancellazione per timeout e un'allocazione di controllo trattenuta di proposito. Il wrapper Docker
vincola l'intero processo e registra l'RSS su Linux senza esporre l'host a un esperimento di OOM
illimitato:

```powershell
./scripts/memory-soak.ps1 -AllocationMiB 64 -MaxInFlight 4 -ContainerMemory 1g
```

Il verde prova che le future dei task possedute dal runtime e i percorsi di cancellazione rilasciano
le loro allocazioni sintetiche e che il rilevatore di crescita riconosce un leak noto. Non prova che
librerie native non ancora completate restituiscano o riusino la memoria; i loro adapter reali
devono riusare questo harness prima di scegliere fra esecuzione in-process o isolata.

## Benchmark di coordinamento riproducibile

Eseguire un riscaldamento e almeno cinque campioni registrati sullo stesso host e con lo stesso
profilo energetico:

```text
cargo run --release -p plenora-runtime-example-benchmark -- \
  --iterations 20000 --max-in-flight 32
```

Il comando stampa un oggetto JSON con tempo trascorso e frequenze del worker e dello scheduler. Il
driver del worker tiene al più `max_in_flight` task Tokio nel suo `JoinSet`; lo scheduler conserva
un solo cursore invece di creare una coda di occorrenze. Registrare SHA del commit, CPU, sistema
operativo, versione di Rust e mediana. Questo benchmark isola il coordinamento del runtime e non va
presentato come throughput atteso di NATS, del database, di Python o delle librerie reali.

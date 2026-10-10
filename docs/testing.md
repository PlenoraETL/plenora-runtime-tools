# Qualifica di coverage e fuzz

Il repository tratta la coverage come un gate di qualifica che fallisce, non come un badge
informativo. La CI usa `cargo-llvm-cov 0.8.7` con Rust 1.98.1 e fallisce quando la coverage di riga
unita scende sotto il 90%.

## Ambito della coverage

Il profilo misurato comprende tutti i crate di produzione e gli adapter, le loro suite di test
normali, i test ignorati contro il server NATS JetStream effimero e pinnato, il test di dead-letter
da NATS ad Apalis e il test completo del worker su capability e recupero dopo crash. Gli esempi
eseguibili e i pacchetti di harness di architettura/integrazione
sono esclusi dall'invocazione normale sul workspace; il binario di integrazione pertinente si
aggiunge poi in modo esplicito. `cargo-llvm-cov` esclude per default dal report i file sorgente di
test a sé stanti.

La CI costruisce il profilo unito in quest'ordine:

```text
cargo llvm-cov clean --workspace
cargo llvm-cov --workspace <documented package exclusions> --locked --no-report
cargo llvm-cov -p plenora-runtime-nats --test real_nats --locked --no-report -- --ignored
cargo llvm-cov -p plenora-runtime-integration-tests --test nats_worker_dlq --locked --no-report -- --ignored
cargo llvm-cov -p plenora-runtime-integration-tests --test nats_capability_e2e --locked --no-report -- --ignored
cargo llvm-cov report --summary-only --fail-under-lines 90
```

La soglia è di proposito sulla coverage di riga. La coverage dei rami non si dichiara perché la
strumentazione stabile di Rust usata da questo workspace oggi non la riporta.

## Target di fuzz

Il workspace indipendente `fuzz/` pinna `cargo-fuzz 0.13.2`, `libfuzzer-sys 0.4.13` e
`nightly-2026-08-01`. I suoi sei target esercitano:

- limiti, validazione e mutazione atomica dei metadati dei messaggi;
- gli input del codec di routing delle capability;
- i calcoli del retry esponenziale su tentativi e durate estremi;
- la validazione della configurazione NATS di connessione, producer, consumer e replay;
- l'iniezione e l'estrazione del carrier di propagazione W3C.
- identificatori di componente/pianificazione del controllo, piani cron/fuso orario, jitter
  deterministico e limiti delle specifiche dei sottoprocessi.

I corpus di partenza sono versionati sotto `fuzz/corpus/`. I crash e l'output di build locale
restano ignorati sotto `fuzz/artifacts/` e `fuzz/target/`.

Su un host di tipo Unix con la nightly pinnata e `cargo-fuzz` installati, un target si esegue con:

```text
cargo +nightly-2026-08-01 fuzz run retry_policy fuzz/corpus/retry_policy -- -max_total_time=60 -timeout=5 -max_len=32768
```

Su Windows, si esegue lo stesso comando in un container Linux o in WSL, perché libFuzzer richiede
un ambiente di tipo Unix. La CI esegue 2.000 iterazioni di smoke per ogni target a ogni push e pull
request. `.github/workflows/fuzz.yml` esegue campagne con budget di tempo su una copia di ciascun
corpus: 60 secondi per target sulle pull request che toccano `fuzz/`, 600 secondi per target ogni
domenica e nel dispatch manuale (input `seconds`). Gli input che causano crash si conservano come
artefatti dell'esecuzione; ogni input di regressione minimizzato va tenuto nel corpus.

## Qualifica della ritenzione di memoria

La suite normale del workspace include piccoli contratti deterministici che provano che successo,
fallimento dell'handler, timeout ed esecuzione concorrente limitata rilasciano le allocazioni
sintetiche possedute dai task. La sonda dell'RSS su Linux è di proposito opzionale, perché il
comportamento dell'allocatore e la contabilità della memoria di processo dipendono dall'ambiente.

La sonda completa si esegue da PowerShell:

```powershell
./scripts/memory-soak.ps1
```

Costruisce una volta la sonda a lunga vita, poi esegue gli scenari di plateau, frammentazione,
concorrenza, errore, cancellazione e leak voluto in container Docker senza rete e con un limite di
memoria rigido. I campioni CSV e un manifesto JSON dell'esecuzione si scrivono sotto
`target/memory-soak/`.

Il classificatore confronta l'RSS mediano del primo e dell'ultimo quartile dopo il riscaldamento. Un
plateau dell'allocatore alto ma stabile passa; una crescita trattenuta monotona oltre la tolleranza
configurata fallisce. Il leak voluto deve fallire con il codice d'uscita dedicato alla crescita,
altrimenti il wrapper rifiuta l'esecuzione. Questo qualifica solo il runtime e il rilevatore. Ogni
adapter verso una libreria concreta dovrà poi ripetere lo stesso carico di lunga durata con
operazioni rappresentative della libreria.

## Resilienza delle risorse e dello scheduler

La suite sempre attiva esercita campionamento fail-closed, isteresi dell'ammissione, 1.000 cicli di
pressione/recupero, recupero limitato di un arretrato di 10.001 occorrenze, timeout del dispatch,
retry sicuro dopo un crash prima dell'avvio, riconciliazione dell'esito sconosciuto, ripristino del
cursore dopo il riavvio e shutdown dello scheduler:

```text
cargo test -p plenora-runtime-resources --all-targets --locked
cargo test -p plenora-runtime-scheduler --all-targets --locked
```

Sono test indipendenti dal backend. Il recupero durevole dello scheduler e del ciclo di vita va
ripetuto attraverso il futuro adapter di `database-tools` prima che l'API del consumer si congeli.

Cron/fuso orario, pausa/ripresa, saturazione del trigger manuale, ciclo di vita dei sottoprocessi e
routing autorizzato del piano di controllo sono coperti dalle suite normali dei rispettivi crate:

```text
cargo test -p plenora-runtime-subprocess --all-targets --locked
cargo test -p plenora-runtime-control --all-targets --locked
cargo test -p plenora-runtime-control-http --all-targets --locked
```

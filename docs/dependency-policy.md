# Policy delle dipendenze

Le dipendenze condivise si dichiarano in modo centralizzato nel workspace. `Cargo.lock` è
committato, i requisiti con wildcard sono vietati e le versioni duplicate si rivedono.

Le dipendenze git richiedono un ADR, un commit pinnato e una motivazione documentata. I nuovi client
di infrastruttura richiedono un ADR e devono restare dietro un confine di adapter o di trait. Le
feature di default vanno rivedute, non accettate implicitamente.

La CI esegue release pinnate di `cargo audit` e `cargo deny check` su entrambi i lockfile (il
workspace e il workspace separato `fuzz/`) a ogni push e pull request, e ogni settimana attraverso
`supply-chain.yml`. Le GitHub Actions sono pinnate per SHA completo del commit, con il tag di
release in un commento (per `dtolnay/rust-toolchain`, che non ha release per toolchain, il
commento dice che lo SHA è un commit dell'action e non la versione di Rust); gli strumenti della CI si installano a versioni esatte. Ogni pin esatto in
`Cargo.toml` e `fuzz/Cargo.toml` porta accanto la sua motivazione. Le eccezioni di licenza, di
advisory e di sorgente devono essere strette, documentate e, dove possibile, limitate nel tempo.

La qualifica di rilascio si avvia con uno SHA di commit atteso esplicito. Tutti gli altri job di
`ci.yml` (qualità sui tre sistemi operativi, con la documentazione, anti-panic, policy delle
dipendenze, NATS reale, coverage e fuzz smoke) dipendono da quel
job di verifica e qualificano quindi la stessa revisione estratta, con la toolchain pinnata Rust
1.98.1; la MSRV dichiarata è Rust 1.98 (ADR 0007).


## Licenze transitive dei provider reali

Eccezioni strette per crate in deny.toml, valutate sulle versioni del lock S3.
Non ampliano la allowlist generale. Rivederle a ogni aggiornamento di questi
pacchetti e rimuoverle quando la dipendenza esce dal grafo.

| Crate/versione | Licenza dichiarata | Necessita e testo conservato |
| --- | --- | --- |
| borrow-or-share 0.2.4 | MIT-0 | referencing/jsonschema del driver GeoParquet IO; [testo](licenses/borrow-or-share-0.2.4.txt) |
| tiny-keccak 2.0.2 | CC0-1.0 | const-random/ahash del grafo Arrow; [testo](licenses/tiny-keccak-2.0.2.txt) |
| webpki-roots 1.0.10 | CDLA-Permissive-2.0 | root TLS del provider PostgreSQL gia adottato; [testo](licenses/webpki-roots-1.0.10.txt) |

I testi provengono dai pacchetti esatti verificati nel Cargo.lock; il testo CDLA
va reso disponibile insieme ai dati distribuiti. L'approvazione del grafo non
sostituisce l'inclusione dei testi dovuti negli artefatti di distribuzione.
Audit offline con database locale e yanked disabilitato e solo evidenza parziale:
restano obbligatori audit/deny aggiornati della CI e del job settimanale.

## Manifesto di famiglia

La baseline immutabile in `.ci/plenora-dependencies/family-baseline.toml` proviene
dalla qualifica database del 19/11/2026. `scripts/manifesto_dipendenze.py --write`
genera `plenora-dependencies.toml` dai due lock e da Cargo metadata con tutte le
feature; la modalita ordinaria verifica la riproducibilita senza scrivere.
Il guardiano `controlla_dipendenze.py --repo . --nome plenora-runtime-tools`
confronta versioni, fonti, interfacce e duplicati con le decisioni di famiglia.
La CI verifica anche lo snapshot con SHA-256 esplicito nel workflow.

Le qualifiche descrittive non autorizzano dipendenze. Le decisioni esplicite in
[`manifesto_deroghe.json`](../scripts/manifesto_deroghe.json) raccolgono una voce
per crate e i singoli archi identificati dal fingerprint del guardiano. Lo script
espande soltanto i 68 archi transitivi incompatibili e le due identita fork
approvati, nel formato delle deroghe di famiglia: motivazione, percorso verificato
nel lock, profili, ambito feature/target, proprietario, rimozione e riesame entro
il 2027-02-13. Nuovi genitori, versioni o provenienze restano errori; le deroghe
cessate non vengono emesse. La baseline e il guardiano non cambiano.

Il riesame dei 72 archi duplicati ha riallineato quattro vincoli larghi con
`cargo update -p tempfile --precise 3.27.0 --offline`: tempfile usa getrandom
0.4.3; errno, rustix 1.1.6 e tempfile usano windows-sys 0.61.2. Versioni di
pacchetto, pin diretti e lock fuzz invariati. I genitori degli altri archi sono
gia ai riferimenti applicabili della baseline; le linee preferite non soddisfano
i loro requisiti, oppure sono archi opzionali conservati dal lock completo.
La classificazione (b) riguarda questi requisiti e le versioni qualificate nella
famiglia, non afferma l'assenza universale di release upstream piu recenti.

Restano due errori bloccanti, senza deroga: encoding_rs 0.8.35 e zip 9.0.3,
entrambi con condizione **chiude con IO 10.0.0**. La presenza dei due fork Parquet
resta una garanzia di unificazione indebolita: si conservano patch di correttezza
e identita distinte finche i prodotti proprietari migrano dopo riqualifica.

Nessuna nuova licenza viene ammessa. Le 24 dipendenze private di prodotto senza
SPDX restano soggette alla regola gia presente `licenses.private.ignore = true`:
la qualifica registra separatamente tale esclusione e il digest del file di
licenza quando disponibile. Non sono dichiarate licenze SPDX ammesse. Tutte le
licenze pubbliche risolte soddisfano la policy vigente. L'eccezione
RUSTSEC-2026-0548 resta fino all'adozione di IO 10.0.0; il pin IO non cambia.

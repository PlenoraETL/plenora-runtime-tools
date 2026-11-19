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

Le qualifiche descrittive non autorizzano dipendenze: nessuna nuova deroga e
stata aggiunta. Le deroghe cessate sono rimosse dal manifesto generato, mentre
la baseline rimane intatta. L'allineamento dei lock e dei pin diretti sha2 0.11.0
e syn 3.0.6 lascia 76 errori bloccanti (72 archi duplicati, due fork e due
versioni imposte da IO). La chiusura richiede allineamento upstream oppure una
decisione esplicita di famiglia, registrata e revisionata; non basta rigenerare.

Nessuna nuova licenza viene ammessa. Le 24 dipendenze private di prodotto senza
SPDX restano soggette alla regola gia presente `licenses.private.ignore = true`:
la qualifica registra separatamente tale esclusione e il digest del file di
licenza quando disponibile. Non sono dichiarate licenze SPDX ammesse. Tutte le
licenze pubbliche risolte soddisfano la policy vigente. L'eccezione
RUSTSEC-2026-0548 resta fino all'adozione di IO 10.0.0; il pin IO non cambia.

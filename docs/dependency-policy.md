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

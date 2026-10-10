# Policy delle dipendenze

Le dipendenze condivise si dichiarano in modo centralizzato nel workspace. `Cargo.lock` è
committato, i requisiti con wildcard sono vietati e le versioni duplicate si rivedono.

Le dipendenze git richiedono un ADR, un commit pinnato e una motivazione documentata. I nuovi client
di infrastruttura richiedono un ADR e devono restare dietro un confine di adapter o di trait. Le
feature di default vanno rivedute, non accettate implicitamente.

La CI esegue release pinnate di `cargo audit` e `cargo deny check` su entrambi i lockfile (il
workspace e il workspace separato `fuzz/`) a ogni push e pull request, e ogni settimana attraverso
`supply-chain.yml`. Le GitHub Actions sono pinnate per SHA completo del commit, con il tag di
release in un commento; gli strumenti della CI si installano a versioni esatte. Ogni pin esatto in
`Cargo.toml` e `fuzz/Cargo.toml` porta accanto la sua motivazione. Le eccezioni di licenza, di
advisory e di sorgente devono essere strette, documentate e, dove possibile, limitate nel tempo.

La qualifica di rilascio si avvia con uno SHA di commit atteso esplicito. Tutti i job dei sistemi
operativi, della documentazione, della policy delle dipendenze e di NATS reale dipendono da quel
job di verifica e qualificano quindi la stessa revisione estratta, con la toolchain pinnata Rust
1.98.1; la MSRV dichiarata è Rust 1.98 (ADR 0007).

# Allineamento delle dipendenze per 0.1.1

Verifica locale del 9 novembre 2026 su Windows, Rust 1.98.1,
`CARGO_BUILD_JOBS=3`, senza commit. I manifest dei crate restano a 0.1.0.
Non esisteva un changelog del prodotto: la prima voce prepara la successiva
versione correttiva. Il changelog in `contracts/upstream/` appartiene ai contratti
vendorizzati e non viene modificato.

## Versioni e grafo

| Dipendenza | Prima | Dopo | Ambito |
| --- | --- | --- | --- |
| serde_json | 1.0.151 | 1.0.154 | Pin workspace e lock radice/fuzz |
| thiserror / thiserror-impl | 2.0.20 | 2.0.21 | Lock radice/fuzz |
| tokio-rustls | 0.26.4 | 0.26.5 | Lock radice/fuzz |
| hyper | 1.11.0 | 1.11.1 | Solo lock radice; assente da fuzz |
| h2 | assente | assente | Nessuna dipendenza artificiale aggiunta |
| tokio | 1.53.3 | 1.53.3 | Pin esatto invariato |
| Rust | 1.98.1 | 1.98.1 | Toolchain invariata; MSRV 1.98 |

Sono gli unici cambiamenti di versione nei lock. Il resolver aveva anche
spostato il collegamento errno/windows-sys tra due versioni gi? presenti;
il collegamento originale a windows-sys 0.61.2 ? stato conservato e
riverificato con `--locked`, evitando una modifica estranea al perimetro. Nessun lock vendor, maturin,
PyO3 o SDK Python nel repository. Le feature dirette di serde_json restano
`default-features = false`, `features = ["std"]`. NATS abilita inoltre transitivamente `raw_value`, conservata e inclusa nelle sonde.
Restano le feature TLS ring
scelte dall'adapter NATS e il server HTTP/1; HTTP/2 non ÃƒÂ¨ abilitato.

Aggiornamenti eseguiti con `cargo update --offline --manifest-path <manifest>
-p <pacchetto> --precise <versione>`, per ogni coppia applicabile della tabella,
su `Cargo.toml` e `fuzz/Cargo.toml`. Nessun aggiornamento globale del grafo.

## Prove differenziali

La [sonda Rust](../tests/dependency-alignment/probe.rs) ÃƒÂ¨ eseguita dal
[runner Python](../tests/dependency-alignment/run.py):

```sh
python tests/dependency-alignment/run.py
```

Il runner genera due workspace isolati sotto `target/deps-alignment/`, parte
in entrambi dal lock candidato e cambia solo le tre dipendenze sotto prova.
Verifica che le altre versioni risolte siano identiche. Usa dipendenze giÃƒÂ 
presenti, pin esatti, rete disabilitata e tre job di build. Richiede nella cache
Cargo entrambe le versioni e le fixture TLS del pacchetto tokio-rustls 0.26.4;
non installa nÃƒÂ© scarica nulla. Le fixture restano nella cache, senza copiarne
le chiavi nel repository. I log sono `probe-before.log` e `probe-after.log`
sotto `target/deps-alignment/`.

HTTP usa un trasporto in memoria, input forniti per fasi, poll espliciti e
header Date disabilitato. TLS usa duplex in memoria, certificati verificati e
un provider di tempo fisso iniettato. Non ci sono attese basate sul tempo di
sistema o infrastrutture esterne.

| Prova | Prima | Dopo |
| --- | --- | --- |
| Richiesta HTTP canonica | Risposta 200 e body `ok` | Identica byte per byte |
| Header terminato con LF/CRLF, ultimo frammento separato | Nessuna risposta dopo i poll previsti | Risposta 200 completa |
| `TE: gzip, Trailers`, risposta con `Trailer: x-proof` | Trailer assente | `x-proof: yes` sul wire |
| Due righe `TE: gzip` e `TE: trailers` | Trailer assente | `x-proof: yes` sul wire |
| Body chunked: un frame, Pending, poi EOF nello stesso poll di connessione | Terminatore non ancora inviato | Terminatore `0\r\n\r\n` inviato |
| Due record TLS da quattro byte giÃƒÂ  disponibili | Prima lettura di 4 byte | Prima lettura di 8 byte |
| Stream TLS completo, client e server | 8 byte `aaaabbbb` | Identici |
| Stringa JSON con byte UTF-8 invalido e tre spazi | `Syntax`, riga 1 colonna 6 | `Syntax`, riga 1 colonna 2 |
| RawValue da slice e reader, stesso UTF-8 invalido | `Syntax`, riga 1 colonna 6 | `Syntax`, riga 1 colonna 2 |
| JSON `-0`, `-0.0`, `-0e2` | Serializzati come `-0.0` | Identici |
| JSON 1.5, massimo u64, minimo i64 | Valori conservati | Identici |
| Conversione da Value numerico 1e100 a f32 | `Ok(inf)` | Identica |

Gli ultimi casi invariati non sono dichiarati come correzioni nel changelog.
La sonda numerica descrive serde_json, non una conversione ammessa dalle API
runtime. Non prova equivalenza per tutti i floating point.
Il decoder Discovery mappa gli errori JSON alla propria categoria senza
esporre posizione o testo upstream; i relativi test della suite passano.
Il cambiamento di posizione ÃƒÂ¨ documentato come diagnostica della dipendenza,
non come modifica dell'errore pubblico Discovery.

La revisione dei sorgenti nella cache ha inoltre verificato che la modifica
thiserror riguarda il parsing di espressioni turbofish nella macro, mentre le
modifiche Hyper client/HTTP2 e serde_json `arbitrary_precision`,
`preserve_order` non sono percorsi abilitati da questo workspace. Le sonde
qualificano i casi elencati, non una prova esaustiva di equivalenza dei trasporti.
Non ÃƒÂ¨ stato modificato codice di produzione o logica critica del repository.

## Gate locali

Tutti i comandi Cargo di compilazione sono stati eseguiti con `--offline`
oltre alle opzioni CI, e con `CARGO_BUILD_JOBS=3`. Comandi completi ed exit code
sono conservati in `target/deps-alignment/gates.json`, con un log per gate.

| Comando | Esito Windows |
| --- | --- |
| `cargo fmt --all --check` | Passato; solo workspace, nessun vendor |
| `cargo check --workspace --all-targets --locked --offline` | Passato |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | Passato |
| `cargo clippy --workspace --lib --all-features --locked --offline -- -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::unreachable` | Passato |
| `cargo test --workspace --all-targets --all-features --locked --offline` | 365 passati, 0 falliti, 8 ignorati |
| `cargo test --workspace --all-features --doc --offline --locked` | Passato; nessun doctest definito |
| `cargo doc --workspace --no-deps --all-features --locked --offline` con `RUSTDOCFLAGS=-D warnings` | Passato |
| `python -m unittest discover -s scripts -p 'test_*.py'` | 23 passati |
| `python scripts/check-contract-adoption.py` | Passato |
| `python scripts/check_comments.py` | Passato |
| `python scripts/check_docs.py` | Passato |
| `python tests/dependency-alignment/run.py` | Passato, confronto prima/dopo e asserzioni |
| `cargo deny --offline check bans licenses sources` | Passato |
| `cargo deny --offline --manifest-path fuzz/Cargo.toml check bans licenses sources` | Passato |
| `cargo deny --offline check` e variante `--manifest-path fuzz/Cargo.toml` | Bloccati dal lock del database advisory in percorso non scrivibile |
| `cargo audit --no-fetch` e variante `--file fuzz/Cargo.lock` | Bloccati: database advisory locale assente |

I due gate completi sono stati poi ripetuti con successo usando una copia
scrivibile del database gi? disponibile in cache, sotto
`target/deps-alignment/advisory-dbs/`. Nessun fetch: commit RustSec
`263a4d951a0867d4bddc29dddd6d59427c78f9a7`, data 2026-11-02.
Per deny, `target/deps-alignment/deny-offline.toml` ? identico alla policy
versionata salvo `advisories.db-path` verso quella copia. Comandi riusciti:

```text
cargo deny --offline --config target/deps-alignment/deny-offline.toml check
cargo deny --offline --manifest-path fuzz/Cargo.toml --config target/deps-alignment/deny-offline.toml check
cargo audit --no-fetch --db target/deps-alignment/advisory-dbs/advisory-db-3157b0e258782691
cargo audit --no-fetch --db target/deps-alignment/advisory-dbs/advisory-db-3157b0e258782691 --file fuzz/Cargo.lock
```

Tutti e quattro hanno exit code 0. Audit avverte che l'indice crates.io non ?
accessibile per il lock globale `.package-cache`: il suo controllo dei crate
ritirati non ? qualificato da questa esecuzione. Deny passa anche il controllo
yanked usando la cache offline. Il database non ? aggiornato via rete alla
data della prova; non si attesta l'assenza di advisory pubblicati dopo il suo
commit. I tentativi iniziali bloccati rimangono registrati separatamente.

Strumenti installati: cargo-deny 0.20.2, cargo-audit 0.22.2, come CI. I warning
sulle versioni duplicate di cargo-deny non sono stati soppressi. Un primo
comando deny con `--disable-fetch` non era valido per questa versione; ÃƒÂ¨ stato
corretto usando `--offline`, senza dichiarare passato il tentativo invalido.

Non eseguiti: CI Linux/macOS e CI Windows remota, NATS reale e gli otto test
ignorati dalla suite, coverage con soglia 90%, fuzz smoke/campagne su Linux,
soak Docker, audit aggiornato via rete e consumatore aggregato con gli altri
prodotti Plenora. Non sono gate verdi. La sonda TLS in memoria non qualifica
un deploy NATS con TLS e permessi reali. Il lavoro resta una modifica locale;
non ÃƒÂ¨ una qualifica di rilascio o merge.

## Contenuto dei pacchetti e licenze

Tutti i crate del workspace sono `publish = false`; non ÃƒÂ¨ stato prodotto o
pubblicato un pacchetto. Nessun testo di licenza versionato ÃƒÂ¨ stato riscritto.

L'ispezione degli archivi `.crate` presenti in cache per entrambi i lock non
ha trovato archivi mancanti. Nei pacchetti aggiornati serde_json e thiserror
sono distribuiti test e fixture di compilazione; tokio-rustls 0.26.5 include
`src/common/test_stream.rs`, Hyper include `src/client/tests.rs`.
Nessun PEM di chiave privata ÃƒÂ¨ stato rilevato nei cinque pacchetti aggiornati.

Nel grafo giÃƒÂ  esistente sono invece distribuite chiavi di prova: async-nats
0.50.0 contiene, per esempio, `tests/configs/certs/client-key.pem`,
`rootCA-key.pem` e `server-key.pem`; ed25519 2.2.3, pem-rfc7468 0.7.0 e pkcs8
0.10.2 contengono PEM sotto `tests/examples/`. Non sono nuove dipendenze nÃƒÂ©
chiavi di deploy introdotte dall'allineamento. Altri match del marcatore PEM
sono esempi o codice di parsing, non prova da soli della presenza di una chiave.
L'inventario dei soli percorsi, senza contenuto delle chiavi, ÃƒÂ¨ conservato in
`target/deps-alignment/package-test-keys.json`; quello dei test dei pacchetti
aggiornati in `target/deps-alignment/package-fixtures.json`. La ricerca dei
marcatori PEM non costituisce una scansione esaustiva di segreti binari.

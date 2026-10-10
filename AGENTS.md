# AGENTS.md

Il documento che si legge per primo. Non descrive il repository — quello lo fa
il repository — ma dice **che cosa non è negoziabile** e **dove sta il resto**.

## Le regole che non cambiano

Questo repository si tratta come una libreria safety-critical, come le altre
librerie Plenora. Sono policy, non fatti del codice, e questo è l'unico posto in
cui sono scritte.

1. **Niente failure silenziose.** Un risultato sbagliato è sempre peggio di un
   errore. L'ammissione di un messaggio (metadati, identificatori, nomi e
   versioni di capability e operazione, contratti, deadline, chiavi di
   idempotenza) e ogni conversione sono esatte per costruzione: un valore non
   canonico si rifiuta con una categoria esplicita, non si normalizza in un
   valore plausibile. Qui non esiste alcuna tolleranza accettata. Lo stesso vale
   per il contenimento dei processi e per la settlement: una terminazione o una
   pubblicazione non confermata si riporta come tale, non si presume riuscita.
2. **I test verdi non bastano.** Una modifica a logica critica — codec e
   ammissione, retry e settlement, outbox/inbox, scheduler, terminazione dei
   processi, concorrenza — si rilegge da un secondo lettore (Codex), non solo
   nella sintesi di chi l'ha scritta.
3. **Ogni bug è una classe.** Trovato un difetto, si cerca la stessa classe in
   tutto il workspace, adapter ed esempi compresi, prima di chiudere la
   correzione.
4. **Deviazioni esplicite.** Uno scostamento da un contratto comune si scrive
   nel codice **e** in [`docs/contract-alignment.md`](docs/contract-alignment.md);
   un rischio residuo in [`docs/security-review.md`](docs/security-review.md);
   un gate aperto in [`docs/qualification.md`](docs/qualification.md). Ogni voce
   dice regola, ambito, rischio e condizione di chiusura. Una garanzia
   indebolita si dichiara come tale.
5. **Determinismo testato.** Stesso input, stesso output. Nelle prove
   deterministiche il tempo entra da un orologio iniettabile (`ManualClock` nel
   testkit), non dal clock di sistema; un'ottimizzazione si verifica con un
   oracolo contro il percorso generico.
6. **Nessun `unsafe`** nel workspace (`unsafe_code = "forbid"`). Nessuna
   dipendenza nuova senza una motivazione scritta accanto al pin in
   `Cargo.toml`, con versione esatta (`=x.y.z`). Una dipendenza git o un nuovo
   client di infrastruttura richiede un ADR in [`docs/adr/`](docs/adr/README.md).
7. **Suite completa prima del commit**: fmt, clippy con `-D warnings`, clippy
   anti-panic sulle librerie (`--lib`), test completi. Prima del merge in
   `main` la CI è verde su Linux **e** Windows; macOS gira ma non è
   obbligatorio.
8. **Errori senza dati.** Messaggi d'errore, `Display` e `Debug` non
   trasportano payload, valori di metadati, credenziali o segreti: contesto
   operativo sì, dato no ([`docs/error-model.md`](docs/error-model.md)).

## Dove sta il resto

| serve | sta in |
| --- | --- |
| come si costruisce e che cosa si esegue | [`README.md`](README.md) |
| crate, adapter e direzione delle dipendenze | [`docs/architecture.md`](docs/architecture.md) e [`docs/boundaries.md`](docs/boundaries.md) |
| gate di qualifica e gate di freeze dell'API | [`docs/qualification.md`](docs/qualification.md) |
| rischi residui e checklist di rilascio | [`docs/security-review.md`](docs/security-review.md) |
| pin dei contratti comuni e lacune note | [`docs/contract-alignment.md`](docs/contract-alignment.md) e [`contracts/source.json`](contracts/source.json) |
| politica delle dipendenze e della supply chain | [`docs/dependency-policy.md`](docs/dependency-policy.md) e [`deny.toml`](deny.toml) |
| coverage, fuzz e prove di memoria | [`docs/testing.md`](docs/testing.md) |
| decisioni architetturali | [`docs/adr/`](docs/adr/README.md) |
| segnalare una vulnerabilità | [`SECURITY.md`](SECURITY.md) |
| perché una decisione è stata presa | `git log` |

## Prima di dire «fatto»

I comandi stanno nel [`README.md`](README.md) e nei workflow sotto
`.github/workflows/`: sono la fonte, e ricopiarli qui li farebbe divergere al
primo cambiamento. Ciò che questo documento aggiunge è l'ordine di lettura del
risultato:

- i gate della CI devono essere verdi su Linux **e** Windows;
- `cargo audit` e `cargo deny` possono diventare rossi senza un commit, quando
  esce un'advisory: un rosso lì non dice che il commit è cambiato;
- le prove su NATS reale valgono per il container effimero della CI, non per un
  deploy con TLS e permessi dei subject reali;
- se un gate non è stato eseguito, si dice che non è stato eseguito. Un gate
  saltato non è un gate passato.

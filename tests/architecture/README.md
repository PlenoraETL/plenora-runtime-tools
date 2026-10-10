# Test d'architettura

Questo crate di soli test fa rispettare la direzione delle dipendenze e le regole di sicurezza del
codice sorgente di produzione.

Il crate è membro del workspace radice e si esegue con:

    cargo test -p plenora-runtime-architecture-tests

I test usano solo la libreria standard di Rust. Ispezionano i manifest e il sorgente di produzione
sotto crates e adapters. I sorgenti di test e gli esempi sono esclusi di proposito dalle scansioni
di produzione.

I confini applicati coprono la fuoriuscita di implementazioni di broker e worker, la fuoriuscita di
driver di database, le dipendenze HTTP e di telemetria nei crate di fondazione, l'indipendenza dal
backend nell'osservabilità e l'esclusione dall'adapter HTTP di autenticazione, business e ambito
PFM. Le scansioni sui percorsi di panic e sulla fuoriuscita di segreti mascherano documentazione,
commenti e letterali Rust dove non rappresentano codice eseguibile, mentre i valori che portano
segreti dentro le macro di formattazione e di logging restano controllati.
Anche gli elementi inline protetti da `cfg(test)` sono esclusi dalle scansioni di sola produzione.

I gate a livello di workspace richiedono inoltre versioni delle dipendenze esatte o ereditate dal
workspace, direzione delle dipendenze approvata, lint di sicurezza condivisi,
`forbid(unsafe_code)` alle radici dei crate di produzione e nessun costrutto unsafe o di abort nel
sorgente di produzione. Invarianti statiche mirate coprono code limitate, validazione di payload e
concorrenza, configurazione NATS con TLS predefinito, opt-in esplicito al replay, viste di health e
`Debug` redatte, hook di cancellazione e hook deterministici di iniezione dei guasti. La ritenzione
di task e report del runtime, le storie e gli script di guasti del broker fake, e le dimensioni di
voci, chiavi, valori e aggregato dei metadati dei messaggi devono restare limitate in modo
esplicito. Ogni pacchetto eredita la MSRV del workspace, ogni job di CI che esegue Cargo la
seleziona e ogni job di qualificazione dipende dal controllo esplicito dello SHA atteso del
workflow.

# Store durevole catene

PostgreSQL implementa AcceptanceStore, Store e DurableExecutionStore con una
sola autorita per ricevuta, invocazione, evidenze e settlement. Nessun profilo
memoria di produzione. [ADR 0009](../../docs/adr/0009-chain-postgres.md).

`migrate` inizializza solo lo schema corrente 3 da database vuoto, oppure
riapre lo stesso schema riducendo la retention. Versioni precedenti, schema
parziale senza identita e versioni sconosciute sono rifiutati; non esiste
conversione di dati. Il solo sorgente SQL e `src/bootstrap.sql`.

Ogni operazione verifica primary/read-only/fsync e usa il lock transazionale
comune, READ COMMITTED e synchronous_commit. COMMIT non confermato resta
Unconfirmed. Le ricevute conservano identita, route, CAS e stato di settlement;
nessuna lease autorizza la ripetizione di un effetto ignoto.

Le prove offline verificano codifica SQL, un solo bootstrap/versione, policy,
redazione e modello con ManualClock. Le prove PostgreSQL ignorate per default
verificano bootstrap vuoto, riapertura, rifiuto di versioni inattese, retention
limitata, accettazione concorrente, CAS, restart, fencing e commit incerto.
La sintassi SQL e la persistenza effettiva richiedono PostgreSQL reale.

```sh
cargo test -p plenora-runtime-chain-store --locked postgres_bootstrap -- --ignored --nocapture --test-threads=1
cargo test -p plenora-runtime-chain-store --locked postgres_real -- --ignored --nocapture --test-threads=1
```

Usare soltanto il database effimero dedicato della CI. Le prove di bootstrap
richiedono CREATEDB e creano/rimuovono database isolati da template0; le prove
store riducono permanentemente la retention del database dedicato.
Le campagne PostgreSQL e coverage restano nei workflow correnti: nessun job
di aggiornamento di schemi precedenti e richiesto.

Il bootstrap conserva il catalogo finale precedente (13 tabelle), incluso il
CHECK SQL storico; solo il valore registrato 3 e ammesso. Le API rimosse non
sono ripristinate. `postgres_bootstrap_matches_baseline_catalog` confronta
colonne ordinate, default, vincoli/indici con definizioni, funzioni, trigger,
RLS, owner e ACL con un oracolo test congelato al commit a973b6e. La guardia di
riapertura controlla il layout, non attesta modifiche amministrative delle
espressioni dei vincoli: vedere PR23 in docs/contract-alignment.md.

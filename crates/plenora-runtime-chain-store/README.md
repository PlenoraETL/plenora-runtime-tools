# Store durevole catene P2.2

Primitive PostgreSQL per acceptance, start unico con fence, inbox/report/outbox
atomici, replay immutabile, alias e retention. Nessun engine ETL o profilo
memoria di produzione. Decisione e limiti: [ADR 0009](../../docs/adr/0009-chain-postgres.md).

`PostgresStore::new` configura soltanto il client; `migrate` installa lo schema
v2 e la retention. Ogni operazione ricontrolla la fonte autorevole. `start`
richiede acceptance già confermata; restituisce Started una sola volta,
RequiresRecovery per intenzioni pendenti e Replay per terminali. `finish`
registra report, envelope e inbox nella stessa transazione. Il report può
essere letto anche dopo la finestra di delivery, fino alla propria scadenza.
`claim` restituisce una lease di 30 secondi; `confirm` deve seguire la conferma
reale del broker. Nessun ACK deve precedere la conferma dello store.

StartPermit non è clonabile o serializzabile; Publication non ha costruttore
pubblico. Non si può ricostruire una nuova autorità di avvio dopo un crash.
Un esito tardivo può ancora essere conservato dal proprietario del permit,
ma oltre la finestra la pubblicazione resta scaduta. L'inbox generico non
può essere scritto separatamente dalla finalizzazione.

## Prove

Il test normale esegue il doppio in memoria con ManualClock, la validazione
esatta e l'enumerazione RT-022/027. I due test PostgreSQL sono ignorati per
default. `postgres_real` avvia anche sottoprocessi che terminano prima del
commit, dopo lo start e dopo finish; il processo padre riapre lo store.
Include race con/senza chiave, chiavi tra run diversi, rollback di finish,
lettura autorevole, lease stale, risultato immutabile, collisioni e retention.

Usare un database PostgreSQL **effimero dedicato**, con un ruolo amministrativo
per le fault injection. Il test riduce permanentemente la policy del database:
una nuova esecuzione richiede un database nuovo. Non usare dati di produzione.

```powershell
$env:CARGO_BUILD_JOBS = '2'
$env:PLENORA_POSTGRES_URL = 'host=127.0.0.1 port=PORT user=postgres password=PASSWORD dbname=chain_store'
cargo test -p plenora-runtime-chain-store --locked postgres_real -- --ignored --nocapture --test-threads=1
```

Il job PostgreSQL di [.github/workflows/ci.yml](../../.github/workflows/ci.yml)
usa PostgreSQL 17.6 bookworm fissato per digest OCI, porta loopback effimera e
cleanup always del container e volume. Il digest è stato risolto dal registry
Docker ufficiale; non è stato eseguito PostgreSQL sull'host locale.

Il journal P2.3/P2.4 implementa DurableExecutionStore: ricevuta precedente
all'ammissione, risultato e route immutabili, CAS, lease e pending recuperabile.
La migrazione v2 impedisce ai binari v1 di operare e controlla le collisioni con
le primitive P2.2 in entrambi gli ordini. La retention elimina i rapporti inattivi
conservando i tombstone. delivery_tests verifica queste proprieta sul PostgreSQL
reale del job CI; i test di integrazione combinano anche NATS e il runner Apalis.

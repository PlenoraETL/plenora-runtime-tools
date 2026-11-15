# ADR 0009: store operativo PostgreSQL delle catene

Stato: aggiornato il 15 novembre 2026; qualifica su backend reale richiesta in CI.

## Decisione

Il titolare ha scelto PostgreSQL, indipendente dall'engine ETL. Il crate
`plenora-runtime-chain-store` usa `tokio-postgres =0.7.18`, client asincrono
con pin esatto e senza dipendenze ETL. Non esiste profilo memoria di produzione.
Il doppio in memoria vive esclusivamente sotto `cfg(test)`.

Ogni operazione apre una connessione e verifica primary, transaction_read_only
 e fsync; impone READ COMMITTED e synchronous_commit=on. Un advisory lock
transazionale comune ordina bootstrap corrente, riduzione policy, ammissione, CAS,
lettura e manutenzione. Non sono ammessi snapshot o replica fallback.
L'acknowledgement di COMMIT è obbligatorio; una risposta persa è Unconfirmed,
mai Created/Started presunto. Il clock è quello del database.

AcceptanceStore è implementato anche nella sua forma sincrona, tramite thread
con runtime dedicato e fallimento esplicito della creazione. I consumer async
usano accept_async; il bridge sincrono deve stare nell'executor blocking.
Il prodotto implementa DurableExecutionStore: DeliveryJournal e DeliveryChange
conservano l'unica autorita di invocation/evidence/settlement. Le vecchie
primitive start/finish/claim/confirm e InboxStore separato sono rimosse.
Le transizioni CAS persistono ricevuta, evidenze e conferma di pubblicazione;
una lettura o una lease scaduta non concede autorita per ripetere il dominio.
Il bootstrap inizializza direttamente il catalogo finale dello schema 3.
Nessun upgrade incrementale e offerto; versioni registrate diverse da 3 vengono
rifiutate. Le 13 tabelle originali restano nel DDL per identita del catalogo,
senza ripristinare le API rimosse.

## Conseguenze

Una singola serializzazione limita il throughput, ma rende verificabile
l'at-most-one-start anche tra processi. Un'intenzione senza esito richiede
recovery: nessuna lease permette di rieseguire il dominio. Dopo restart
sono recuperabili esito e ricevute di pubblicazione; soltanto la pubblicazione può essere
riacquisita. La dedup della chiave usa scope tenant/operazione distinto dallo
scope run/step. Il boundary deve fornire input canonico completo di selettori,
versioni, destinazione, consenso e budget; le identità di trasporto non fanno
parte dell'input semantico. Non viene promesso exactly-once al broker.

Finestra massima 604800 secondi, rapporti 7776000 secondi dalla creazione della ricevuta,
tombstone permanenti di ID, chiavi e fence. Le riduzioni sono persistite e
non estendibili successivamente. Scaduta la finestra, il relay non invia;
scaduti i rapporti, vengono rimossi report e envelope anche se non consegnati,
con report_expired e stato di delivery ancora interrogabili.
Le operazioni applicano expiry anche senza manutenzione periodica. Il clock
operativo PostgreSQL deve essere amministrato senza arretramenti arbitrari.

Il trasporto iniziale è NoTls su rete fidata/loopback. Non è una qualifica
TLS, failover, backup o disaster recovery: rischi e condizioni di chiusura
sono in [security-review](../security-review.md). La perdita di connessione
non viene nascosta dagli errori del driver: Display/Debug/source non li espongono.

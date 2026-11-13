# Runtime boundary

`RuntimeBoundary` espone una sola ammissione sopra il registro congelato del
dispatcher generico. `admit_raw` riceve i byte JSON della **sola mappa metadata**
e un `SerializedMessage` con payload opaco e header vuoti; non è un nuovo envelope
wire. `admit_message` e `WorkerMessageDecoder<AdmittedRequest>` sono facciate della
stessa pipeline per i trasporti. I trasporti devono conservare o rifiutare
duplicati prima della conversione nella mappa; il decoder NATS ora li rifiuta.

L'ordine è protocol → unsupported → timeout → limite payload → validazione
del contratto di dominio. Tutti i rifiuti prima dell'invocazione hanno
validate/none/never. La grammatica delle versioni precede `u16`: un intero
canonico non rappresentabile è unsupported. I limiti portabili dei metadati
restano quelli di messaging (64 chiavi, 8192 byte/valore, 32768 byte totali);
il JSON grezzo è limitato a sei volte il totale per le escape JSON.

Il profilo UTC riceve T/t, Z/z e +00:00, frazioni di 1–9 cifre; rifiuta
-00:00, offset non zero, separatori alternativi e secondi intercalari,
non rappresentabili esattamente da `SystemTime`. La scadenza uguale a now è
già scaduta. Il clock è obbligatorio e condiviso con il dispatcher.

Il costruttore richiede una `AdmissionPolicy` e un `BoundaryResultSink`:
la policy è un validatore puro, senza resolver o I/O; dichiara il limite della
chiave per selettore e valida il payload secondo il contratto del componente.
Il boundary non cerca arbitrariamente campi JSON nel payload. La policy deve
rifiutare i due canali deadline con invalid_configuration. L'handler possiede
il resolver e viene invocato soltanto dopo questa barriera.

`AdmittedRequest` non ha costruttore pubblico. Il wrapper worker riapplica
l'ammissione al momento della consegna, verifica che il contesto abbia le stesse
identità e usa il vero `CapabilityDispatcher`. Il sink installato dal boundary
sostituisce quello eventualmente presente nel dispatcher fornito e aggiunge
nuova identità e causation al risultato prima di consegnarlo al sink richiesto.
Il sink deve riportare Confirmed o OutcomeUnknown senza inferire consegna.

`Rejection` è l'unico errore pubblico del boundary, anche per gli errori di
dispatch. Espone i quattro assi; Debug/Display/source non riportano dati.
`to_message` usa il media type comune e riflette solo campi ben formati.
La serializzazione ripetuta della stessa istanza conserva l'identità.

## Ambito delle prove e gate aperti

[I probe](../../tests/integration/tests/boundary_facades.rs) eseguono i 21 vettori upstream e la tabella
RT-012/016…023 dell'[inventario](../../docs/boundary-probes.json), con metadati
grezzi, dispatcher reale e sink fixture. Non sono una prova di persistenza,
replay dopo restart, CAS o pubblicazione terminale del runner.

Il profilo prodotto P2.3/P2.4 usa DurableRuntime con PostgreSQL, DurableWorker
nel runner Apalis e NegotiatedForwarder per send/retry/DLQ. Il journal precede
l'ammissione, conserva rifiuti e risultati, recupera soltanto la finalizzazione
dopo evidence e non ripete il dominio Running senza evidenza. RouteTable lega
il relay alla destinazione persistita. OutcomeBudget e DeliveryPlan prenotano
il journal JSON entro 64 MiB prima dell'invocazione. I test chain_delivery
coprono questi percorsi; le prove con servizi reali restano ignorate per CI.
Lo scope host delle chiavi di idempotenza resta P1.4/P1.5: attualmente rifiutate.
Gli esempi legacy restano tali e non vengono attestati conformi dal wrapper.
Regole, rischi e condizioni di chiusura sono in
[contract-alignment](../../docs/contract-alignment.md),
[qualification](../../docs/qualification.md) e
[security-review](../../docs/security-review.md).

P2.1b sposta qui CapabilityRegistry e CapabilityDispatcher: capabilities conserva
solo DTO outbound, discovery e contratti dell'adapter. Il metodo di dispatch
grezzo è privato; RuntimeBoundary è il solo WorkerHandler registrato. Gli esempi
non-runtime sono dichiarati nei manifest. Le prove di facciata stanno nel crate
di integrazione, per non aggiungere dipendenze infrastrutturali al boundary.

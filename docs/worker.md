# Esecuzione dei worker

Le API dei worker espongono tipi di handler e di contesto di proprietà di Plenora. Concorrenza e
canali sono limitati, la policy di retry è iniettata, e lo shutdown ferma l'accettazione prima di
fare il drain del lavoro in corso.

WorkerExecutor non possiede code e non avvia task. Un semaforo limita le future degli handler
guidate dal chiamante. I job in attesa di capacità osservano sia lo shutdown del runtime sia il
drain. Il lavoro ammesso termina in modo cooperativo entro il periodo di grazia configurato. Un
timeout di drain del worker riporta il numero di task rimasti; poiché l'executor neutrale rispetto
all'engine non possiede handle dei task, la terminazione forzata e il requeue delle consegne restano
responsabilità dell'adapter concreto dell'engine. I task avviati tramite `RuntimeHandle` vengono
interrotti dal supervisore del core allo scadere del suo periodo di grazia.

L'ammissione ha tre stati: `Accepting`, `Paused` reversibile e `Draining` terminale. La pressione
sulle risorse usa `Paused`: i nuovi job sono rifiutati prima dell'invocazione dell'handler con una
disposizione dell'adapter ritentabile, mentre gli handler attivi proseguono. Il recupero può
riaprire solo un cancello in pausa; il drain non si può mai invertire.

I fallimenti degli handler conservano la loro sorgente ed espongono categoria, fase, certezza
dell'effetto remoto e decisione di retry stabili. L'adapter dell'engine resta responsabile di
tradurre queste decisioni in ACK, NACK, retry o dead-letter.

`plenora-runtime-apalis` è il ponte concreto verso l'engine. Avvolge un `WorkerExecutor` in un
servizio Tower clonabile, configura la concorrenza di Apalis e la cattura dei panic, e collega il
segnale del monitor di Apalis all'arresto dell'ammissione e al drain limitato di Plenora. Le
decisioni di retry restano esiti espliciti dell'adapter: il ponte non clona mai una consegna
posseduta.

`BrokerWorkerRunner` compone quel servizio con qualsiasi `MessageConsumer` neutrale rispetto al
broker. Il livello di concorrenza di Apalis diventa pronto prima che lo stream del consumer venga
interrogato. Con `max_in_flight = 5`, quando la coda è inattiva non esiste alcun task di handler, al
più cinque future di consegna girano in concorrenza, e i messaggi in più restano nel broker finché
non si libera uno slot. Un handler riuscito diventa ACK, `RetryAfter` un NAK ritardato nativo del
broker, `DoNotRetry` un NAK/TERM permanente, e l'ammissione in shutdown un NAK di shutdown.
`DeadLetter` richiede un `DeadLetterSink` collegato esplicitamente: il record viene pubblicato per
primo e l'originale riceve TERM solo dopo la conferma del broker. La decodifica dei metadati
canonici richiede UUID stabili di messaggio e di
correlazione prima che un handler possa girare. Un fallimento terminale nell'interrogazione del
consumer ferma il monitor e viene restituito con la sua sorgente concreta, invece di essere trattato
come uno shutdown pulito della coda.

Le consegne possono portare una policy di heartbeat validata. Finché la future dell'handler è
attiva, il runner del broker rinnova il possesso senza consumare la consegna. NATS traduce il
rinnovo in JetStream `+WPI`. La finestra di fallimento dell'heartbeat deve essere più breve
dell'`ack_wait` del consumer durable. I rinnovi riusciti azzerano il contatore dei fallimenti
consecutivi. Esaurito il budget di fallimenti, la future dell'handler viene scartata, si tenta un
NAK ritentabile, e i fallimenti di heartbeat e di settlement sono riportati separatamente. Scartare
la future asincrona di un handler non ferma il lavoro staccato né un'operazione bloccante già in
corso; quelle operazioni devono usare un proprio ponte di cancellazione cooperativa.

L'heartbeat dell'istanza di worker è un terzo segnale, indipendente. `WorkerInstanceHeartbeatReporter`
campiona la capacità limitata viva dell'executor ed emette identità stabile di processo e worker,
sequenza monotona, `Starting/Ready/Draining/Stopped`, `max_in_flight`, `in_flight`, slot disponibili
e ora dell'osservatore. Il runner di Apalis possiede il timer periodico validato, emette snapshot
di transizione, e scarta il timer insieme al monitor; dopo lo shutdown non resta alcun task di
heartbeat staccato. L'osservatore è sincrono e non deve bloccare. I futuri adapter di persistenza
devono usare un passaggio di consegne limitato in modo esplicito, invece di fare I/O su database o
rete dentro `record`.

## Controllo dei task attivi

Ogni `WorkerExecutor`, `ApalisWorkerService`, `BrokerDeliveryService` e `BrokerWorkerRunner`
espone un `WorkerTaskControl` clonabile. L'handle non possiede tipi di handler, engine o broker e
può quindi essere trattenuto da `plenora-runtime-control` o da una CLI di Plenora prima che il
runner venga spostato nella sua future di esecuzione asincrona. `WorkerAdmissionHandle` espone in
modo indipendente la capacità, senza payload, e le operazioni di pause/resume/drain.

```rust,ignore
let task_control = runner.task_control();
let active = task_control.active_tasks();

if let Some(task) = active.first() {
    let outcome = task_control.request_cancellation(task.task_id);
    println!("cancellation={outcome:?}");
}

let report = task_control.request_message_cancellation(message_id);
println!("matched={} requested={}", report.matched, report.requested);
```

Il registro contiene solo gli handler ammessi; i messaggi ancora in attesa in JetStream restano
stato del broker e non occupano voci del registro. La sua capacità è esattamente `max_in_flight`,
gli snapshot sono senza payload e limitati, e le voci sono rimosse da una guardia di esecuzione in
caso di successo, fallimento, cancellazione, timeout o future scartate. `WorkerTaskId` è locale alla
vita di un singolo executor, quindi un'API di controllo remota deve accoppiarlo con l'`instance_id`
del worker; è disponibile anche la cancellazione per `MessageId` canonico, che raggiunge ogni
tentativo di consegna attivo contemporaneamente per quel messaggio.

La cancellazione è cooperativa e conserva il primo motivo. Gli handler di lunga durata devono
attendere `WorkerContext::cancelled()` o collegare `context.cancellation` all'operazione che
invocano. L'executor interroga l'handler per il periodo di grazia di pulizia configurato e limitato,
poi restituisce un esito di cancellazione tipizzato. Nell'attuale runner del broker di Apalis, una
cancellazione `Requested` esplicita diventa `DoNotRetry`, poi NAK/TERM permanente. Lo shutdown del
processo e la perdita del lease mantengono i propri percorsi di settlement ritentabili, distinti.

## Passaggio di consegne limitato del ciclo di vita

`WorkerLifecycleDispatcher::channel` collega sia `TaskLifecycleObserver` sia
`WorkerInstanceHeartbeatObserver` a un unico ricevitore asincrono limitato in modo esplicito. Le
chiamate dell'osservatore usano `try_send` e non attendono mai: quando il canale è pieno,
l'osservazione più recente viene scartata e `dropped_full` avanza; dopo la chiusura del ricevitore,
avanza `dropped_closed`. Lo snapshot espone anche l'occupazione corrente, i totali accettati e
consegnati, e lo stato `Open/Saturated/Closed`. La capacità predefinita è 1.024, con un massimo
rigido di 65.536 osservazioni.

Il dispatcher non avvia task e non fa persistenza. Un'applicazione ospite possiede l'unico
ricevitore e può guidarlo come task di runtime supervisionato. È questo il futuro punto di
collegamento per un adapter di database o di telemetria; né il crate del worker né il suo adapter
del broker importano quelle librerie non ancora complete. La saturazione influisce solo sulla
freschezza delle osservazioni e non cambia mai l'esecuzione degli handler né il settlement del
broker.

`WorkerLifecycleHealthReporter` traduce gli snapshot nel `HealthRegistry` condiviso. Un passaggio
di consegne opzionale resta pronto anche se degradato; uno obbligatorio diventa `NotReady` quando è
saturo e non sano quando il ricevitore si chiude. Le applicazioni chiamano `refresh` dal loro
monitor supervisionato, tenendo il lock del registro fuori dalle callback dell'osservatore.
L'osservabilità espone `worker_lifecycle_queue_depth` e `worker_lifecycle_dropped_total`; i
chiamanti registrano la profondità della coda e gli incrementi del contatore dallo stesso snapshot,
senza introdurre una dipendenza dal worker nel crate di osservabilità.

I tipi di Apalis restano confinati nel crate dell'adapter; `runtime-worker` espone solo tipi di
configurazione, contesto, handler, esecuzione e drain di proprietà di Plenora.

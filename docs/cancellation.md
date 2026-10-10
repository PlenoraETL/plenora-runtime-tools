# Cancellazione

La cancellazione del runtime è cooperativa, limitata, osservabile e idempotente. Il rilascio
iniziale non forza le primitive di cancellazione delle altre fondazioni Plenora in un unico tipo
condiviso.

Il drain del worker chiude per prima cosa l'ammissione. Il lavoro in attesa di capacità viene
rifiutato, mentre gli handler già ammessi ricevono il segnale di arresto condiviso e possono
terminare durante il periodo di grazia limitato. Un timeout del drain è osservabile. `RuntimeHandle`
interrompe poi i task Tokio ancora supervisionati e riporta quanti hanno superato il periodo di
grazia; i loro handle di completamento si risolvono con un report di cancellazione sicuro rispetto
all'oscuramento. L'executor del worker in sé non possiede task, quindi un adapter del motore
possiede ancora la terminazione forzata degli handler e la policy di rimessa in coda delle consegne.

Il runner Apalis basato su broker smette di interrogare il broker prima che parta il suo
terminatore. Le consegne che osservano l'arresto attraverso la normale ammissione del servizio
ricevono un NAK di arresto. Se un handler non cooperativo viene abbandonato dopo la scadenza di
grazia, la sua consegna senza acknowledgement resta idonea alla riconsegna da parte del server dopo
l'attesa di acknowledgement configurata.

I ponti verso la cancellazione di database, dati e I/O saranno implementati dietro adapter quando i
loro contratti saranno stabili.

Ogni task del worker ammesso porta ora anche un `TaskCancellationToken` in cui vince il primo che
scrive. Un handler può attendere `WorkerContext::cancelled` per distinguere l'arresto del runtime da
timeout locale al task, perdita del lease, richiesta esplicita o drop dell'esecuzione. Una scadenza
di esecuzione segnala il token del task, concede il periodo di grazia limitato configurato per la
pulizia, poi restituisce un timeout tipizzato con una disposizione di retry esplicita. Se in
quell'esatto istante un altro proprietario ha vinto la gara del primo scrittore, il percorso della
scadenza riporta il motivo che ha davvero vinto invece di contraddire il token. Il drop di una
future di esecuzione segnala `ExecutionDropped` in modo sincrono, permettendo alle operazioni figlie
cooperative che hanno conservato il token di fermarsi anche se la loro future genitore non esiste
più. L'esaurimento dell'heartbeat di JetStream imposta `LeaseLost` prima del drop della future
dell'handler.

L'arresto HTTP segue lo stesso ordine: il segnale di runtime condiviso ferma le nuove ammissioni, ad
Axum viene chiesto il drain delle connessioni attive e l'adapter restituisce un timeout strutturato
quando il periodo di grazia configurato scade. Liveness e readiness restano distinte durante la
transizione.

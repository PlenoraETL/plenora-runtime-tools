# Esempio del ciclo di vita di un worker NATS

Questo esempio è la prova di concetto generica di un consumer con la forma di un processo. Si
collega a un deployment NATS JetStream esistente, lega un pull consumer durevole, esegue un worker
Apalis e serve health e readiness HTTP dallo stesso ciclo di vita del runtime. I future degli
handler vengono creati su richiesta fino a `PLENORA_MAX_WORKERS`. Apalis attende capacità prima di
interrogare NATS, così i messaggi in eccesso restano in JetStream finché uno slot non si libera.
Non crea stream, consumer né altra infrastruttura del broker.

Ambiente:

| Variabile | Obbligatoria | Significato |
|---|---:|---|
| `PLENORA_NATS_SERVERS` | No | URL separati da virgole; predefinito `tls://127.0.0.1:4222`. |
| `PLENORA_NATS_TOKEN` | No | Token bearer. Il valore è avvolto in `SecretString` e non viene mai stampato. |
| `PLENORA_NATS_ALLOW_PLAINTEXT` | No | Deve valere `true` (senza distinzione di maiuscole) per consentire un URL `nats://`. |
| `PLENORA_MAX_WORKERS` | No | Massimo di handler concorrenti; predefinito `32`, massimo assoluto `4096`. |
| `PLENORA_NATS_STREAM` | No | Nome di uno stream esistente; predefinito `PLENORA_WORK`. |
| `PLENORA_NATS_DURABLE` | No | Consumer durevole esistente; predefinito `plenora-worker-nats`. |
| `PLENORA_NATS_SUBJECT` | No | Subject di filtro del consumer; predefinito `plenora.work`. |
| `PLENORA_NATS_DLQ_SUBJECT` | No | Subject dead-letter dedicato; predefinito `plenora.work.dlq`. |
| `PLENORA_HTTP_BIND` | No | Indirizzo di bind HTTP; predefinito `127.0.0.1:3001`. |

L'esempio congela all'avvio una capability segnaposto: `plenora.example-tools@v1`, operazione
`execute`. Oltre agli UUID canonici di messaggio e di correlazione, ogni consegna contiene quindi
queste chiavi di routing portabili:

| Chiave di metadati | Valore d'esempio |
|---|---|
| `plenora.capability.name` | `plenora.example-tools` |
| `plenora.capability.version` | `1` |
| `plenora.capability.operation` | `execute` |

Lo stream esistente deve conservare sia il subject operativo sia il subject dead-letter
configurato. Il consumer durevole esistente deve usare ACK esplicito, `max_deliver=5`,
`max_ack_pending` uguale a `PLENORA_MAX_WORKERS` e un'attesa ACK di 30 secondi. Ogni messaggio deve
contenere le chiavi di metadati UUID canoniche `plenora.message.id` e
`plenora.trace.correlation_id`; le consegne malformate vengono terminate senza invocare l'handler.

Gli handler di lunga durata rinnovano il proprio possesso in JetStream ogni cinque secondi. Tre
fallimenti consecutivi dell'heartbeat cancellano il future dell'handler e provocano un NAK
ritentabile. La finestra di fallimento risultante di 15 secondi viene validata perché resti più
corta dell'attesa ACK configurata di 30 secondi.

I fallimenti degli handler usano il producer JetStream dedicato come `DeadLetterSink`. La consegna
originale viene terminata solo dopo che JetStream conferma la pubblicazione nella DLQ. Un sink
mancante, un errore di pubblicazione o `OutcomeUnknown` lasciano l'originale idoneo alla
riconsegna invece di perderlo in silenzio.

L'esempio emette un heartbeat di istanza del worker senza payload ogni dieci secondi e a ogni
transizione del ciclo di vita. Riporta `instance_id`, nome del worker,
`Starting/Ready/Draining/Stopped`, capacità massima, handler attivi e slot disponibili. Un
osservatore di produzione deve consegnare questi snapshot a persistenza o telemetria tramite una
coda limitata in modo esplicito.

Prima di spostare il runner dentro `run()`, un servizio che lo incorpora può conservare
`runner.task_control()`. Quell'handle indipendente dal motore elenca al massimo
`PLENORA_MAX_WORKERS` snapshot senza payload dei task attivi e richiede la cancellazione
cooperativa per ID di task locale all'executor o per ID canonico del messaggio. Gli ID di task
vanno associati all'`instance_id` dell'heartbeat fuori dal processo. La cancellazione richiesta è
terminale per il messaggio corrente del broker; gli handler devono collegare il token di
cancellazione fornito alle operazioni di lunga durata su dati, database o I/O quando quelle
librerie saranno disponibili.

TLS in stile produzione:

```text
PLENORA_NATS_SERVERS=tls://nats.example.internal:4222
```

Testo in chiaro locale esplicito:

```text
PLENORA_NATS_SERVERS=nats://127.0.0.1:4222
PLENORA_NATS_ALLOW_PLAINTEXT=true
```

Eseguire dalla radice del workspace:

```text
cargo run -p plenora-example-worker-nats
```

Mentre il processo è in esecuzione, `GET /health` riporta la health aggregata dei componenti e
`GET /ready` riporta se questa istanza può continuare a ricevere traffico. Un fallimento terminale
del worker o di HTTP e Ctrl-C entrano tutti nello stesso percorso di arresto limitato: fermare
l'ammissione dei worker, fare il drain di HTTP, fare il drain di NATS, poi terminare il runtime.

L'handler registrato è di proposito un segnaposto. Il futuro consumer Plenora registrerà adapter
di proprietà dell'applicazione per `data-tools`, `database-tools` e `IO-tools` nello stesso
registro limitato; quelle librerie non diventano dipendenze dei crate generici del runtime.
Aggiungere un'altra libreria è una registrazione `CapabilityHandler` in più e non cambia il codice
di NATS, Apalis, HTTP o dei worker.

Liste di server, dimensioni degli URL, dimensione del token, concorrenza dei worker, dimensione del
payload, ACK pendenti, tentativi di riconnessione, timeout delle operazioni e drain di arresto sono
limitati. L'output su console contiene identità e stato del ciclo di vita, mai corpi dei messaggi
o credenziali.

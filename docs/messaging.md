# Messaggistica

Il crate messaging definisce envelope neutrali rispetto al broker, confini di serializzazione,
contratti di producer e consumer, proprietà della consegna, acknowledgement, decisioni di retry,
dati di dead-letter e scoperta delle capability.

Nessun codec di payload né broker è obbligatorio. MessageCodec converte valori tipizzati in un media
type e byte opachi. MessageMetadata accetta valori binari e richiede chiavi portabili con namespace
come myapp.tenant o plenora.trace.traceparent. L'output di Debug oscura i byte del payload, i
payload tipizzati e i valori dei metadati. La mappa portabile è fail-closed a 64 voci, 128 byte per
chiave, 8 KiB per valore e 32 KiB di byte complessivi fra chiavi e valori.

I messaggi destinati al worker usano le chiavi di metadati canoniche `plenora.message.id`,
`plenora.trace.correlation_id` e la facoltativa `plenora.message.causation_id`. Le prime due sono
UUID obbligatori per `MetadataMessageDecoder`; metadati d'identità non validi vengono rifiutati
prima dell'esecuzione dell'handler.

Delivery possiede la capability di acknowledgement del suo adapter. Chiamare ack o nack consuma la
delivery, il che rende impossibile un secondo acknowledgement attraverso l'API sicura.

RetryPolicy riceve il tentativo corrente, numerato da uno, e un errore classificato. La policy
standard ExponentialBackoff limita ritardi, tentativi e un budget facoltativo di tempo trascorso. Il
jitter con seme è deterministico. OutcomeUnknown resta distinto dalla pubblicazione confermata e non
viene mai convertito in successo o retry, a meno che l'applicazione non scelga esplicitamente il
retry.

BrokerCapabilities rende individuabili consumo durevole, replay, ordinamento, supporto nativo al
dead-letter e garanzie exactly-once, senza promettere funzionalità che un adapter non può fornire.

`DeadLetterSink` trasforma qualsiasi `MessageProducer` dedicato in un instradamento dead-letter
portabile. Aggiunge metadati limitati di motivo, tentativo, istante del fallimento e il
deterministico `<message-id>.dlq`, senza modificare il payload. L'ID distinto della DLQ impedisce
alla de-duplicazione a livello di stream di JetStream di scambiare il record della DLQ per la
pubblicazione originale. `Confirmed` e `OutcomeUnknown` restano separati fino all'adapter del
consumer.

`plenora-runtime-nats` implementa i contratti con un producer JetStream a subject fisso e un pull
consumer durevole. La configurazione orientata alla produzione richiede TLS per impostazione
predefinita e si aggancia a infrastruttura predisposta in anticipo; testo in chiaro locale e
creazione delle risorse sono opt-in espliciti. La pubblicazione è confermata solo dopo che
l'acknowledgement di JetStream si risolve. I fallimenti ambigui dopo l'invio restituiscono
`OutcomeUnknown`. L'acknowledgement positivo della consegna usa il double ACK confermato dal server;
il lavoro ritentabile si mappa su NAK, `RetryAfter` conserva il suo ritardo in un NAK ritardato
nativo, il rifiuto permanente si mappa su TERM e l'arresto si mappa sul NAK ritardato configurato.

JetStream non dichiara alcuna capability DLQ nativa. Il runner broker di Apalis la emula
esplicitamente usando un producer a subject fisso dedicato a un subject DLQ predisposto. Invia TERM
per la consegna originale solo dopo la pubblicazione confermata nella DLQ. Configurazione mancante,
fallimento della pubblicazione o `OutcomeUnknown` lasciano l'originale senza acknowledgement e
idonea alla riconsegna; un fallimento del TERM dopo la pubblicazione confermata è riportato a parte.

I consumer operativi e di replay richiedono `max_deliver`, `max_ack_pending` e limiti locali in byte
del payload finiti e non nulli. La configurazione della connessione richiede inoltre un limite
finito e positivo ai tentativi di riconnessione, perché la libreria client tratta sia un limite
assente sia un limite pari a zero come un ciclo di riconnessione illimitato. L'aggancio verifica,
prima che il consumo inizi, il timeout di acknowledgement del durable esistente, i limiti di
consegna e di ack pendenti, il filtro, la policy di ACK e la policy di consegna. I messaggi in
arrivo troppo grandi o altrimenti malformati vengono chiusi con TERM senza esporre i loro byte a un
handler e il consumo continua, così una consegna avvelenata non può fermare un worker; il consumer
conta questi rifiuti e conserva l'ultimo motivo sicuro rispetto all'oscuramento. Solo un settlement
fallito o un vero fallimento dello stream termina il consumer.

I metadati binari sono codificati in modo reversibile in header NATS riservati. La configurazione
di replay nomina sia il durable operativo sia quello di replay e ne rifiuta l'uguaglianza, così il
replay non può spostare né sovrascrivere il cursore del consumer operativo. L'adapter dichiara
solo le capability di consumer durevole e di replay. Gli eventi di connessione aggiornano
health/readiness; la riconnessione controllata preserva il consumer durevole, e il drain ordinato si
completa interrogando gli stream del consumer finché non terminano dopo `begin_drain`.

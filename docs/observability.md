# Osservabilità

`plenora-runtime-observability` definisce le convenzioni per il tracing strutturato, la
propagazione del contesto compatibile con W3C, le metriche a bassa cardinalità e l'oscuramento dei dati sensibili,
senza scegliere un backend di telemetria.

Le applicazioni gestiscono installazione del subscriber, campionamento, exporter, collector,
archiviazione e dashboard. Il codice di libreria emette attraverso un sink neutrale rispetto al
backend e resta valido quando si usa il sink no-op.

Il vocabolario stabile delle metriche copre task e spegnimento del runtime, ricezione ed
elaborazione dei messaggi, profondità della coda e scarti del ciclo di vita del worker, stato del
relay dell'outbox, connettività del broker e ritardo del consumer. I contatori degli scarti del
ciclo di vita prendono gli incrementi dallo snapshot del dispatcher del worker e usano solo il
vocabolario limitato di motivi `full` o `closed`. Chiavi e valori degli attributi sono validati e
limitati. Gli identificatori di richiesta, di messaggio e di correlazione stanno negli span o nei
log, non nelle etichette delle metriche.

Segreti, header di autorizzazione e cookie, credenziali del broker, metadati arbitrari, payload,
corpi dei dead-letter, configurazione interna ed errori sorgente concreti non devono essere emessi
per impostazione predefinita. I wrapper diagnostici mostrano un marcatore fisso al posto del valore oscurato, a
meno che un'applicazione non prenda una decisione di divulgazione separata ed esplicita.

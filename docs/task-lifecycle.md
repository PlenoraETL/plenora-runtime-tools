# Ciclo di vita dei task

`plenora-runtime-worker` emette eventi ordinati e privi di payload per ogni tentativo di consegna
di un messaggio:

```text
Queued -> Running -> Succeeded
                  -> Failed
                  -> TimedOut
                  -> Cancelled(reason)
```

Ogni evento porta solo ID del messaggio, ID di correlazione, tentativo, sequenza monotona per
tentativo, ora dell'osservatore e un evento tipizzato. L'avanzamento consiste in `completed_units`
più un `total_units` facoltativo e diverso da zero; messaggi arbitrari, dati di riga, percorsi,
stringhe di connessione ed etichette sono esclusi. Gli heartbeat automatici del ciclo di vita
includono l'ultimo snapshot numerico dell'avanzamento.

`TaskLifecycleObserver` è sincrono e deve essere non bloccante. È un confine di osservazione: la
disponibilità dell'osservatore non cambia mai ACK, NAK, retry o successo dell'handler.
`WorkerLifecycleDispatcher` fornisce ora la coda esplicitamente limitata usata da un futuro adapter
di persistenza. In saturazione scarta l'osservazione più recente, espone i contatori full/closed,
proietta uno stato facoltativo o obbligatorio nella salute del runtime e ha hook per le metriche
neutrali rispetto al backend. L'I/O su database o rete resta vietato dentro `record`; il ricevitore
appartiene all'applicazione, che lo supervisiona.

`WorkerConfig` limita in modo indipendente la concorrenza massima, il drain allo spegnimento, il
timeout di esecuzione facoltativo, il periodo di grazia per la pulizia dopo l'annullamento di un
task e la cadenza facoltativa degli heartbeat del ciclo di vita. Un timeout ha un `RetryDecision`
esplicito. Le durate pari a zero sono rifiutate.

`TaskCancellationToken` conserva il primo motivo di annullamento. Gli handler dovrebbero passarne un
clone alle chiamate cooperative verso gli adapter di database, dati o I/O, quando quelle
integrazioni esisteranno. Lo spegnimento del runtime resta un segnale separato a livello di
processo, così che un handler possa distinguere un drain ordinato da un task che ha perso il lease
o la scadenza.

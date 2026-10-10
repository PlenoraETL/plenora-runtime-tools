# Architettura

`plenora-runtime-tools` fornisce fondamenta riutilizzabili per processi e servizi. Non orchestra le
librerie Plenora di dati, database o I/O e non contiene logica di business.

La stratificazione implementata è:

```text
runtime-worker  ----> runtime-core
       +-----------> runtime-messaging

runtime-capabilities ----> runtime-core
             +----------> runtime-worker
             +----------> runtime-messaging

runtime-outbox  ----> runtime-messaging

runtime-testkit ----> runtime-core
       +-----------> runtime-messaging
       +-----------> runtime-outbox
       +-----------> runtime-worker
       +-----------> runtime-capabilities

runtime-apalis  ----> runtime-core
       +-----------> runtime-worker
       +-----------> runtime-messaging

runtime-nats    ----> runtime-core
       +-----------> runtime-messaging

runtime-http    ----> runtime-core
       +-----------> runtime-messaging

runtime-observability ----> runtime-messaging

runtime-resources ----> runtime-core
       +-------------> runtime-worker

runtime-scheduler ----> runtime-core

runtime-subprocess ----> Tokio process primitives only

runtime-control ----> runtime-messaging
       +-----------> runtime-worker
       +-----------> runtime-resources
       +-----------> runtime-scheduler
       +-----------> runtime-subprocess

runtime-control-http ----> runtime-control + Axum
       +-----------> runtime-worker
       +-----------> runtime-resources
       +-----------> runtime-scheduler
```

Il relay dell'outbox pubblica attraverso il contratto di producer di messaging e non dipende dal
worker. Il testkit dipende da core, messaging e outbox per poter fornire tutti i fake di prima
classe. Dipende anche da capabilities e worker per fornire il fake generico di adapter privo di
payload.

Il crate capabilities è un livello di integrazione applicativa sopra worker e messaging. Possiede un
registro di avvio limitato, un instradamento portabile e versionato e un dispatcher neutrale
rispetto al motore. Le librerie Rust concrete restano dipendenze del consumer Plenora che le
incorpora e sono avvolte da piccoli adapter posseduti dall'applicazione; non diventano mai
dipendenze dei crate di runtime.

Il crate resources osserva la pressione sul processo e controlla solo il cancello di ammissione
reversibile del worker. Lo scheduler dipende solo dai contratti di tempo e di arresto, più il
calcolo cron/fuso orario neutrale rispetto al backend; effetti collaterali del dispatch e
persistenza restano confini posseduti dall'applicazione. Il crate subprocess possiede il
contenimento ma nessun protocollo applicativo. Il crate control è un livello di aggregazione solo
verso l'interno; il suo adapter HTTP separato possiede il trasporto e richiede un'autorizzazione
dell'applicazione.

Il package HTTP è un adapter in ingresso pur vivendo insieme ai crate riutilizzabili: i tipi Axum e
Tower si fermano al confine di quel package. Il package observability definisce convenzioni stabili
per span, propagazione, metriche e oscuramento. Le applicazioni installano subscriber ed exporter
concreti.

Tokio, Tower, Apalis, NATS JetStream, Axum e i driver di database sono scelte implementative, non
concetti dell'API del consumer. Le decisioni rilevanti non stabilite dalla specifica richiedono un
ADR.

L'adapter Apalis fornisce anche un `BrokerWorkerRunner` neutrale rispetto al broker. Le
applicazioni compongono all'avvio un `MessageConsumer` come `JetStreamConsumer` con quel runner.
Questo mantiene l'adapter NATS indipendente dalla meccanica del worker, permettendo al tempo stesso
alla readiness di Apalis di applicare backpressure prima che venga prelevata la consegna successiva
dal broker.

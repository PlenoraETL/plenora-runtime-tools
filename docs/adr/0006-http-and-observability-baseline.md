# ADR 0006: base di HTTP e osservabilità

- Stato: accettato
- Data: 2026-08-16

## Contesto

La Milestone 4 aggiunge un confine di servizio HTTP riusabile e convenzioni comuni di telemetria.
La superficie HTTP deve comporsi con le route dell'applicazione senza assorbire l'autorizzazione di
business. La telemetria deve propagarsi attraverso i confini HTTP e di messaggistica senza scegliere
un exporter, un collector o un backend di un fornitore. Gli endpoint di health e la diagnostica non
devono rivelare per impostazione predefinita dettagli dei componenti, credenziali, payload o
configurazione interna.

## Decisione

- Fissare Axum 0.8.9 e tower-http 0.7.0 con le feature predefinite disattivate e solo le feature
  server HTTP, JSON, Tokio, tracing, request-id, sensitive-header e trace richieste dall'adapter.
- Tenere i tipi HTTP di Axum e Tower dentro `plenora-runtime-http`; i contratti di core,
  messaging, worker e outbox restano indipendenti dallo stack web.
- Trattare `x-request-id` come identificatore diagnostico locale al trasporto e `x-correlation-id`
  come identificatore dell'operazione attraverso i confini. Validare i valori in ingresso limitati,
  generare quelli mancanti, collegarli entrambi al contesto della richiesta e restituirli negli
  header di risposta.
- Esporre liveness e readiness come endpoint separati. Restituire per impostazione predefinita solo
  uno stato aggregato e sicuro rispetto alla redazione; i messaggi dettagliati dei componenti
  restano interni.
- Collegare l'ammissione HTTP e il servizio ordinato al segnale di arresto del runtime con un
  periodo di grazia limitato.
- Fissare tracing 0.1.44, OpenTelemetry API 0.32.0 e tracing-opentelemetry 0.33.0. Il crate di
  osservabilità fornisce convenzioni e un ponte di interoperabilità, ma non installa un subscriber
  globale, un SDK, un exporter, un collector o un backend di un fornitore.
- Tenere le metriche dietro un piccolo contratto di sink con nomi di strumenti stabili e attributi
  validati a bassa cardinalità. Fornire un default no-op, così le librerie non richiedono mai un
  runtime di telemetria.
- Redigere segreti, credenziali, payload, header di autorizzazione, cookie e sorgenti arbitrarie
  di errori interni dall'output diagnostico predefinito.

## Conseguenze

Le applicazioni mantengono la responsabilità di composizione delle route, autenticazione,
autorizzazione, installazione del subscriber, scelta dell'exporter e configurazione del collector.
HTTP e messaggistica possono condividere il contesto di correlazione e di trace senza che uno dei
due contratti core dipenda da un backend di telemetria. Adapter di metriche e tracing specifici di
un backend si possono introdurre in seguito senza cambiare il ciclo di vita del runtime né i
contratti di messaggistica.

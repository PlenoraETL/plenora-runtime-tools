# ADR 0005: base degli adapter Apalis e NATS

- Stato: accettato
- Data: 2026-08-15

## Contesto

La Milestone 2 introduce Apalis come adapter del motore di worker e NATS JetStream come adapter di
broker concreto. Entrambe le librerie evolvono indipendentemente dai contratti Plenora e devono
restare dietro i confini degli adapter. Pubblicazione e acknowledgement possono avere un effetto
remoto sconosciuto, mentre una consegna Plenora possiede la propria capability di acknowledgement
e non si può clonare in sicurezza.

## Decisione

- Fissare Apalis 0.7.4 stabile con le feature predefinite disattivate. Attivare solo la
  limitazione della concorrenza e la cattura dei panic. Non adottare la release candidate di
  Apalis 1.0 nel rilascio iniziale.
- Tenere la classificazione dei retry in Plenora. Non applicare un layer di retry di Tower a una
  consegna posseduta.
- Fissare async-nats 0.50.0 con le feature predefinite disattivate e attivare JetStream, TLS con
  Ring, conversione temporale Chrono e credenziali NKey.
- Richiedere TLS per impostazione predefinita per le connessioni di produzione. L'uso locale in
  chiaro richiede una configurazione esplicita.
- Per impostazione predefinita, legare i producer a un subject esplicito e i consumer a uno stream
  e a un consumer durevole esistenti. La creazione di infrastruttura richiede un flag di
  provisioning esplicito.
- Una pubblicazione JetStream è confermata solo dopo che il suo acknowledgement di pubblicazione è
  risolto. I fallimenti ambigui dopo l'invio restano OutcomeUnknown e non vengono mai ritentati
  implicitamente.
- Usare acknowledgement espliciti del pull consumer. L'acknowledgement positivo attende la conferma
  del server. I fallimenti ritentabili diventano NAK; quelli permanenti e quelli rifiutati dal
  consumer diventano TERM. L'arresto diventa un NAK ritardato configurato dall'adapter.
- Esporre una riconnessione controllata per la rotazione delle credenziali e la verifica
  deterministica. Il drain ordinato è asincrono: dopo `begin_drain`, i chiamanti interrogano gli
  stream del consumer finché non terminano prima di completare l'arresto del processo.
- Dichiarare le capability in modo conservativo: consumer durevoli e replay sono veri; consegna
  ordinata, dead-letter nativo ed exactly-once restano falsi.
- Preservare i metadati binari Plenora tramite una codifica testuale reversibile in header NATS
  riservati. Non mettere mai segreti grezzi o payload nell'output Debug o di errore.
- Comporre NATS e Apalis al confine dell'applicazione tramite il `BrokerWorkerRunner` indipendente
  dal broker; nessuno dei due adapter concreti dipende dall'altro.
- Interrogare il broker solo dopo che la readiness di concorrenza di Apalis concede capacità.
  Preservare il ritardo di retry in `NackReason::RetryAfter` e tradurlo nel NAK ritardato di
  JetStream.
- Fissare il server di integrazione effimero all'immagine ufficiale multi-architettura
  nats:2.14.5-alpine3.22 con digest OCI
  sha256:d4ac35882ac65aff236cd65b9d3fa4d24332c681e1a85f94eedccd3cdd65b1da.

## Conseguenze

Il core dei worker resta indipendente da Apalis e la messaggistica resta indipendente da NATS.
Le applicazioni devono farsi carico dell'idempotenza per la consegna at-least-once e per la
pubblicazione incerta. I test degli adapter possono esercitare il comportamento reale di JetStream
senza creare implicitamente infrastruttura di produzione. Un'adozione futura di Apalis 1.0 o di un
diverso provider TLS richiede una nuova revisione di compatibilità.

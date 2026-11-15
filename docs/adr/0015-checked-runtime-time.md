# ADR 0015: aritmetica temporale checked del runtime

Stato: adottata nel codice; qualifica multipiattaforma aperta in [qualification](../qualification.md#tempo-rt).

## Decisione

`plenora-runtime-core::temporal` e il modulo unico delle conversioni clock/calendar,
dell'ammissione timeout/intervallo/retry e delle deadline monotone. I consumer
conservano errori specifici per campo zero e aggiungono categorie di range senza
valori nei messaggi. Nessun nuovo pacchetto esterno: chrono eredita il pin esistente;
messaging e subprocess aggiungono soltanto l'arco interno verso core.

Timeout relativi, campionamenti, heartbeat, ack wait, connessioni, drain e lease
operativi accettano durate positive fino a 24 ore incluse. La validazione verifica
anche `Instant::checked_add`; i reset usano la deadline checked al momento del reset.
Retry NATS e shutdown NAK ammettono anche zero (retry immediato), massimo 24 ore;
i nanosecondi devono essere rappresentabili esattamente come i64 prima del wire.
Il limite vale anche per il NAK ricevuto direttamente dall'API, non solo dalla config.

Le deadline assolute del binding sono date contrattuali: non si riduce il loro
orizzonte a un giorno. Scaduta o uguale al clock significa timeout prima dell'handler;
clock fuori chrono significa internal, deadline monotona non rappresentabile significa
resource_limit, senza invocazione. Il dispatcher consuma la stessa deadline verificata.

Cron ammette ancore UTC negli anni 1..=9999, con margine rispetto ai limiti chrono
per offset locali e iterazione del calcolatore pinnato. `TimeRange` distingue il rifiuto
da `NoFutureOccurrence`; `try_next_after` conserva questa distinzione. Il vecchio trait
`SchedulePlan::next_after` dichiara il rifiuto con None, come per esaurimento del piano.
Le conversioni calendariali preservano segno e nanosecondi; leap second e perdita di
precisione del clock di piattaforma sono rifiutati verificando il round-trip.

## Semantiche distinte preservate

Intervallo di calendario `FixedIntervalPlan`, jitter e misfire grace non sono timeout
operativi: conservano il dominio Duration e la fine del piano su checked overflow.
Il backoff generico conserva la saturazione al cap dichiarata dalla sua policy;
l'adapter rifiuta il ritardo fuori dal proprio dominio prima del settlement NATS.
Retention del journal resta limitata a 7/90 giorni, data di dominio persistita.
Nessun float temporale in ingresso nel perimetro di produzione esaminato.

`RuntimeHandle::with_config` resta infallibile: una config temporale invalida viene
rifiutata da spawn con `SpawnError::InvalidTime`; shutdown abortisce gli eventuali
task e restituisce `DrainOutcome::InvalidTime`, mai Completed o TimedOut fittizi.
Un clock dead-letter invalido restituisce Metadata con sorgente TimeError: non si
pubblica e non si termina l'originale. Resta la normale responsabilita del broker
per la redelivery di una delivery non confermata.

## Conseguenze e verifica

Riduzione intenzionale del dominio dei timeout; nessuna normalizzazione o clipping.
La nuova superficie pubblica in `boundary-api.txt` comprende primitive checked,
categorie di errore e query cron fallibile, non un nuovo ingresso di dispatch.
La guardia architetturale riusa il sanitizer di produzione e vieta le famiglie
infallibili note, con prove di regressione; non e un'analisi del flusso dati Rust.
Test deterministici usano clock iniettati e limiti costruiti con checked arithmetic.
La qualifica Linux/Windows, i backend reali e il limite della guardia sono registrati
in [security-review](../security-review.md#tempo-rt) e [qualification](../qualification.md#tempo-rt).

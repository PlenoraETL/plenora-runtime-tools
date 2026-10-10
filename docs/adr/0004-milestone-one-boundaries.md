# ADR 0004: confini di dipendenza e di esecuzione della Milestone 1

- Stato: accettato
- Data: 2026-08-15

## Contesto

Il diagramma illustrativo delle dipendenze della specifica si può leggere come se mettesse
l'outbox sotto il worker, mentre la sua API dell'outbox e l'ordine di merge raccomandato
descrivono la pubblicazione tramite MessageProducer. Anche l'esecuzione dei worker ha bisogno di
un'implementazione a concorrenza limitata senza vincolare in anticipo l'API core ad Apalis.

## Decisione

- Runtime-outbox dipende da runtime-messaging e pubblica tramite MessageProducer. Non dipende da
  runtime-worker.
- Runtime-worker possiede i contratti di handler, contesto, configurazione, errore, delega dei
  retry, ammissione e drain. Non possiede alcuna coda e non crea alcun task.
- Apalis implementerà un adapter dopo che i contratti core del worker saranno congelati.
- Runtime-testkit dipende da core, messaging e outbox, così fornisce direttamente fake di broker,
  outbox, inbox e idempotenza. Il suo test di accettazione della Milestone 1 usa worker come
  dipendenza di sviluppo.
- Il workspace radice resta l'unico punto di integrazione di dipendenze e lockfile durante il
  lavoro in parallelo.

## Conseguenze

Il relay dell'outbox si può riusare con qualunque producer e testare senza un motore di worker. La
concorrenza dei worker è limitata senza far trapelare tipi dell'adapter. L'implementazione in
parallelo dei crate resta sicura finché manifest condivisi, lockfile e documentazione trasversale
vengono integrati in serie.

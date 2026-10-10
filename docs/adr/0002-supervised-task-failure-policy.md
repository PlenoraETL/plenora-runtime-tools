# ADR 0002: policy di fallimento dei task supervisionati

- Stato: accettato per runtime-core 0.1
- Data: 2026-08-15

## Contesto

La specifica richiede che il fallimento di un task critico renda il runtime unhealthy e avvii
l'arresto. Richiede che il comportamento dei task opzionali sia configurabile, ma non definisce la
reazione predefinita al fallimento di un task richiesto.

## Decisione

- Il fallimento critico segna la health come unhealthy, la readiness come non pronta e avvia
  l'arresto.
- Il fallimento richiesto segna la health come degradata e la readiness come non pronta, ma tiene
  vivo il processo per la diagnostica e il ripristino esterno.
- Il fallimento opzionale registra un report e, per impostazione predefinita, degrada la health
  senza cambiare la readiness.
- Il fallimento opzionale può invece essere ignorato nell'aggregazione della health o configurato
  per avviare l'arresto.

Ogni fallimento resta disponibile nei report dei task indipendentemente dalla policy di health. I
panic e la cancellazione del runtime vengono convertiti in fallimenti di task governati.

## Conseguenze

Health e readiness restano distinte. Un processo può restare vivo rifiutando nuovo lavoro dopo il
fallimento di un componente richiesto. La policy opzionale può cambiare senza esporre un tipo
specifico dell'executor.

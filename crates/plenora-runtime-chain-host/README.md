# Host di qualifica scriptato

ScriptedHost usa PostgreSQL per lo stato e documenti fixture per gli esiti.
Non invoca librerie ETL reali. Richiede migrate prima delle operazioni durevoli;
costruire l'oggetto non attesta readiness.

NATS e request/reply del solo harness, non sostituisce il boundary. La readiness
del listener usa un canale oneshot dopo subscribe/flush, senza sleep.
Decisione, rischio e chiusura: [ADR 0010](../../docs/adr/0010-scripted-chain-facades.md).

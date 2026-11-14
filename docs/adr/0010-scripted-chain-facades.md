# ADR 0010: facciate di qualifica con adapter scriptati

Stato: implementazione sperimentale, non profilo di produzione qualificato.

Il coordinatore Rust possiede l'algoritmo; CLI, binding PyO3 sync/async e client
remoto serializzano lo stesso enum di richieste. PostgreSQL resta lo store del
run. Il client NATS riusa il pin del workspace; il request/reply qui introdotto
è un harness esplicito di qualifica e non sostituisce il percorso protetto
boundary/dispatcher/JetStream di P2.3/P2.4.

Regola: nessuna superficie può rivendicare parità del prodotto prima del
cablaggio al boundary. Ambito: `ScriptedHost`, `RemoteClient`, CLI scripted e
wheel sperimentale. Rischio: scambiare parità del coordinatore per ammissione,
negoziazione e settlement del prodotto. Condizione di chiusura: ingresso e
invocazioni attraverso il dispatcher durevole, corpus RT su cinque superfici,
broker/store reali e prove su artefatti installati. Il timeout remoto di attesa
non modifica la deadline del run e non attesta cancellazione.

Gli adapter sono fake dichiarati. Il loro esito è configurato da documenti
fixture, senza invocare DATA, DB, IO, STORAGE o REST reali.

# Sintassi CLI del profilo scriptato

Le superfici restano sul profilo di adapter scriptati, senza attestazione delle
librerie ETL. Ogni comando seleziona `--config FILE` oppure `--server URL --subject
SUBJECT` e richiede `--json` (o `--json-lines` per gli eventi):

- `capabilities`
- `chain validate --plan FILE`
- `chain submit --plan FILE --run UUID` e `chain run --plan FILE --run UUID`,
  con `--idempotency-key KEY` e `--deadline-ms MILLISECONDS` opzionali
- `chain status --run UUID`, `chain result --run UUID`
- `chain events --run UUID --cursor SEQUENCE --limit SIZE`
- `chain cancel --run UUID --revision REVISION`, `chain resume --run UUID --revision REVISION --recovery MODE`
- `artifact get --ref REFERENCE`, `report get --run UUID`

I numeri sono decimali canonici. Opzioni duplicate, inutilizzate o endpoint misti
sono rifiutati. Gli eventi JSON-lines terminano alla prima pagina vuota: non
significa che il run sia terminato. Errori successivi agli eventi sono una riga
Response JSON e un exit code non zero; nessuna riga gia emessa viene ritirata.
`--recovery` accetta `observe`, `continue`, `reconcile` (default compatibile).
Observe legge soltanto la revisione; continue rifiuta un tentativo attivo/ignoto;
reconcile consulta esclusivamente le prove del provider fidato. Nessun modo
permette replay ignoto o supera budget, consenso, deadline e stop al primo errore.
Il chiamante non puo fornire una prova di stop. `chain report` resta alias di
`report get`; `--report-index N --offset N` seleziona una pagina bounded.

La risposta contiene `value`, `error` e `failure`: gli assi strutturati provengono
da Rust, con risultato/contesto conservato separatamente. Exit code: 0 controllo
riuscito, 2 rifiuto, 3 fallimento noto, 4 recovery, 5 cancellazione confermata,
6 esito non ottenuto (incluso timeout della chiamata). `submit` con 0 e soltanto
accettazione; `result` con 0 puo leggere un esito fallito. Dettagli in
[interfaccia](../../docs/chain-interface.md).

Restano disponibili `scripted-call CONFIG` e `remote-call SERVER SUBJECT` con
Request JSON da stdin e Response JSON su stdout. CONFIG puo contenere credenziali
e va protetto dall'host. Il timeout remoto non prova cancellazione.
Limiti: [ADR 0010](../../docs/adr/0010-scripted-chain-facades.md).


Il budget di osservazione remoto e separato dalla deadline autorizzata del run:
`--observation-timeout-ms N` nei comandi CLI con `--server`,
`Client.remote(..., observation_timeout_ms=N)` e `AsyncClient.remote(...)`,
`RemoteClient::connect_with_timeout(server, subject, N)` in Rust. Default 10000 ms;
intervallo 1..86400000 ms. Un timeout non cancella, non ripete e non estende il run.
Il client Rust e l'unica autorita del timeout; il timer implicito NATS e disabilitato.
I comandi legacy remote-call conservano il default; la sintassi chain espone il parametro.

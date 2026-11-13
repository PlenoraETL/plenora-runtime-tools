# Binario sperimentale

Le superfici restano sul profilo di adapter scriptati, senza attestazione delle
librerie ETL. Ogni comando seleziona `--config FILE` oppure `--server URL --subject
SUBJECT` e richiede `--json` (o `--json-lines` per gli eventi):

- `capabilities`
- `chain validate --plan FILE`
- `chain submit --plan FILE --run UUID` e `chain run --plan FILE --run UUID`,
  con `--idempotency-key KEY` e `--deadline-ms MILLISECONDS` opzionali
- `chain status --run UUID`, `chain result --run UUID`
- `chain events --run UUID --cursor SEQUENCE --limit SIZE`
- `chain cancel --run UUID --revision REVISION`, `chain resume --run UUID --revision REVISION`
- `artifact get --ref REFERENCE`, `report get --run UUID`

I numeri sono decimali canonici. Opzioni duplicate, inutilizzate o endpoint misti
sono rifiutati. Gli eventi JSON-lines terminano alla prima pagina vuota: non
significa che il run sia terminato. Errori successivi agli eventi sono una riga
Response JSON e un exit code non zero; nessuna riga gia emessa viene ritirata.
La prova di recovery scriptata e nella configurazione host: non esiste ancora
un controllo pubblico `--recovery`, ne un worker autonomo qualificato.

Restano disponibili `scripted-call CONFIG` e `remote-call SERVER SUBJECT` con
Request JSON da stdin e Response JSON su stdout. CONFIG puo contenere credenziali
e va protetto dall'host. Il timeout remoto non prova cancellazione.
Limiti: [ADR 0010](../../docs/adr/0010-scripted-chain-facades.md).

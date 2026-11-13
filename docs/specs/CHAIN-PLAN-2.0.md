# CHAIN-PLAN-2.0 — piano, esito, eventi ed errori catene

Stato: **bozza normativa locale da proporre**, versione 2.0, 2026-11-13.
Le parole DEVE, NON DEVE e RIFIUTA sono vincolanti per questa bozza.
Non attestano ratifica centrale, codice Rust, engine, adapter o superfici pubbliche.
P1.2 specifica il formato; P1.3 realizza tipi e validazione preventiva contro
discovery effettive. Le cinque superfici adottano lo stesso contratto.

## Fonti e compatibilità

Decisioni del titolare: [ADR 0008](../adr/0008-chain-product-scope.md).
Fonte progettuale: PROGETTO-CATENE del 12 novembre, B, C, F/P1.2 e G,
digest registrato in [inventario](../chain-features.json).
COMPOSITION-1.0 e decisione 0023 sono stati letti dai blob `origin/main` di
plenora-contracts, commit `38d68b402389855edbcecb7ce57c31e91282cb4e`, percorsi
`specs/composition/COMPOSITION-1.0.md` e
`decisions/0023-primary-geometry-across-components.md`. Nessuna modifica a quel repository.

Identificatori proposti: `plenora-chain-plan-v2`, `plenora-chain-result-v2`,
`plenora-chain-event-v2`, `plenora-chain-error-v2`; `schema_version` è sempre 2.
Capability `plenora.runtime-tools`, operazioni chain.*@2 e nuovi archi richiedono
ratifica distinta. Catene v2 NON significa Runtime Binding 2.0: wire resta 1.
Pacchetto, catalogo, operazione, payload, piano DATA e Arrow sono assi indipendenti.
Nessun contratto, catalogo, schema o vettore v1 cambia significato.

Schemi eseguibili Draft 2020-12:
[piano](../../contracts/chains-v2/schemas/chain-plan-v2.schema.json),
[esito](../../contracts/chains-v2/schemas/chain-result-v2.schema.json),
[evento](../../contracts/chains-v2/schemas/chain-event-v2.schema.json),
[errore](../../contracts/chains-v2/schemas/chain-error-v2.schema.json),
[definizioni comuni](../../contracts/chains-v2/schemas/common.schema.json).
Ogni oggetto ha campi chiusi; le mappe hanno nomi vincolati e valori tipizzati.
Non esistono estensioni libere, JSON pointer o interpolazione di payload.
Le categorie, le fasi e il retry referenziano lo schema comune error-v1 fissato
nel repository; non se ne introduce una copia divergente.

## Documento, identità e digest

UTF-8 senza BOM, membri duplicati rifiutati prima della deserializzazione,
nessun NaN/Infinity, surrogate Unicode isolate o numero floating point.
I numeri del contenitore sono interi JSON lessicali fra 0 e 2^53−1, positivi
dove lo schema richiede; `-0` non è canonico; `2.0`, `2e0`, booleani e stringhe non sono versioni.
I payload di dominio sono documenti separati: possono contenere numeri del
proprio contratto senza modificarli o convertirli in questo contenitore.
UUID minuscoli con trattini, ID e porte ASCII `[a-z][a-z0-9_-]*`, 1–64 caratteri.
Versioni positive fino a 2147483647; pacchetti esatti major.minor.patch,
SHA-256 esadecimali minuscoli; niente latest, range o fallback.

Massimi: 1 MiB UTF-8 per documento di questo profilo, 1024 step,
64 porte complessive input+output per step, profondità JSON 64.
La radice ha profondità 1; ogni valore figlio e chiave aggiunge un livello.
Parametri e rapporti esterni hanno quote e validazione del proprio contratto,
anche quando non sono inline. Il deployment può solo restringere i limiti.
Gli altri massimi di mappe/array/stringhe sono fissati dagli schemi.

Canonizzazione proposta `CHAIN-C14N-1`: dopo parser e schema, ordinare le
chiavi lessicograficamente per code point, mantenere ordine degli array,
emettere UTF-8 senza BOM, spazi o newline; stringhe JSON con escape obbligatori
per virgolette, backslash e controlli (escape brevi per backspace, tab, newline,
form feed e carriage return, altrimenti `\u00xx` minuscolo), Unicode restante
letterale; interi decimali, booleani/null JSON. Nessuna normalizzazione Unicode.
Digest SHA-256 di questi byte, vettori nel test. Comprende TUTTO il piano,
incluse annotazioni, parametri referenziati per digest, risorse, budget e consenso.
Una diversa revisione o destinazione richiede nuovo digest e autorizzazione.
Non è una dichiarazione di adozione RFC 8785 o del digest IPC come uguaglianza Arrow.

## Piano e step

Il piano risolto contiene `plan_id`, `revision`, `components`, `inputs`, `steps`,
`outputs`, `budgets`, `execution`, `loss_policy`, `requirements`; `annotations`
è solo una mappa limitata di identificatori APP senza privilegi.
Componenti: alias, identità, versione pacchetto, digest artefatto/discovery,
catalogo referenziato per digest, SHA contratti, claim. Si confrontano esattamente
con il registro congelato dell'host; il client non può certificare la propria discovery.

Ogni step contiene ID, `step_version=1`, componente, nome operazione pubblico
e versione numerica separati, contratti input/output esatti, parametri, ingressi
e uscite nominati, `depends_on`, budget, materializzazione e retry.
`parameters` è un riferimento a documento di dominio immutabile con contratto
e SHA-256, non un oggetto JSON libero: l'adapter DEVE validarne l'intero payload
contro lo schema dell'operazione e inserirvi le porte solo negli slot dichiarati.
Questa forma evita di duplicare gli schemi delle librerie. Il riferimento non
esenta dal limite cumulativo `request_bytes` né autorizza a risolverlo prima
dell'ammissione del protocollo. Un riferimento opaco non è URL, path o credenziale.
Connessioni, segreti e destinazioni sono binding protetti dell'host, mai segreti inline.

Ogni input ha una sola sorgente: input iniziale del piano oppure step/porta.
Una porta dichiara contratto, content type, cardinalità, obbligatorietà,
digest schema (null se dinamico) e requisito Primary Geometry. Il profilo iniziale
ammette cardinalità `one` e sole porte obbligatorie. Gli output multipli sono
porte distinte; output opzionali e cardinalità diverse richiedono estensione,
non vengono ignorati. Output pubblici indicano porta esatta, contratto atteso
e pubblicazione durevole. Sink senza output hanno comunque dipendenze di controllo.

Modalità unica: sequential, max_parallel_steps=1, on_error=stop, recovery=durable.
Ordine topologico con scelta dell'ID lessicograficamente minimo fra i pronti,
ricalcolata dopo ogni step; ordine dell'array non implica dipendenze.
Fan-in/fan-out sono espliciti, nessuno step viene eliminato dal motore.
Errore terminale: nessun nuovo avvio; discendenti blocked, altri skipped,
successi precedenti immutati. Nessuna compensazione o transazione distribuita.

## Archi e Arrow

[Archi proposti](../../contracts/chains-v2/proposed-edges.json) sono bozze
separate da pipelines-v1/file-pipelines-v1. Una coppia di versioni v1 non
autorizza run@3, io.read/write@2 o write@2. `direct` richiede contratto e content
type comuni; `adapter_required` un adapter nominato/versionato presente e
qualificato; `provisional` è sempre rifiutato. Un arco direct riguarda le porte
Arrow, NON l'intero documento io.read → richiesta data.run.
I descrittori di mapping devono conservare tutti i manifest e rapporti.
Un digest schema produttore dinamico (null) puo essere verificato contro quello
fisso del consumer alla barriera obbligatoria; gli altri attributi delle porte
direct devono coincidere esattamente. Il manifest originale non si riscrive.

Handoff: `plenora-arrow-interchange-v1`, IPC stream/file, o valori nativi
compatibili; il profilo iniziale effettua checkpoint durevoli tra step.
Preservare schema, field metadata anche ignoti, nullabilità, tipi, ordine e
significato delle righe, field_id, geometrie, CRS/assi e primaria. Nessuna stringa
implicita, colonna scartata o geometria scelta per posizione. Streaming simultaneo,
spill e cancellabilità si provano sul descrittore, non dal content type.
La memoria in-process non è riprendibile dopo crash senza pubblicazione durevole.

Decisione 0022: identità conservata attraverso rename/scrittura/rilettura;
provenienza DB separata; `mixed` non enumerabile omette types, mai stringa vuota.
Decisione 0023: `plenora.primary_geometry` è schema metadata, field_id oppure
`none`. Una geometria senza key è primaria; più geometrie senza key sono
trasportabili senza primaria. Rimuovere la primaria non ne elegge un'altra;
una sola geometria residua non primaria emette `none`. Rename conserva identità;
remapping aggiorna la key tramite provenienza. Join multi-input non elegge;
append/union conservano solo se tutte le primarie confluiscono nello stesso
campo, non perché gli ID numerici coincidono. Operando esplicito risolto sullo
schema ricevuto prima del completamento. Key invalida: `schema`, classe
vocabolario dopo campi e prima di CRS; operando/primaria mancante: `schema`,
classe operazione. GeoParquet senza primaria: `unsupported`. Mai invalid_plan.
Sink mono-geometria non scarta le altre; hop senza claim Primary Geometry
non garantisce conservazione semantica solo perché trasporta byte sconosciuti.

Il controllo del claim del produttore e del requisito Primary Geometry vale
anche per gli output pubblici del piano, senza un consumer step successivo.

STORAGE→IO richiede integrità, pubblicazione confermata, identificazione formato
e descrittore file-pipeline. REST→Arrow deve consumare il risultato completo,
ordine/cardinalità/errori record/parzialità. Arrow→REST ha contratto distinto
per tipi/null/row identity/batch/riassociazione: non è l'inverso implicito.
Query DB summary JSON non è una sorgente Arrow.

## Budget, consenso e autorizzazione

Budget globale obbligatorio: memory_bytes, elapsed_ms, rows_read, rows_produced,
artifact_bytes, spool_bytes, request_bytes, result_bytes, max_attempts.
Tutti finiti e positivi; zero/null non significa illimitato. Budget step solo
restrittivi. Limite effettivo minimo di piano, step, host, componente e trasporto;
inapplicabilità o insufficienza della prenotazione produce rifiuto.
Righe e tentativi sono lavoro cumulativo, inclusi fallimenti/retry/riletture.
Byte pubblicati e spool vivo sono distinti; memoria è prenotazione globale
comprendente buffer, copie e concorrenza interna del componente. Non è hard RSS.

Deadline effettiva fissata all'accettazione: minimo fra deadline chiamante
e durata globale; step usa residuo globale e limite locale. Attese, backoff e
pubblicazione consumano tempo; nessun rinnovo al restart. Piano senza deadline
assoluta duplicata: sul runtime vive soltanto nelle metadata. Cleanup ha finestra
finita separata, che non autorizza nuovi effetti. Clock UTC persistito, timer
monotono locale e ManualClock nelle prove engine. Quote di risultati, artefatti,
rapporti ed errore terminale distinte; mai troncare per rientrare nel limite.

Consenso default deny. `loss_consent` è consentito solo per database.write@2;
assente/false nega. True richiede un grant univoco per step, operazione/versione,
destinazione, digest ingressi e digest documento parametri. `input_digest` è
CHAIN-C14N-1 di `{bindings: step.inputs, plan_inputs: plan.inputs}`; il digest
del piano autorizzato lega anche tutti i predecessori e l'intero grafo.
Il digest effettivo degli artefatti prodotti si fissa nel journal prima dello
step; variazioni alla ripresa richiedono recovery/nuova autorizzazione.
Nessun grant extra, globale, su altro step o diverso input è valido.
Una sola conferma informata del piano completo, verificata dall'host contro il
contesto autenticato. Il documento non è una firma o una prova di autorizzazione.
La referenza non risolve autorizzazioni reali: quelle restano un gate P1.3/host.

L'adapter porta il booleano invariato fino a accept_loss/LossConsent.
Niente consenso dedotto da lossy/native/warning/dry-run o successo precedente.
Il consenso DB non autorizza perdite IO/REST/conversioni e non scavalca strict,
chiavi/range/provider/formati. Report WriteOutcome/loss_report v3 conservati
integralmente anche negli errori; risk/evaluation/effective distinti, rollback
confermato conserva evaluation ed effective vuoto. Conservative non significa
alterazione provata, conteggi per regola non sono celle uniche. Ratifica write@2
e schemi DB v2/v3 è gate separato, non dedotta dalla versione pacchetto.

## Esiti e rapporti

Esito terminale: identità run/piano/digest/revisione, result_message_id,
state_revision, tempi, contatori, TUTTI gli step inclusi non avviati, output
nominati disponibili, errore primario/altri ordinati, rapporti, recovery, delivery.
StepResult conserva operazione/versione, attempt, manifest input/output,
stato, assi, errori, contatori, tempi, stop_confirmed e upstream_result integrale.
Rapporti ed esiti upstream sono documenti autenticati con contratto e SHA-256:
questa forma referenziata è esplicita nel contratto, non sostituzione silenziosa
di campi upstream obbligatori. Il sistema DEVE conservarli recuperabili e integri.
Nessun contenuto di report viene copiato in message/Display/Debug.

Artifact: ref autorizzato, porta/contratto/content type/schema, byte reali,
SHA-256, righe/colonne note o null, producer/attempt, pubblicazione, lease e
rapporti. Solo confirmed può soddisfare un output succeeded. Digest byte IPC
non equivale a uguaglianza semantica. Lease fan-out dura fino all'ultimo consumer.

`succeeded` richiede tutti gli step riusciti, tutti gli output confermati,
rapporti completi e nessun errore; losses=present rende visibili perdite consentite.
Report non disponibile NON equivale a zero perdite. Losses è none/present/unavailable;

La completezza comprende anche i rapporti dentro ogni errore primario e negli
altri errori di catena e step: `reports_complete=true` non può ignorarli.
aggregazione per step/output/regola nei documenti completi, nessun totale inventato.
`rejected` prima degli effetti; `failed` fallimento noto; `partial` completamento
parziale noto; `cancelled` solo arresto confermato; `requires_recovery` per ignoto.
Effetti di dominio, pubblicazione e consegna sono assi distinti: persisted non
prova delivery confirmed, risposta persa non cancella un commit noto.

Aggregato remote_effect: unknown prevale e conserva commit noti; altrimenti
partial per effetti parziali o commit prima di arresto incompleto; committed
per effetti previsti tutti confermati; none se nessun effetto residuo provato.
rolled_back resta nell'esito step e aggrega none se non ci sono altri effetti.
Letture pure non diventano committed solo perché terminate; le pubblicazioni
contano secondo il contratto del componente. Retry intera catena NON è OR dei
retry step: committed/partial/unknown vietano safe/after/requires_idempotency_key;
restano never/quarantine/requires_recovery. resource_limit con none richiede never.
Gli errori e gli step vincolano l'aggregato: unknown non può diventare none,
requires_idempotency_key non può diventare safe e after non perde il delay minimo.
I contatori di lavoro cumulativo dell'esito sono la somma degli step; max_attempts
coincide con i tentativi registrati. Memoria, spool vivo e durata globale non si
sommano come righe. I contatori ammessi non superano il budget: la richiesta che
eccederebbe una quota viene fermata prima del consumo. Inizio/fine sono ordinati
alla precisione delle nove cifre frazionarie ammesse, senza troncamento.

## Errori ed eventi

Errori con categoria/fase/remote_effect/retry del vocabolario comune,
code chiuso, step_id opzionale tramite null, causa upstream e report referenziati.
Message fisso `Chain operation rejected or incomplete.`; nessun payload,
valore metadata, nome dato, segreto, path o messaggio eccezione incluso.
L'errore originale completo resta nel documento protetto, non viene riscritto
per nascondere un commit unknown o convertire schema/unsupported in invalid_plan.

| Codice | Categoria/fase della validazione di riferimento | Significato |
|---|---|---|
| CHAIN_DOCUMENT | protocol/validate | UTF-8, JSON o numeri non canonici |
| CHAIN_SCHEMA | invalid_plan/validate | Forma chiusa, campo o vincolo strutturale |
| CHAIN_VERSION | unsupported/validate | Coppia esatta non disponibile |
| CHAIN_DUPLICATE | invalid_plan/validate | Membro, step o descrittore duplicato |
| CHAIN_REFERENCE | invalid_plan/validate | Componente, step o porta inesistente |
| CHAIN_CYCLE | invalid_plan/validate | Ciclo di dati o controllo |
| CHAIN_BOUNDARY | unsupported/validate | Confine, mapping, claim o barriera incompatibile |
| CHAIN_CONSENT | authorization/validate | Consenso fuori scope o non coerente |
| CHAIN_BUDGET | resource_limit/validate | Restrizione step ampliata o retry oltre quota |
| CHAIN_LIMIT | resource_limit/validate | Limite documento, profondità o porte |
| CHAIN_RESULT | protocol/validate | Esito contraddittorio |
| CHAIN_EVENT | protocol/validate | Evento contraddittorio |
| CHAIN_CONFLICT | conflict/validate | Identità/digest differenti |
| CHAIN_UPSTREAM | originale | Errore pubblico della libreria preservato |
| CHAIN_CANCEL | cancelled/fase provata | Stop richiesto o confermato, effetto secondo prova |
| CHAIN_RECOVERY | execution/fase provata | Evidenza necessaria prima della ripresa |

Il validatore emette soltanto codice fisso e valid=false; i rifiuti prima invoke
hanno none/never. Le categorie geometriche restano quelle del contratto Arrow.

Eventi: event_id UUID, run_id, sequence positiva monotona per run, state_revision,
occurred_at UTC, kind, step_id/attempt, state, result_ref ed errore sicuro.
Kind: accepted, step_state, cancel_requested, cancel_confirmed, recovery_required,
result_persisted, delivery_confirmed. Solo step_state porta step_id; gli altri
attempt=0. Eventi risultato richiedono result_ref. Il consumer deduplica event_id,
usa sequence come cursor durevole, rileva gap; il replay conserva identità e contenuto.
Un singolo documento non prova ordine storico/CAS: è obbligo dello store P2.2.
Ricezione UTC T/t, Z/z o +00:00; niente -00:00, offset non zero o date impossibili;
emissione T/Z. Non si leggono orologi nei test della referenza.

## Cancellazione, idempotenza e ripresa

Invocazione distinta dal piano: run_id, plan_digest, binding immutabili,
correlation_id, chiave opzionale, deadline e contesto autenticato host.
Questa specifica definisce gli invarianti; schema trasporto/invocazione è P2.1,
non un'API eseguibile annunciata da P1.2.
Chiave scoped tenant+operazione: stesso digest/input riusa run, diverso produce
conflict/validate/none/never. Non inoltrare chiavi a componenti senza supporto.
run_id, step_id, attempt, message_id e result_message_id sono identità distinte.
Retry/redelivery conserva messaggio e negoziazione; nuovo tentativo autorizzato
ha nuova identità e causation. Duplicati riusano l'esito durevole.

Stati: pending→ready→admitted→running→finalizing→succeeded, alternative rejected,
failed, cancel_requested→cancelled, requires_recovery; blocked/skipped non avviati.
Transizioni con CAS/fence; lease scaduto non prova terminazione né abilita un writer.
PostgreSQL registra intenzione, input, consenso e quota prima invoke; esito,
manifest e outbox nella stessa transazione locale. Nessun avvio se store non conferma.

| Crash | Ripresa obbligatoria |
|---|---|
| Prima intenzione | Riammissione inbox/acceptance |
| Intenzione senza prova avvio | Solo prova/fence che esclude avvio consente ripetizione |
| Running senza esito | Recovery provider/artefatto; altrimenti unknown |
| Commit noto, journal incompleto | Ricostruire da ricevuta verificabile, non presumere rollback |
| Esito/outbox persistiti, risposta o ACK persi | Ripubblicare stesso risultato, mai dominio |
| Temporaneo non pubblicato | Cleanup con ownership/lease, mai output disponibile |

Resume richiede run_id, expected state_revision ed evidenza autenticata quando
necessaria; riusa succeeded solo con artefatti integri e autorizzati. Partial
richiede piano di recovery del componente, mai offset inventato; unknown non
consente riprova cieca. Tentativi e report storici immutabili. Tombstone coprono
delivery/retry/redrive/resend; scaduto è rifiutato, non nuovo. Rimozione solo con
epoche equivalenti, numeri operativi da fissare in P2.2.

Cancel è richiesta durevole idempotente: impedisce nuovi avvii, propaga stop,
trattiene handle/quota dei lavori sincroni fino a termine. Future cancellata non
prova stop thread/provider. Finestra scaduta senza prova resta recovery/unknown
quando pertinente; job REST residui nel report. Interrompere attesa client non
cancella run. Nessuna promessa exactly-once remoto, rollback globale, snapshot
comune, checkpoint metà operazione, hard RSS o equivalenza canvas legacy.

## Validazione e criterio di concordanza

Ordine: limiti/codec/duplicati → schema/versioni → registro esatto → unicità,
porte e DAG → confini/adapters/claim → budget/consenso. Nessun resolver/invoke.
L'implementazione di riferimento segue ordine deterministico dell'array per
il primo difetto all'interno della fase; il solo esito di un documento con più
difetti non è un oracolo per la precedenza di categorie upstream.
Schema dinamico richiede schema_barrier prima di ogni consumer dipendente;
validate indica deferred e non promette compatibilità dei valori futuri.
Prima di ogni invoke: verifica digest, schema effettivo, discovery e autorizzazione
immutate, prenotazioni/provider/sink. Modifica produce rifiuto/recovery.

[Validatore](../../scripts/validate_chain.py),
[test](../../scripts/test_chain_contract.py) e
[manifest fixture](../../contracts/chains-v2/fixtures/manifest.json).
Il registro fixture è esplicito, chiuso e offline; non è discovery di release.
I verdetti di fixture distinguono schema (forma), valid (ammissione di riferimento)
e code. `schema=null` significa rifiuto lessicale prima dello schema.
Un JSON Schema standard non esprime aciclicità, uguaglianza di digest o integrità
referenziale: schema=true/valid=false è il rifiuto semantico atteso, non divergenza.
La concordanza richiede schema=false ⇒ valid=false e nessuna fixture con esito
diverso dal proprio oracolo. I test controllano tutti i file, i campi ignoti
annidati, la determinazione del digest e i confini dichiarati.

```text
python -m pip install -r scripts/requirements-chains.txt
python -m unittest discover -s scripts -p 'test_*.py'
python scripts/validate_chain.py plan contracts/chains-v2/fixtures/plan-valid.json --context contracts/chains-v2/fixtures/context.json
```

Il risultato `reference_only` non certifica payload referenziati, conservazione
Arrow reale, report disponibili, autorizzazioni, provider o runtime. I gate sono
espliciti in [qualification](../qualification.md),
[contract-alignment](../contract-alignment.md) e [security-review](../security-review.md).

## Tempo osservato oltre deadline

La durata osservata puo superare il budget durante stop/finalizzazione. Il
contatore elapsed_ms conserva il valore esatto; l'overrun e ammesso soltanto
su esito non succeeded con errore timeout esplicito nello step/risultato
pertinente. Gli altri contatori restano limitati. Questa regola non autorizza
nuove invocazioni oltre deadline e non converte un commit noto in rollback.

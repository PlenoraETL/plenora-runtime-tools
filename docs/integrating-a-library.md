# Integrare una libreria Rust

Questa è la checklist di implementazione per collegare `data-tools`, `database-tools`, `IO-tools` o
una futura libreria Rust a un consumer Plenora. Le librerie possono evolvere in modo indipendente:
nessuna libreria concreta diventa una dipendenza di `runtime-tools`.

## Proprietà e forma

Il consumer finale possiede un piccolo adapter che implementa `CapabilityHandler`. Lo si registra
una sola volta all'avvio sotto un'identità versionata come `plenora.data-tools@v1`. Il registro è
poi congelato e condiviso da ogni task del worker ammesso dinamicamente.

```text
JetStream -> broker adapter -> bounded worker -> capability dispatcher -> app adapter -> library
                                      |                    |
                                      |                    +-- operation/input/error translation
                                      +-- timeout/cancellation/progress/heartbeat
```

Il modello completo, che compila, è
[`examples/capability-adapter`](../examples/capability-adapter/README.md). Non copiare in produzione
i suoi tipi di libreria simulati; sostituirli con la vera API pubblica quando quell'API è stabile.

## Decisioni richieste per ogni adapter

Prima dell'implementazione, congelare questi elementi per ogni operazione:

| Contratto | Decisione richiesta |
|---|---|
| Identità | Nome della capability con namespace e versione wire positiva. |
| Operazione | Identificatore completo con namespace, in minuscolo, come `io.read`, e versione positiva del contratto pubblico; mai dedurre l'instradamento dal contenuto del payload. |
| Input | Identificatore immutabile del contratto `plenora-...-vN`, content type, limite in byte e validazione eseguita prima dell'invocazione. |
| Output | Contratto di output immutabile, content type dichiarato, limite in byte e `CapabilityResultSink` confermato; il solo acknowledgement è valido soltanto per un contratto esplicito a output vuoto. |
| Cancellazione | Quali chiamate concrete sono cooperative e che cosa succede quando una future viene abbandonata (drop). |
| Avanzamento | Unità numeriche e totale facoltativo; niente percorsi, righe, credenziali o testo arbitrario. |
| Retry | Varianti d'errore concrete mappate esplicitamente su `PlenoraErrorRetry`; `CapabilityFailure::with_public_error` ricava la classe di retry del worker senza interpretare stringhe o stati. |
| Effetto | `PlenoraErrorRemoteEffect` esatto; usare `Unknown` quando la prova è impossibile. |
| Idempotenza | Chiave stabile, comportamento in caso di conflitto, confine della transazione e comportamento del risultato memorizzato. |
| Segreti | Quali campi non devono mai entrare in `Debug`, errori, eventi del ciclo di vita, etichette o motivi della DLQ. |

L'adapter restituisce `CapabilityResponse`; il dispatcher possiede i metadati canonici del risultato
e invoca il result sink configurato. Se una scrittura su database e la pubblicazione di un messaggio
devono essere atomiche, usare una outbox su database invece di pubblicare dalla transazione
dell'handler.

Per le operazioni che portano artefatti, attendere lo schema immutabile del payload posseduto dal
componente e decodificare esattamente quello schema. Non dedurre percorsi locali, campi segreti,
riferimenti ad artefatti o configurazione del provider scandendo JSON arbitrario.

## Ponte di cancellazione

Controllare `context.shutdown` o `context.cancellation` prima di invocare la libreria. Durante le
chiamate lunghe, inoltrare un clone del token del task quando la libreria lo supporta e mettere
anche la chiamata in gara con `context.cancelled()`. Il drop di una future Rust non prova che un
thread, un sottoprocesso, uno statement di database o una richiesta remota si siano fermati. Se la
cancellazione può entrare in gara con un effetto esterno, classificare il risultato come
`OutcomeUnknown`; non dichiarare `NotStarted`.

## Test di contratto minimi dell'adapter

Ogni adapter reale deve aggiungere test deterministici per:

1. ogni versione di operazione, contratto di input e content type supportati;
2. operazione sconosciuta, input malformato e rifiuto per dimensione eccessiva prima
   dell'invocazione della libreria;
3. ogni variante d'errore concreta e la sua mappatura di retry/effetto;
4. cancellazione prima dell'avvio e durante una chiamata di lunga durata;
5. monotonia dell'avanzamento e assenza di payload o segreti nelle osservazioni;
6. pubblicazione o persistenza del risultato, compresi gli esiti ambigui;
7. consegna duplicata e comportamento di idempotenza dove l'operazione ha effetti collaterali;
8. `Debug`, `Display` e catene di source senza fughe di credenziali o payload;
9. drain ordinato con una chiamata attiva e terminazione forzata dopo il limite configurato;
10. un test di accettazione a livello di consumer attraverso l'adapter della libreria reale, non
    solo chiamate dirette.

Usare `FakeBroker`, `ManualClock` e il dispatcher limitato del ciclo di vita per i casi
deterministici. Tenere i test contro servizi reali opt-in e limitati nel tempo reale. Il soak del
consumer finale deve esercitare insieme tutti e tre gli adapter reali prima che l'API pubblica
venga congelata.

## Aggiungere una quarta libreria

Aggiungere un nuovo adapter e una nuova registrazione posseduti dall'applicazione. Non modificare il
formato dei messaggi oltre a una nuova identità di capability versionata, e non aggiungere la
dipendenza concreta ai crate core, messaging, worker, NATS, Apalis, HTTP, observability o outbox.
Capacità del worker, heartbeat, retry, dead-letter, health e comportamento di arresto esistenti
restano condivisi.

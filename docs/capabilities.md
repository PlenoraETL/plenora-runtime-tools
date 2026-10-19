# Integrazione generica delle capability

`plenora-runtime-capabilities` è il punto di collegamento stabile fra la meccanica generica del
runtime e le librerie Rust di proprietà dell'applicazione. I crate del runtime non importano
`data-tools`, `database-tools`, `IO-tools` né futuri crate di fondazione.

## Modello di registrazione

Il consumer ospite costruisce un registro limitato durante l'avvio e poi lo congela. Ogni identità
contiene un namespace ASCII minuscolo e una versione di wire positiva, per esempio
`plenora.data-tools@v1`. Anche ogni operazione è l'identificatore pubblico completo con namespace,
come `data.run`; gli alias locali come `run` sono rifiutati. Un solo handler può instradare più
operazioni validate per quella libreria.

```rust,ignore
let discovery = CapabilityDiscovery::from_json(component.capabilities_v2()?)?;
let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(16)?)?;
builder.register_discovered(discovery, RestToolsAdapter::new(rest_tools))?;

let dispatcher = CapabilityDispatcher::with_result_sink(
    builder.build(),
    CapabilityDispatcherConfig::new(1024 * 1024)?,
    confirmed_result_producer,
)?;
```

`CapabilityDispatcher` implementa `WorkerHandler<CapabilityRequest>`, quindi lo stesso valore si
innesta nell'executor dei worker neutrale rispetto all'engine o nel runner del broker di Apalis. La
ricerca nel registro non avvia task e non modifica il runtime. Le future dinamiche dei worker sono
comunque ammesse solo fino al limite `max_in_flight` del worker.

## Proprietà degli adapter

Le librerie concrete non implementano il trait del runtime e non dipendono da runtime-tools. Il
consumer possiede il wrapper di traduzione:

Per un esempio completo, compilato e provato insieme al workspace, usare
[`examples/capability-adapter`](../examples/capability-adapter/README.md). Il frammento abbreviato
che segue illustra solo la proprietà:

```rust,ignore
struct DataToolsAdapter {
    tools: DataTools,
}

#[async_trait]
impl CapabilityHandler for DataToolsAdapter {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        // Validate operation/content type, decode into data-tools types, forward cancellation and
        // progress, call the library, then classify any concrete error explicitly.
        Ok(CapabilityResponse::new(output_contract, serialized_output))
    }
}
```

L'handler restituisce un risultato pubblico serializzato o un fallimento classificato. Il
dispatcher valida il risultato rispetto ai metadati di discovery congelati, aggiunge i metadati
canonici di Runtime Binding e lo pubblica tramite un `CapabilityResultSink` neutrale rispetto al
broker. Un sink mancante, un output incompatibile, una pubblicazione fallita o un esito di
pubblicazione ambiguo non possono diventare un acknowledgement riuscito del worker.

Aggiungere una quarta libreria significa aggiungere un altro wrapper e una registrazione all'avvio.
Il codice di NATS, Apalis, HTTP, esecuzione dei worker, heartbeat, retry, DLQ e shutdown non cambia.

## Contratto di wire e limiti

`CapabilityMessageCodec` memorizza l'identità della richiesta di Runtime Binding 1.0 in cinque
chiavi di metadati portabili riservate:

- `plenora.capability.name`;
- `plenora.capability.version`;
- `plenora.capability.operation`;
- `plenora.operation.version`;
- `plenora.input.contract`.

`CapabilityRequest` espone la versione dell'operazione tramite `OperationVersion` e
lo schema di input immutabile tramite `ContractId`. Il codec rimuove le cinque chiavi riservate
prima di passare l'input opaco a un adapter e conserva tutti gli altri metadati, compresi
`plenora.trace.correlation_id` e i controlli di esecuzione supportati. I byte del payload e il
content type restano neutrali rispetto al trasporto. Il dispatcher rifiuta
i payload sopra il limite configurato prima della ricerca o dell'invocazione. Il registro ha un
valore predefinito di 64 handler, un massimo rigido di 4.096, rifiuta i duplicati e non espone
alcuna modifica dopo `build()`.

La registrazione tramite discovery è obbligatoria per `plenora.rest-tools@v1`. Il suo profilo
richiede `rest.test`, `rest.generate`, `rest.enrich`, `rest.download` e `rest.upload`, tutte sulla
superficie del runtime. Le richieste sono rifiutate prima dell'invocazione quando stato/versione
dell'operazione, contratto di input, involucro JSON o controlli dichiarati non corrispondono. Le
scadenze UTC assolute cancellano il token di invocazione; un timeout dopo che l'invocazione può
essere cominciata resta un esito remoto sconosciuto. Le scadenze sono confrontate con un `Clock`
iniettabile che per default è l'orologio del sistema operativo, così il comportamento ai bordi è
riproducibile senza dipendere dal tempo realmente trascorso.

Le prove dei vettori di richiesta e la revisione immutabile dei contratti sono documentate in
[`contract-alignment.md`](contract-alignment.md). Il trasporto confermato dei risultati di successo
è implementato. Gli errori pubblici tipizzati impongono categoria, fase, effetto remoto, retry e
dettagli limitati; la pubblicazione dell'errore terminale dopo l'esaurimento dei retry resta legata
al settlement del worker ed è ancora un gate.

Le capability sconosciute e l'input sovradimensionato sono classificazioni dead-letter sicure con
effetto remoto `NotStarted`. I fallimenti degli adapter conservano la loro sorgente concreta ma
richiedono una classe di retry esplicita e la certezza dell'effetto. `OutcomeUnknown` mantiene
quindi il comportamento di retry fail-closed esistente.

Runtime-tools tratta il JSON del payload come proprietà del componente. Aggiungerà l'estrazione di
riferimenti ad artefatti e a segreti solo dagli schemi di payload REST rilasciati; non scandirà il
JSON in cerca di nomi di campo indovinati. Così il runtime resta un orchestratore a scatola chiusa
invece di un client HTTP.

## Test

`FakeCapabilityHandler` sta in `plenora-runtime-testkit`. Registra solo osservazioni senza payload
su instradamento, identità, tentativo e conteggio dei byte. La cronologia delle invocazioni e gli
esiti scriptati FIFO sono entrambi limitati. I consumer possono provare nuovi adapter e
l'instradamento senza NATS né alcuna libreria concreta non ancora completa. La checklist di rilascio
per ogni adapter è in [`integrating-a-library.md`](integrating-a-library.md).

## Adozione del binding 2.0 dal lato host

La discovery accetta le coppie `plenora-runtime-binding-v1`/`1` e
`plenora-runtime-binding-v2`/`2`; `runtime_capability()` restituisce wire `1`
in entrambi i casi. Un contratto e una versione incoerenti sono rifiutati.

`NegotiationContext::from_headers` legge esattamente i due valori esterni
`plenora.negotiation.requires` e `plenora.negotiation.accepts_downgrade`.
Non inserirli nel payload o nei metadati riservati del messaggio runtime.
Il contesto passa ad `accept_runtime_request` con l'identita del messaggio,
la provenienza esterna/interna, il minimo di ingresso e la capacita dello
hop di portarlo al successivo. `AcceptanceStore::accept` deve essere una
lettura autorevole con creazione atomica e durevole: un errore o una scrittura
non confermata rifiuta la richiesta. Dopo la finestra dichiarata dal deploy,
uno store conserva tombstone per impedire che un'identita scaduta torni nuova.

`select_runtime_destination` decide dal documento capability della destinazione
reale: la 2.0 verso la 1.0 richiede downgrade esplicito, una discovery
illeggibile rifiuta. L'API non autorizza da sola una specifica operazione:
restano obbligatori i controlli discovered su operazione, versione, superficie,
contratto, content type e controlli.

Queste API non sono ancora collegate a NATS/Apalis: nessuno store durevole,
minimo 2.0 o finestra di retention e configurato nei trasporti esistenti.
Le deviazioni RT-026/027 sono nel [registro](contract-alignment.md).
I test con store in memoria provano la policy, non la durabilita.

`CapabilityOperation::surface_side_effect` restituisce l'effetto effettivo
della superficie (SB-001), oppure `None` se non annunciata. Gli override
`plenora.surface_side_effects` devono essere strettamente maggiori dell'effetto
dell'operazione e nominare solo superfici effettivamente annunciate.

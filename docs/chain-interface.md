# Interfaccia delle catene nel profilo scriptato

Una sola implementazione Rust governa ammissione, esecuzione, recovery, pagine
eventi e proiezione degli errori. CLI, wheel sync/async e trasporto remoto
presentano lo stesso protocollo chiuso. Il profilo usa doppi dichiarati delle
librerie; non attesta decoder Arrow, transazioni database o provider reali.

## Errori

Response conserva `value` e il discriminatore `error`; `failure` e la proiezione
strutturata comune: code, kind (domain/boundary/coordination), category, phase,
remote_effect, effect_scope, retry, run_id e domain_code. Gli assi originali del
dominio e del boundary non vengono ricostruiti dal solo codice. `effect_scope`
distingue la run conosciuta dalla richiesta senza ricevuta di run. Il risultato
completo e i rapporti restano in value; non entrano in Display/Debug o str/repr.
Le ricevute precedenti senza failure sono leggibili: la proiezione si deriva
dai loro campi conservati. Una proiezione presente ma contraddittoria e rifiutata.

| Codici di coordinamento | Significato |
|---|---|
| invalid, schema, unsupported | Ammissione, schema o funzione incompatibile |
| conflict | Identita, input immutabile o revisione discordante |
| resource_limit | Riserva o limite insufficiente |
| timeout, storage | Deadline/attesa o conferma durevole non ottenuta |
| pending | Nessun terminale disponibile |
| recovery | Riconciliazione necessaria, mai replay implicito |
| failed, cancelled | Esito noto allegato; cancel soltanto con stop confermato |
| expired, not_found | Identita scaduta o assente nello scope autenticato |

`run` restituisce/solleva il fallimento con il risultato originale. Se manca
un terminale dopo una ricevuta confermata, conserva run_id, revisione, stato e
observed_result (ultima vista classificata, non una prova di stop). Un tentativo
attivo rende l'effetto unknown; senza nuova lettura si conserva la ricevuta
confermata e si dichiara l'incertezza. Non si sostituisce l'identita canonica con
quella candidata di un replay idempotente. `result` legge anche risultati falliti
senza trasformare il successo della lettura in successo del dominio.

Python espone ChainError per call, costruzione, migrate e connessione; `code`
e stabile, `result` conserva il documento e le altre proprieta copiano failure.
`AsyncClient.create` inizializza su thread con GIL rilasciato dal binding Rust.
Il costruttore non-async differisce il lavoro al primo await. Interrompere
l'attesa non chiama cancel e non prova arresto del provider.

## Recovery e pagine

`resume_controlled` Rust, Request.resume.recovery, `--recovery` CLI e keyword SDK
accettano observe, continue, reconcile. Observe non invoca e non muta; continue
rifiuta intenzioni attive/ignote; reconcile usa solo prove del provider fidato.
La revisione resta obbligatoria ed esatta. Il default compatibile e reconcile;
il metodo Rust resume precedente delega alla stessa riconciliazione. Nessuna
scelta abilita continue-on-error o rinnova budget, deadline o autorizzazione.

EventCursor Rust e condiviso dagli iteratori SDK e dalla CLI JSON-lines. Ogni
pagina e verificata interamente prima di consumarla; pagina vuota termina la
lettura corrente, non la run. ReportCursor legge rapporti troppo grandi per
una singola risposta; non tronca l'evidenza. Sintassi completa nel
[README CLI](../crates/plenora-runtime-chain-cli/README.md).

## Matrice eseguibile e limiti della prova

Il corpus surfaces.json contiene 18 righe RT, 11 controlli e tutte le 37
operazioni dell'unione cataloghi fissati/inventario, incluse quelle condizionali
e le versioni legacy: 330 combinazioni obbligatorie con cinque superfici.
Il generatore usa nomi/versioni esatti, contratti del catalogo quando presenti
e contratti esplicitamente fixture per le voci solo inventariate; identita fixture-* esplicitamente sintetiche senza
porte di dominio: prova selezione ed esecuzione del wrapper, non la semantica
di una transazione o del formato reale. Ogni operazione viene eseguita in un
piano di uno step, con stop, rapporto e contatore di invocazione verificati.

test_surface_matrix degli script confronta le fonti e il prodotto cartesiano;
la rimozione di qualunque cella o caso fa fallire il gate. Il test pubblico Rust
esegue i casi con primary e Transport dichiarati fake. Le prove della wheel e
del binario installati usano PostgreSQL; il job NATS esegue anche Client.remote,
AsyncClient.remote e CLI remota contro host reali. I/O bloccante di test resta
fuori dai runtime async. Senza infrastruttura le prove sono saltate, mai verdi
per sostituzione con un backend in memoria.

Le righe RT della matrice esercitano le regressioni pertinenti delle facciate
e collegano le garanzie strutturali ai gate di architettura. Non sostituiscono
il corpus completo raw-ingress/settlement/crash: il criterio 6, inclusa tutta
la crash matrix C e i controlli remoti end-to-end, resta il passo successivo.

# Control plane del runtime

`plenora-runtime-control` aggrega handle limitati e privi di payload già posseduti da
un'applicazione. Non scopre processi o librerie dinamicamente. Un builder di avvio registra un
numero finito di worker, scheduler, monitor di memoria e supervisori di sottoprocessi con nome, poi
congela il registro.

L'API generica supporta:

- scoperta dei componenti;
- ammissione/capacità del worker e snapshot limitati dei task attivi;
- pausa/ripresa reversibile del worker, drain terminale e cancellazione cooperativa per ID locale
  del task o ID canonico del messaggio;
- snapshot del cursore dello scheduler, pausa/ripresa e attivazione manuale limitata;
- stato di pressione sulla memoria del processo;
- capacità e contatori del ciclo di vita dei sottoprocessi.

Non vengono esposti payload dei task, valori di argomenti/ambiente dei sottoprocessi, testo degli
errori degli handler, credenziali o risultati di comandi. `WorkerTaskId` è locale alla durata di un
singolo executor, quindi un sistema esterno deve abbinarlo all'identità worker/istanza registrata.
La cancellazione resta cooperativa; l'endpoint non è una primitiva per uccidere thread.

`plenora-runtime-control-http` converte quei contratti in route Axum componibili sotto
`/runtime/control`. La costruzione richiede sempre un `ControlRequestAuthorizer` posseduto
dall'applicazione. Questo riceve gli header della richiesta più un'azione/componente/target
validati e deve fallire in modo chiuso senza registrare nei log gli header grezzi. L'adapter parte
in sola lettura; le route di pausa, ripresa, drain, cancellazione e attivazione manuale
restituiscono 404 finché `enable_mutations()` non viene chiamato esplicitamente.

L'attivazione manuale accetta un oggetto JSON limitato che contiene `triggered_at_unix_ms`. Quel
timestamp è l'identità deterministica dell'occorrenza: un chiamante che ritenta dopo un risultato
HTTP incerto deve riusare lo stesso valore, e il dispatcher a valle deve usare l'identità
dell'occorrenza come chiave di idempotenza. L'adapter non la sostituisce mai silenziosamente con un
nuovo timestamp del server.

Plenora dovrebbe montare il router solo su un listener interno o su una route amministrativa
protetta, applicare nell'authorizer la sua vera policy di identità/ruoli, mantenere i limiti HTTP di
frequenza/concorrenza delle richieste e registrare in audit le modifiche accettate fuori dalla
callback sincrona di autorizzazione. Runtime-tools non implementa utenti, ruoli, token, persistenza
o autorizzazione di business.

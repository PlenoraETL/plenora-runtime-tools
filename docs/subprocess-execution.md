# Esecuzione generica in sottoprocesso

L'isolamento in sottoprocesso è una modalità di contenimento per operazioni selezionate, pesanti o
basate su codice nativo. Quando un processo figlio esce o viene terminato, il sistema operativo ne
recupera lo spazio di indirizzamento, comprese le arene dell'allocatore e la memoria trattenuta
dalle librerie native.

È utile quando un'operazione usa una libreria nativa che trattiene pool globali di processo, può
mandare in crash il processo, non può cooperare con la cancellazione o richiede un tetto di memoria
rigido per singola operazione. Non è il default: avvio del processo, serializzazione, IPC, recupero
dagli effetti duplicati e packaging del deployment aggiungono costi. Gli handler Rust async ordinari
dovrebbero restare nel processo quando la loro memoria raggiunge un plateau stabile e rispettano la
cancellazione.

`plenora-runtime-subprocess` implementa il livello di contenimento neutrale rispetto al protocollo.
Una `SubprocessSpec` seleziona un eseguibile, un vettore di argomenti, un ambiente e una directory
di lavoro espliciti; non invoca mai una shell e per impostazione predefinita azzera l'ambiente
ereditato. Numero di argomenti, byte degli argomenti, voci d'ambiente e byte d'ambiente sono
validati prima dello spawn. `Debug` riporta dimensioni e conteggi invece dei valori.

`SubprocessSupervisor` applica concorrenza massima esplicita, scadenza in tempo reale, scadenze per
la terminazione ordinata e per il kill forzato, conservazione limitata di stdout/stderr e reaping. Le
pipe vengono svuotate anche dopo che il prefisso conservato è pieno, così un figlio prolisso non si
blocca su una pipe del sistema operativo piena. La cancellazione distingue una richiesta che vince
mentre è in coda da una che termina un figlio in esecuzione. Unix può isolare un gruppo di processi;
Windows crea un nuovo gruppo di processi e usa il percorso di terminazione dell'albero della
piattaforma. Ogni segnale all'albero della piattaforma è a sua volta limitato dalla scadenza del
kill forzato, e un segnale forzato all'albero che non può essere consegnato imposta
`SubprocessReport::tree_termination_degraded` e incrementa `tree_termination_failures` invece di
essere scartato, così un contenimento degradato è osservabile per ogni invocazione e in aggregato.
Un flag falso registra solo che non è stato osservato alcun fallimento di consegna; non prova mai
che nessun discendente sia sopravvissuto. Terminazione, reaping e rilascio dei lettori delle pipe
avvengono su ogni percorso di fallimento successivo allo spawn, e un errore il cui reaping diretto o
la cui terminazione dell'albero non ha potuto completarsi riporta `containment_failed`. Abbandonare
la future di esecuzione rilascia entrambi i lettori delle pipe e uccide forzatamente l'albero isolato
dal percorso di drop; poiché `Drop` non può fare await, ogni attesa che quel segnale esegue è
limitata da un piccolo budget fisso invece che dal timeout di kill forzato configurato, anche se la
creazione dell'helper di piattaforma resta una chiamata sincrona al sistema operativo a cui il crate
non può imporre una scadenza senza una primitiva nativa che evita deliberatamente. Anche un segnale
che non riesce a consegnare incrementa `tree_termination_failures`.

I segnali di gruppo vengono consegnati solo finché il leader non è ancora stato raccolto (reaped),
perché un identificatore di gruppo di processi può essere riciclato una volta raccolto il suo leader
e il crate si rifiuta di inviare un segnale distruttivo che potrebbe raggiungere un gruppo
estraneo. La conseguenza è esplicita: un discendente che ignora la terminazione ordinata e
sopravvive a un leader che esce da sé, o entro la finestra di terminazione ordinata, non viene
ucciso forzatamente. Il contenimento è una proprietà dei percorsi di terminazione e di abbandono,
non di un figlio che termina normalmente lasciandosi dietro discendenti in background.

Linux supporta inoltre un tetto di RSS per figlio, verificato a intervalli. Il supervisore espone
al control plane solo la capacità e contatori cumulativi del ciclo di vita.

Il crate deliberatamente non definisce un protocollo wire di comando/risultato, un modello di
idempotenza, un nome di libreria, un percorso dell'eseguibile o un codec di payload. Ogni adapter di
capability posseduto dall'applicazione decide se chiamare Rust nel processo o serializzare un
comando limitato verso un figlio supervisionato. Le API delle tre librerie reali e sonde di memoria
rappresentative sono ancora necessarie prima di fare quella scelta. Un'uscita o un kill recupera
solo la memoria del processo; non prova che un effetto remoto su database, file o rete sia o non
sia avvenuto.

# Scheduler generico

`plenora-runtime-scheduler` è un motore di dispatch programmato, limitato e neutrale rispetto al
backend. Non importa NATS, Apalis, un driver di database né una libreria applicativa.

I piani predefiniti coprono l'esecuzione singola, a intervallo fisso e cron con risoluzione al
secondo. `CronPlan` richiede un fuso orario IANA esplicito e usa il database dei fusi orari incluso
per i buchi e le sovrapposizioni dell'ora legale. A un'ora locale autunnale ambigua emette una sola
volta il primo istante corrispondente, poi avanza all'occorrenza locale successiva; un'ora
primaverile inesistente viene saltata. `JitteredPlan` sceglie un ritardo limitato e deterministico
da un seme stabile e lo rimuove prima di calcolare l'occorrenza successiva, così il jitter non
accumula mai deriva nella cadenza. `SchedulePlan` resta pubblico per regole di calendario
specifiche dell'applicazione. Le definizioni possono essere registrate all'avvio oppure aggiunte e
rimosse a runtime sotto la stessa capacità esplicita del registro. I payload programmati devono
contenere identificatori e piccoli comandi, mai dataframe o blob di grandi dimensioni.

Ogni dispatch porta un'identità deterministica composta dall'id della pianificazione e dall'istante
logico di scadenza. Il `ScheduleDispatcher` di proprietà dell'applicazione deve usare questa
identità come chiave di idempotenza. Il lock del registro viene rilasciato per tutta la durata della
chiamata, così un dispatch lento non blocca mai gli snapshot o le modifiche alle pianificazioni, e
un dispatcher può richiamare lo scheduler. L'occorrenza viene marcata come bisognosa di
riconciliazione prima di rilasciare il lock: un tick concorrente la salta, e un avvio manuale, una
pausa, una ripresa e una riconciliazione vengono tutti rifiutati per quella finestra invece di
attenderla. La riconciliazione in particolare viene rifiutata perché un tentativo in corso non si
può cancellare, quindi risolverlo in anticipo potrebbe avviare un secondo effetto esterno per la
stessa occorrenza. Abbandonare il tick chiude il tentativo e restituisce il controllo al
proprietario, così un dispatch interrotto non blocca mai la riconciliazione in modo permanente.
L'insieme dei tentativi non chiusi è tenuto accanto al registro anziché dentro la pianificazione,
così rimuovere una definizione non fa dimenticare che una delle sue occorrenze è ancora in
esecuzione: l'identità non può essere registrata di nuovo finché quel tentativo non si chiude.
Quell'insieme è limitato dallo stesso numero configurato di pianificazioni del registro, e un tick
che lo supererebbe riporta invece saturazione.

Un limite è intrinseco, e una sua conseguenza non lo è. Un dispatch non si può cancellare, quindi
abbandonare il tick che lo ha emesso lascia un effetto il cui destino può stabilirlo solo
l'applicazione; questa incertezza è inevitabile. Ciò che ne segue è una scelta, e la scelta fatta
qui è che un'occorrenza non risolta blocca la sua identità: una pianificazione in attesa di
riconciliazione non può essere rimossa, quindi il suo record non può essere scartato e l'identità
non può essere registrata di nuovo per fare il dispatch della stessa occorrenza una seconda volta.
Prima si riconcilia. Un tentativo che lo scheduler sta ancora guidando si chiude entro il suo
timeout di dispatch, e uno abbandonato si chiude subito, quindi il blocco è sempre rilasciabile. Il
dispatch ha una scadenza e una certezza esplicita dell'effetto:

- `NotStarted` mantiene la stessa occorrenza in scadenza per un tick successivo;
- `OutcomeUnknown` blocca la pianificazione in `ReconciliationRequired` finché il proprietario non
  conferma l'effetto o conferma che il retry è sicuro;
- la conferma fa avanzare il cursore.

Le policy per le esecuzioni mancate sono `Skip`, `FireOnce` e `CatchUp` limitato. I dispatch globali
per tick, il recupero per singola pianificazione, il numero di pianificazioni, la cadenza di polling
e il tempo di dispatch sono tutti finiti. Non viene creata alcuna coda di occorrenze in memoria: il
lavoro arretrato resta rappresentato da un cursore per pianificazione. Una decisione `FireOnce` di
abbandonare l'arretrato rimanente viene ricordata fra un tick e l'altro, così un arretrato più
grande del limite di recupero per pianificazione viene smaltito saltando in modo limitato, invece di
fare il dispatch di una seconda occorrenza arretrata in un tick successivo.

Il dispatch automatico può essere messo in pausa e ripreso senza cambiare `next_due_at`; `Paused`
fa parte dello snapshot persistente. Un avvio manuale è ammesso per definizioni attive, in pausa o
completate, non fa mai avanzare il cursore ricorrente, e usa un semaforo che non attende, limitato
da `max_dispatches_per_tick`. La saturazione viene restituita subito. Un timeout o un effetto
esterno sconosciuto viene restituito al chiamante per una riconciliazione di proprietà
dell'applicazione e non viene mai ritentato implicitamente. L'ammissione manuale è decisa in modo
atomico insieme alla prenotazione dello slot limitato, ma è decisa una volta sola: un'occorrenza
automatica che entra in riconciliazione dopo non ritira un dispatch manuale già ammesso, e
abbandonare la future manuale ne perde l'esito, che il chiamante deve allora trattare come effetto
sconosciuto. L'identità di un'occorrenza manuale è l'id della pianificazione accoppiato con
l'istante fornito e non è resa unica per chiamata, quindi ripetere un istante, o riusare un istante
di scadenza automatico, riusa di proposito l'identità, e un dispatcher idempotente non produce un
secondo effetto. I chiamanti che hanno bisogno di effetti distinti, o che riconciliano per
identità, devono fornire istanti distinti.

Lo scheduler espone snapshot che contengono stato, `next_due_at` e `skip_backlog_through`.
Plenora deve rendere persistenti le definizioni delle pianificazioni e tutti e tre i campi del
cursore tramite `database-tools`, e deve scriverli in modo atomico: rendere persistente il cursore
senza il confine di soppressione permette a un riavvio dentro un arretrato `FireOnce` abbandonato
di fare il dispatch di una seconda occorrenza arretrata. I record scritti prima che il confine
esistesse migrano impostandolo a `None`, il che ripristina il comportamento precedente di
rivalutare l'arretrato dal cursore salvato. Il confine è rispettato solo con `FireOnce`, e un
confine senza cursore viene rifiutato al ripristino. Per il resto riceve esattamente la stessa
fiducia di `next_due_at`: il ripristino controlla che i campi del cursore siano coerenti fra loro
ma non mette mai in dubbio i loro valori confrontandoli con l'orologio di sistema, così un orologio
che torna indietro non può far rieseguire allo scheduler un intervallo già abbandonato.
`restore_from` conserva il prossimo istante di scadenza, il confine di soppressione e un esito
sconosciuto non risolto, impedendo che un riavvio trasformi la riconciliazione in un retry alla
cieca. Finché quell'adapter non esiste, lo scheduler in memoria è funzionante ma la durabilità al
riavvio non è dichiarata.

La composizione applicativa deve eseguire lo scheduler come task di runtime supervisionato
critico. Lo shutdown ferma i nuovi tick e, dentro un tick già in corso, non avvia altre occorrenze
una volta osservato il segnale, quindi il drain è limitato dal dispatch già cominciato e dal suo
timeout configurato. L'ammissione è un controllo prima di ogni dispatch, non un handshake atomico:
un'occorrenza il cui dispatch è partito nello stesso istante in cui è arrivato il segnale arriva
comunque fino a quel timeout.

# Outbox, inbox e idempotenza

La prima milestone fornisce trait neutrali rispetto alla persistenza e store finti deterministici.
Una scrittura di dominio e la sua aggiunta all'outbox devono alla fine condividere un'unica
transazione; allo stesso modo, elaborazione e registrazione nell'inbox devono essere atomiche prima
dell'acknowledgement.

OutboxRelay rivendica un batch limitato e pubblica attraverso MessageProducer. Una pubblicazione
confermata viene marcata come pubblicata. Un errore del producer viene registrato secondo la
politica e restituito con la sua sorgente. OutcomeUnknown è trattenuto per la riconciliazione per
impostazione predefinita; ritentarlo richiede un opt-in esplicito, perché l'effetto remoto potrebbe
essere già avvenuto. Un'esecuzione che termina in anticipo rilascia ogni rivendicazione che non ha
mai tentato come `NotAttempted`/Retry, così un fallimento a metà batch non può parcheggiare le voci
successive finché non scade un lease specifico dello store; il rilascio stesso è best effort, una
voce il cui rilascio fallisce a sua volta è contata in `unreleased_claims`, e un'esecuzione
annullata facendo il drop della sua future non rilascia nulla. Quelle voci restano responsabilità
del lease di rivendicazione o del ripristino dopo crash dello store.

Il primo trait dello store di proposito non prescrive un modello di lease. Gli adapter concreti
devono definire la scadenza delle rivendicazioni e il ripristino dopo crash. InboxDeduplicator è
un aiuto di comodo, non un confine di atomicità: il controllo dell'inbox, l'effetto di dominio e il
record di avvenuta elaborazione devono condividere la transazione durevole del consumer.

L'inizio dell'idempotenza è atomico e distingue gli stati esegui, in corso, conflitto e risultato
memorizzato. La decisione portabile iniziale non porta i byte del risultato memorizzato.

La persistenza concreta su database è rimandata finché plenora-database-tools non sarà abbastanza
stabile per un contratto di adapter.

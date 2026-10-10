# Modello degli errori

Gli errori del runtime devono conservare, quando noti:

- categoria;
- fase di esecuzione;
- certezza dell'effetto remoto;
- disposizione al retry;
- errore sorgente.

La rappresentazione condivisa concreta resta un ADR aperto. Le implementazioni dovrebbero evitare
scelte pubbliche irreversibili finché il vocabolario d'errore più ampio di Plenora non si è
stabilizzato.

Il nucleo del worker oggi fornisce questi concetti attraverso WorkerExecutionError, senza esporre
un tipo del motore. Il relay dell'outbox usa un RelayError generico che conserva le sorgenti dello
store e del producer, un resoconto parziale del batch e l'operazione fallita. L'output di debug
redige le sorgenti concrete.

Gli adapter delle capability restituiscono `CapabilityFailure` con un `RetryErrorClass` e un
`CapabilityRemoteEffect` espliciti. `CapabilityDispatchError` distingue l'instradamento
sconosciuto, il rifiuto del payload prima dell'invocazione e il fallimento di un adapter invocato.
Le sorgenti concrete sono conservate per le catene d'errore ma escluse da `Display` e `Debug`.

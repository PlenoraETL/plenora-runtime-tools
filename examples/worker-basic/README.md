# Esempio di worker di base

Questo esempio usa solo contratti di worker di proprietà di Plenora. Mostra:

- un `WorkerHandler` tipizzato;
- una configurazione a concorrenza limitata;
- una policy di retry esponenziale iniettata;
- il passaggio non bloccante degli eventi di ciclo di vita tramite un dispatcher in memoria
  limitato;
- l'ispezione sicura rispetto alla redazione di un tentativo fallito;
- il drain coordinato di worker e runtime.

Eseguirlo dalla radice del workspace:

```text
cargo run -p plenora-example-worker-basic
```

L'esempio provoca di proposito un fallimento ritentabile, ma ne stampa solo la categoria stabile e
la decisione di retry. Non stampa mai il payload del messaggio né la sorgente dell'handler.
Il ricevitore del ciclo di vita viene svuotato esplicitamente nello stesso flusso async: il
dispatcher non possiede alcun task in background e riporta i contatori di eventi accettati,
consegnati e scartati per saturazione. L'esempio proietta anche lo stato finale del dispatcher
nella health del runtime usando la policy di criticità opzionale.

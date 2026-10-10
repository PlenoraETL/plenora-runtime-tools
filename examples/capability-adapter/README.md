# Adapter generico per una libreria Rust

Questo esempio verificato in compilazione è il modello per collegare `data-tools`, `database-tools`,
`IO-tools` o una futura libreria Rust senza aggiungere quella dipendenza a un crate del runtime.

La `ExampleLibrary` simulata fa le veci della libreria concreta. L'`ExampleLibraryAdapter` di
proprietà dell'applicazione:

- espone un `CapabilityId` versionato, una versione di operazione positiva, un contratto di input
  immutabile, e valida l'operazione e il content type;
- passa l'input opaco all'API concreta solo dopo la validazione;
- collega la cancellazione del task e riporta un avanzamento numerico limitato;
- traduce i fallimenti concreti in una classe di retry esplicita e in una certezza sull'effetto
  remoto;
- viene registrato una sola volta all'avvio in un registro immutabile e limitato.

Sostituire `ExampleLibrary` e `LibraryError` solo quando l'API della libreria reale è stabile.
Tenere l'adapter nel consumer Plenora finale, non in `runtime-tools` e non nella libreria concreta.

Eseguirlo dalla radice del workspace:

```text
cargo run -p plenora-example-capability-adapter
cargo test -p plenora-example-capability-adapter
```

I test sono un contratto iniziale dell'adapter: il successo raggiunge la libreria, le operazioni
non valide no, i fallimenti transitori restano ritentabili e gli effetti esterni incerti non
vengono mai ritentati alla cieca.
La checklist di integrazione in [`docs/integrating-a-library.md`](../../docs/integrating-a-library.md)
definisce i test aggiuntivi richiesti per un adapter reale.

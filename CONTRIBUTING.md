# Contribuire

La specifica di implementazione e i documenti sotto `docs/` sono normativi.

Prima di proporre una modifica:

1. preservare la direzione delle dipendenze documentata in `docs/boundaries.md`;
2. tenere i tipi di business specifici di dominio fuori dai crate del runtime;
3. aggiungere test per ogni requisito implementato;
4. evitare `unwrap`, `expect`, `panic!`, code non limitate e creazione di task non limitata nei
   percorsi di runtime di produzione;
5. documentare le nuove dipendenze infrastrutturali e le decisioni architetturali rilevanti;
6. eseguire formattazione, check, Clippy, test, documentazione, audit e i gate della policy delle
   dipendenze.

Le API pubbliche non sono congelate. Preferire modifiche piccole e revisionabili che completino un
pacchetto di lavoro alla volta.


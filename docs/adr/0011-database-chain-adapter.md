# ADR 0011 — Database reale nel prodotto catene

Stato: implementazione S3. Decisione del titolare: read@1 e write@2 realmente
esposti da database 11.0.0; read@2 resta lacuna upstream, senza alias.

I crate core, engine e provider PostgreSQL provengono esclusivamente dal commit
`f4fdb9b5f09d8babe795f68e894949693aa9d0a1` del repository database, tag
`py-v11.0.0`, versione esatta 11.0.0. Le dipendenze ETL restano nel nuovo
adapter prodotto, mai nelle fondamenta generiche. Il provider PostgreSQL è
quello della libreria, non un secondo motore ETL basato su SQL copiato.

Il percorso diretto Provider/Engine permette ResourceBudget esplicito,
write_with_consent e WriteOutcome::settle; read_loss viene raccolto prima
della pubblicazione. Evita i limiti fissi e la perdita del rapporto sul ramo
sink.finish del RuntimeBinding upstream. Il clock catene è iniettabile; il
budget temporale interno upstream continua a usare Instant.

Arrow 60.1.0 è la versione del provider. L'IPC attraversa un confine esplicito,
senza cast di tipi fra versioni. Il profilo iniziale materializza sotto limiti
finiti e rifiuta i profili che non può contenere. CLI e wheel del prodotto
devono usare il medesimo host Rust, non subprocess delle CLI upstream.

Discovery e selezione delle versioni provengono dalla funzione del crate
compilato; la disponibilità nel catalogo non autorizza altre operazioni.
La ratifica dei contratti catene e i gate Linux/Windows restano separati.

Il primo profilo e tabellare: nessun claim Primary Geometry finche mancano
prove semantiche; geometrie e relativa chiave schema sono rifiutate. I test
reali PostgreSQL e quelli delle superfici installate sono inseriti nei job
esistenti, senza trasformare la loro presenza in una qualifica gia acquisita.
TLS resta il default. Il solo resolver autenticato puo selezionare
allow_plaintext, necessario per la campagna CI effimera su loopback; nessun
parametro del piano puo indebolire questa scelta. Rischio operativo e condizione
di chiusura sono registrati in security-review.md.

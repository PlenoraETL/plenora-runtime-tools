# ADR 0012 - Adattatore IO reale

Stato: adottato per S3, profilo bounded sperimentale.

IO 9.0.0 **candidate, non release pubblicata**: dipendenze core/model/tools al
commit `9c087aa7daf5d9ad10843ebf3027ae8858f66b94` di
`https://github.com/PlenoraETL/plenora-IO-tools.git`. I tre crate sono necessari
rispettivamente per pubblicazione atomica, contratti/budget/cancellazione e
operazioni/discovery reali. Nessuna dipendenza ETL nei crate generici.
Arrow IPC/array/schema =60.1.0 e la stessa versione nativa; serde sullo schema
serve al digest canonico senza omettere metadata. tempfile =3.27.0 governa
staging e cleanup verificato. SHA-256 =0.10.9 e lo stesso algoritmo del journal.

Scelta: invocare `operazioni::read/write`, non il binding runtime upstream,
per fornire budget espliciti e ottenere realmente Arrow con read+destinazione
privata. La pubblicazione di IO write e privata fino ai controlli; il bundle
finale viene pubblicato dal core con no-clobber e durabilita esplicita.
Il risultato originale resta immutabile, distinto dall'effetto esterno.

Conseguenze: risoluzione host di sorgenti snapshot e destinazioni esclusive,
nessun percorso arbitrario nel piano, niente alias delle versioni contrattuali,
profilo Arrow bounded e lossless-only. Il catalogo driver reale determina
availability, non certifica i round-trip. Perdite/troncamenti impediscono publish,
ma i rapporti rimangono nel journal. La normalizzazione IPC primary=none della
candidate e intercettata con confronto schema su entrambi i lati.

Il worker nativo e drenato anche dopo cancel, deadline o lease persa. Mancano
un clock nativo iniettabile e un resolver durevole della pubblicazione esterna:
nessun retry cieco o presunzione di successo. Vedere contract-alignment,
security-review e qualification per ambito, rischio e condizioni di chiusura.

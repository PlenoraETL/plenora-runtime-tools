# Adapter IO reale (candidate)

Pin: `PlenoraETL/plenora-IO-tools`, commit
`9c087aa7daf5d9ad10843ebf3027ae8858f66b94`, **9.0.0 candidate non pubblicata**.
`operazioni::capabilities()` espone realmente `io.read@2` e `io.write@2`.
Nessun alias write@1 e nessuna qualifica tratta dal catalogo target.

L'host autentica il piano completo, snapshot immutabili delle sorgenti (inclusi
sidecar), documenti dei parametri e riferimenti di destinazione. Il riferimento
source e SHA-256 della serializzazione JSON di `Source`; `ref:chain-data` e il
selettore dell'artefatto Arrow verificato della porta write. I percorsi assoluti
sono solo configurazione trusted, mai parametri di una catena.

Il resolver usa discovery e catalogo driver della libreria compilata. La porta
read/output e write/input e Arrow stream 60.1.0 con contratto interchange v1,
Primary Geometry v1 e barriera schema. Nessun cast. Profilo bounded: 1 MiB per
artefatto/documento, 32 colonne, schema 16 KiB, 256 file, riserva memoria/spool
32 MiB per tentativo; pipeline nativa al massimo 8 MiB per memoria/spill.
IPC file, compresso o senza EOS non e qualificabile dal confronto schema di
questo profilo e viene rifiutato. Altri formati sono invocati tramite il driver
reale disponibile e ammessi solo con rapporti completi lossless.

Read materializza realmente Arrow in staging privato (la API senza destinazione
restituirebbe solo summary). Write riceve quell'Arrow e produce un bundle privato.
Solo dopo fidelity, loss report, schema IPC e limiti si pubblica atomicamente
un'intera directory esclusiva, con sidecar. Destinazione preesistente: errore,
nessuna sostituzione. Durabilita non confermata: requires_recovery/unknown.
Il publish_outcome upstream descrive lo staging; remote_effect della ricevuta
catena descrive la destinazione finale. Non vengono confusi o riscritti.

`reported` non concede perdite IO: il contratto catene attuale concede solo
perdite database.write@2 con grant. IO rifiuta le perdite prima della pubblicazione
esterna e conserva il risultato originale e i riferimenti fidelity/loss_report
nel journal, risultato e rapporto catena. Un rapporto assente o troncato resta
unavailable. Recovery senza evidenza confermata non reinvoca IO.

La candidate elimina primary_geometry=none con piu geometrie in IPC pur
riportando lossless. Il confronto esatto schema input/output rifiuta questo caso
sia in read sia in write; nessuna riparazione privata dei metadata. Il documento
upstream resta recuperabile ma non qualifica completezza della conversione.

La cancellazione raggiunge il token nativo; il lavoro spawn_blocking viene sempre
drenato prima di stop_confirmed. Il clock del coordinatore e iniettabile; la API
IO usa anche il proprio clock e non offre una prova di timeout nativo determinista.
Non esiste una garanzia di interruzione immediata durante fsync/rename.
Staging e destinazioni richiedono directory host riservate e non modificabili da
attori esterni durante l'esecuzione.

Profili pubblici: `IoHost::io`, `IoHost::with_io`, CLI `--io-config FILE` o
`io-call FILE`, wheel `Client(config, profile="io")` e
`await AsyncClient.create(config, profile="io")`. Stesso dispatcher/primario.
Le prove IO Rust usano file e libreria reali; ScriptStore simula solo il primario.
I test host PostgreSQL e wrapper installati sono nei job CI esistenti.

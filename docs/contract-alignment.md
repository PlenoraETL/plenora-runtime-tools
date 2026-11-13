# Allineamento ai contratti comuni

## Ambito catene e profilo generico

[ADR 0008](adr/0008-chain-product-scope.md) aggiunge il prodotto di orchestrazione
nel repository; i generici mantengono il perimetro e le deviazioni registrate qui.
Le righe RT e CAT sotto descrivono l’adozione esistente al pin, non la futura
capability del prodotto. In particolare RT-015 resta vero per i generici:
il consumer di dominio sarà nei nuovi crate prodotto, non dentro capabilities.
CAT-002/CAT-003 non attestano il catalogo catene ancora da ratificare.

P1.1 non cambia pin, vettori, manifesti di adozione o stato delle deviazioni RT.
Piano/esito/eventi v2, capability `plenora.runtime-tools`, operazioni chain.*@2,
archi DATA run@3/DB write@2 e conversioni sono proposte da ratificare in passi
successivi senza reinterpretare v1. Catene v2 non è Runtime Binding 2.0:
capability wire resta 1. Il target contratti 38d68b4 del progetto non è il pin
attualmente adottato. Catalogo STORAGE e Primary Geometry restano lavoro futuro.

Regola: ogni claim richiede contratto e prova sul profilo/trasporto effettivo.
Ambito: nuovo host, consumer legacy ed esempi separati. Rischio: estendere una
prova del wrapper alle superfici upstream o chiudere globalmente una deviazione.
Condizione di chiusura: rapporto integrato P2.x e successive prove su artefatti
reali, con scope esplicito. Vedere i [gate](qualification.md#gate-del-prodotto-catene)
e l’[inventario delle superfici](chain-features.md).


Pin adottato: `3aec9f724f5a0ad21366f293895b4d0dbcd0aad1` (precedente `90cffb2b78cace16edad352a19d86f674930c133`).

Il registro sorgente e [contracts/adoption-source.json](../contracts/adoption-source.json); non e un attestato di release v4. Runtime Binding 2.0 e Public Catalogs 2.0 sono adottati con gli ambiti e le deviazioni seguenti. Non si dichiara conformita end-to-end.

Il profilo resta `plenora-runtime-tools-profile-v1`; al pin non esiste alcun catalogo runtime-tools, ne vecchio ne nuovo. Public Catalogs 2.0 governa la selezione del consumer dai documenti capability. Sono copiate le nuove identita database-tools-v2, data-tools-v3, io-tools-v3, senza inventare operazioni runtime.

## P2.1b: confine delle facciate esistenti

Regola: RT-012/016..023, stesso ingresso canonico prima del dispatch registrato.
Ambito implementato: JSON raw, decoder worker, decoder wire NATS effettivamente
usato da receive/replay, BrokerDeliveryService Apalis e composizioni di processo.
[Prove eseguibili](../tests/integration/tests/boundary_facades.rs) confrontano
categoria/fase/retry/remote_effect, riflessione e identità, con zero invoke sui
rifiuti; includono quota trasporto con difetti multipli e metadati broker distinti.
Il nuovo result ID è diverso dalla richiesta e stabile nella singola istanza:
non è un attestato di persistenza o replay dopo restart.

Modifica deliberata dei generici: registro e dispatcher trasferiti da capabilities
al boundary, decodifica del codec privata, CapabilityRequest solo candidato
outbound. Il dispatcher non implementa più WorkerHandler sul candidato e il
lookup degli handler resta privato. Il trait CapabilityHandler è un'estensione
applicativa: una chiamata diretta al proprio adapter non è un ingresso del
registro runtime. I parser di documenti e i worker generici non producono il tipo
AdmittedRequest; nessun executor catene o adapter di dominio viene inventato.

Messaging conserva campi wire decodificabili prima dei limiti della mappa, oppure
un errore wire esplicito; il boundary applica lo stesso materializzatore. NATS
non elimina più il contesto del rifiuto per un valore decodificabile oltre limite.
WorkerContext separa metadata ammessi e broker_metadata; Apalis non li sovrascrive.
Il limite payload del trasporto segue protocol, unsupported e timeout.
Il profilo raw impone al media type decodificato lo stesso budget di 32768 byte
in ogni facciata; oltre budget il documento di metadati non viene riflesso.
Il budget raw metadata è 196608 byte; i limiti base64 derivano dal budget
decodificato, senza confrontare byte encoded con byte decoded. Sono limiti
locali di allocazione, non estensioni della grammatica dei contratti upstream. Nessun
nuovo client infrastrutturale, nessun arco generico verso il prodotto o engine.

Rischio residuo: confondere questa prova locale con un deploy NATS, con le policy
di schemi dei cinque adapter futuri o con un risultato terminale pubblicato.
Condizione di chiusura di questi ambiti: CI Linux/Windows sul commit, prove del
broker reale configurato e passi P2.2/P2.4/S3. CAS/digest durevole, restart e
settlement terminale rimangono aperti. Le righe globali sotto non vengono promosse
per effetto dei soli test del profilo locale. Le note P2.1a sui bypass legacy sono
storiche: P2.1b sostituisce quei percorsi pubblici, non qualifica gli esempi esclusi.

L'esempio capability-adapter resta esplicitamente OUTSIDE-QUALIFIED-BOUNDARY e
chiama il proprio adapter senza registro. Ogni esempio dichiara il profilo nel
manifest, verificato da test; worker-nats usa il boundary e un producer confermato
per i successi. Gli errori terminali non sono pubblicati dal runner: gate P2.4.
L'inventario AST [boundary-api.txt](boundary-api.txt) enumera dichiarazioni, firme,
visibilità, costruttori, derive e macro; variazioni richiedono nuova raccolta e
prove, non basta aggiungere un nome a una lista. Test compile_fail impediscono
costruzione di AdmittedRequest e dispatch pubblico del candidato.

## Stato per requisito del profilo complessivo

| Regola | Stato | Ambito ed evidenza |
|---|---|---|
| RT-001 | implemented | API Rust discovered: Identita runtime dal documento discovery. |
| RT-002 | not_applicable | runtime: Sostituito da RT-025 per la 2.0; wire resta 1. |
| RT-003 | implemented | API Rust discovered: Selettori completi e versioni operazione preservati. |
| RT-004 | implemented | API Rust discovered: Registro discovered controlla operazione, disponibilita e superficie runtime. |
| RT-005 | implemented | API Rust discovered: Content type verificato prima di invocare. |
| RT-006 | implemented | API Rust discovered: Controlli non annunciati rifiutati; REST ora richiede idempotency_key come catalogo. |
| RT-007 | deviation | runtime: Il runtime propaga deadline/cancellazione ma non verifica deduplicazione del componente. |
| RT-008 | implemented | API Rust discovered: CapabilityResponse e sink confermato; nessun successo senza output per le operazioni discovered. |
| RT-009 | implemented | API Rust discovered: Contratto, content type e correlazione preservati nel risultato. |
| RT-010 | deviation | runtime: Modello pubblico e mappature presenti; settlement non pubblica ogni errore terminale. |
| RT-011 | implemented | API Rust discovered: Routing incompatibile rifiutato prima di chiamare handler. |
| RT-012 | deviation | runtime: P2.1b verifica le identita a ogni ingresso registrato; deployment e CI remota non attestati. |
| RT-013 | deviation | runtime: Payload di dominio opachi; nessuna risoluzione autorizzata comune di riferimenti/credenziali. |
| RT-014 | implemented | API Rust discovered: Payload artefatto opaco e metadati risultato preservati, senza reinterpretazione. |
| RT-015 | implemented | API Rust discovered: Resolver e adapter appartengono all applicazione; nessuna dipendenza di dominio. |
| RT-016 | deviation | runtime: P2.1b uniforma i rifiuti locali a validate/none/never; pubblicazione terminale ancora P2.4. |
| RT-017 | deviation | runtime: Parsing canonico unico provato nelle facciate esistenti; generici documentali non sono ingressi ammessi. |
| RT-018 | deviation | runtime: Ordine protocol/unsupported/timeout provato localmente sulle facciate; profilo di deploy non qualificato. |
| RT-019 | deviation | runtime: Serializzazione del rifiuto e riflessione selettiva non integrate nel settlement. |
| RT-020 | deviation | runtime: Identita nuova e causation dei risultati non qualificate end-to-end. |
| RT-021 | deviation | runtime: P2.1b usa il parser UTC unico prima del dispatch; deployment e superfici prodotto future restano da qualificare. |
| RT-022 | deviation | runtime: Grammatica e limite della chiave provati localmente; digest/CAS e conflitto durevole restano P2.2. |
| RT-023 | deviation | runtime: Policy di schema fixture provata prima di invoke; i cinque adapter di dominio reali restano da implementare. |
| RT-024 | deviation | runtime: OutputTooLarge e resource_limit e mai troncato; fase finalize ed effetto unknown conservativi, non quelli provati dal componente. |
| RT-025 | implemented | API Rust discovered: Coppie binding v1/1 e v2/2 ammesse; capability wire sempre 1. |
| RT-026 | deviation | runtime: Policy pubblica select_runtime_destination implementata; i percorsi legacy non la chiamano. |
| RT-027 | deviation | runtime: API AcceptanceStore e confronto contesto implementati; nessuno store durevole o header esterni NATS/Apalis installati. |
| CAT-001 | implemented | consumer discovered: Selezione dalle capability reali, mai dalle superfici target del catalogo. |
| CAT-002 | not_applicable | catalogo runtime-tools: Nessun catalogo runtime-tools pubblicato al pin; non si inventa un catalogo nuovo. |
| CAT-003 | not_applicable | catalogo runtime-tools: Nessuna operazione di dominio posseduta. Cataloghi nuovi database-v2/data-v3/io-v3 vendorizzati senza modificarli. |
| SB-001 | implemented | discovery: Valida superficie e incremento stretto; surface_side_effect restituisce effetto effettivo o None se assente. |
| ERR-015 | implemented | modello errore pubblico: Vettori storage cleanup locale/remoto conservano cleanup/committed e never/requires_recovery. Non prova settlement. |
| ERR-016 | implemented | modello errore pubblico: Vettori commit io/timeout/internal conservano categoria causale separata da unknown. Non prova settlement. |
| INTEROP-001..005 | not_applicable | payload Arrow: Runtime non interpreta Arrow Geometry; nessun claim plenora-interop-vectors-v1. |
| RA-001..010 | not_applicable | adapter REST/Arrow: Adapter di conversione posseduto dall applicazione; runtime trasporta byte opachi. |
| contracts#39 | implemented | gate locale: Diagnostica del gate solo testo fisso; eccezioni non stampate. Nessun valore di manifest o fixture in output. |
| contracts#40 | not_applicable | cache fuzz upstream: Correzione cache del harness Python dei contratti, non del libFuzzer Rust; gate eseguito dalla copia locale aggiornata. |
| ADOPTION-v4 | deviation | release: Registro sorgente locale, non manifesto v4 di artefatti rilasciati: nessun digest inventato per un checkout. |
| CAP-004 | implemented | discovery con versioni simultanee: Rientrata il 2026-10-20: operazioni indicizzate per coppia nome/versione, lookup pubblici e dispatch esplicitamente versionati; duplicati della stessa coppia rifiutati. |

## Deviazioni e rientro

### RT-007

Regola: RT-007. Ambito: runtime. Il runtime propaga deadline/cancellazione ma non verifica deduplicazione del componente. Hazard: Retry di mutazioni senza prova di deduplicazione. Rientro: Qualificare adapter e almeno-un-solo-avvio per identita senza chiave.

### RT-010

Regola: RT-010. Ambito: runtime. Modello pubblico e mappature presenti; settlement non pubblica ogni errore terminale. Hazard: Caller senza risultato terminale. Rientro: Collegare error sink dopo esaurimento retry con pubblicazione confermata.

### RT-012

Profilo boundary P2.1: grammatica e riflessione provate all'ingresso grezzo da
`rt012`/`rt019` in [boundary_facades.rs](../tests/integration/tests/boundary_facades.rs).
P2.1b estende la prova al wire NATS e ad Apalis; i decoder UUID generici non possono costruire AdmittedRequest.

Regola: RT-012. Ambito: runtime. P2.1b verifica le identita a ogni ingresso registrato; deployment e CI remota non attestati. Hazard: Identita non canoniche possono attraversare un adapter. Rientro: Eseguire tutti i probe sulle vere frontiere codec/trasporto.

### RT-013

Regola: RT-013. Ambito: runtime. Payload di dominio opachi; nessuna risoluzione autorizzata comune di riferimenti/credenziali. Hazard: Percorsi o segreti presenti in input di adapter non qualificati. Rientro: Qualificare adapter con schemi di proprieta del componente.

### RT-016

Profilo boundary P2.1: rifiuti validate/none/never provati dai 21 probe upstream
e da `rt016`/`rt018`; il wrapper rimappa anche il timeout pre-invoke del dispatcher.
La pubblicazione dei rifiuti da parte del runner resta il gate RT-019.

Regola: RT-016. Ambito: runtime. P2.1b uniforma i rifiuti locali a validate/none/never; pubblicazione terminale ancora P2.4. Hazard: Semantica pubblica del rifiuto diversa da RT-016. Rientro: Uniformare codec, dispatcher e settlement e provare tutti i probe.

### RT-017

Profilo boundary P2.1: `rt017` prova JSON stringa, duplicati e versioni fino a
8192 cifre prima di `u16`; overflow canonico è unsupported. Il decoder NATS
rifiuta valori riservati multipli prima della mappa: prova
`duplicate_reserved_wire_values_are_rejected_before_map_construction` in
[metadata.rs](../adapters/plenora-runtime-nats/src/metadata.rs).
P2.1b rimuove il decoder pubblico grezzo: il codec generico conserva solo encode. Nessuna chiusura di deployment.

Regola: RT-017. Ambito: runtime. Parsing canonico unico provato nelle facciate esistenti; generici documentali non sono ingressi ammessi. Hazard: Normalizzazione di valori come 01. Rientro: Validazione lessicale esatta prima della conversione in tutto il workspace.

### RT-018

Profilo boundary P2.1: `rt018` esegue le otto combinazioni
protocol/unsupported/timeout su raw JSON e decoder worker; protocol precede
anche un selettore numerico canonico oltre u16. P2.1b prova anche worker, wire NATS e delivery Apalis; gli esempi esclusi dichiarano il proprio profilo.

Regola: RT-018. Ambito: runtime. Ordine protocol/unsupported/timeout provato localmente sulle facciate; profilo di deploy non qualificato. Hazard: Categoria dipende dal percorso adapter. Rientro: Applicare un unico ordine e provare difetti multipli.

### RT-019

Profilo boundary P2.1: `rt019` e tutti i probe upstream provano media type,
contratto, riflessione byte per byte e assenza delle chiavi malformate.
JSON illeggibile non inventa correlation/operation/causation. Resta aperta la
pubblicazione terminale con settlement del runner (P2.4); serializzare non è
pubblicare. La deviazione complessiva non è chiusa.

Regola: RT-019. Ambito: runtime. Serializzazione del rifiuto e riflessione selettiva non integrate nel settlement. Hazard: Metadati mancanti o impropri nel risultato. Rientro: Costruire rifiuti al confine del trasporto con probe neri.

### RT-020

Profilo boundary P2.1: `rt020` e `real_dispatch_success_identity_and_queued_expiry`
provano nuova identità per errore/successo e causation=request.message.id;
serializzare due volte la stessa istanza conserva l'identità. Persistenza,
replay dopo restart e terminali da retry restano P2.2/P2.4: nessuna chiusura
end-to-end dalla sola prova locale.

Regola: RT-020. Ambito: runtime. Identita nuova e causation dei risultati non qualificate end-to-end. Hazard: Caller non puo correlare ogni rifiuto al messaggio. Rientro: Verificare successi e rifiuti sui trasporti reali.

### RT-021

Profilo boundary P2.1: `rt021` usa ManualClock; riceve T/t, Z/z e +00:00,
frazione 1–9 cifre, rifiuta -00:00, offset non zero e secondi intercalari.
Motivo dell'esclusione: SystemTime non ne conserva l'identità temporale.
La regressione `absolute_deadlines_are_evaluated_with_the_injected_clock`
prova uguaglianza e superamento anche nel dispatcher generico. P2.1b rende privato il dispatch grezzo: ogni richiesta registrata passa dal
parser UTC comune prima del calcolo della durata e del poll di invoke.
La deadline resta `DateTime<Utc>` fino al calcolo della durata, senza conversione
anticipata in SystemTime: `nanosecond_deadline_is_not_rounded_by_dispatcher`
prova il caso futuro di un nanosecondo anche su Windows. La prova
`deadline_reached_between_boundary_and_dispatcher_never_polls_handler` controlla
la scadenza fra le due letture dell'orologio.

Regola: RT-021. Ambito: runtime. P2.1b usa il parser UTC unico prima del dispatch; deployment e superfici prodotto future restano da qualificare. Hazard: Deadline non UTC accettata. Rientro: Parser esatto UTC e prova ManualClock su <= now.

### RT-022

Profilo boundary P2.1: `rt022` prova vuota/null/limite specifico e assenza;
il probe upstream storage prova il controllo non supportato. La policy
dell'host è obbligatoria e non inventa dedup remota. Digest/CAS autorevole,
conflitto, race, restart e at-most-one-start restano aperti (P2.2).
La riga complessiva resta deviation.

Regola: RT-022. Ambito: runtime. Grammatica e limite della chiave provati localmente; digest/CAS e conflitto durevole restano P2.2. Hazard: Deduplicazione non garantita. Rientro: Qualificare grammatica, limiti e store deduplicazione del componente.

### RT-023

Profilo boundary P2.1: `rt023` prova un validatore di contratto fixture,
prima di invoke, con deadline uguali e diverse; nessuna ricerca di campi
arbitrari nel boundary. I cinque adapter reali devono implementare la policy
del proprio schema prima dei resolver: questa prova non li qualifica.

Regola: RT-023. Ambito: runtime. Policy di schema fixture provata prima di invoke; i cinque adapter di dominio reali restano da implementare. Hazard: Doppio canale deadline. Rientro: Adapter rifiuta campo payload e metadata insieme.

### RT-024

Regola: RT-024. Ambito: runtime. OutputTooLarge e resource_limit e mai troncato; fase finalize ed effetto unknown conservativi, non quelli provati dal componente. Hazard: Garanzia indebolita: perduta fase/certezza dopo pubblicazione; sink puo avere un limite inferiore. Rientro: Trasportare fase/effetto provati e tradurre tutti i limiti del sink; qualificare trasporto reale.

### RT-026

Regola: RT-026. Ambito: runtime. Policy pubblica select_runtime_destination implementata; i percorsi legacy non la chiamano. Hazard: Downgrade implicito possibile nel routing legacy. Rientro: Collegare policy a ogni destinazione scelta prima dell invio.

### RT-027

Regola: RT-027. Ambito: runtime. API AcceptanceStore e confronto contesto implementati; nessuno store durevole o header esterni NATS/Apalis installati. Hazard: Contesto perso su retry/redrive/resend; nessuna finestra durevole garantita. Rientro: Store atomico autorevole con tombstone e finestra dichiarata; cablare ogni hop e testare riavvio/race.

### ADOPTION-v4

Regola: ADOPTION-v4. Ambito: release. Registro sorgente locale, non manifesto v4 di artefatti rilasciati: nessun digest inventato per un checkout. Hazard: Nessuna evidenza immutabile di release o endpoint conforme. Rientro: Produrre e verificare artefatti con digest reali e manifesto v4, senza claim per contratti con deviazioni.

## Limiti operativi e prove

Wire e selettori restano versione 1. Policy host RT-026/027 testata sulle 21 sequenze comuni tramite API pubbliche e fake atomico; il fake non prova durabilita. Ingress minimo 2.0 e finestra acceptance **non configurati nei trasporti**. Nessuna finestra di retention viene promessa. Limite dispatcher: 1 MiB predefinito, massimo 64 MiB, configurabile per input/output; limiti ulteriori del sink non qualificati.

I 33 vettori runtime-v1 e i due runtime-v2 restano coperti dai test interni del codec/modello. P2.1b esegue i 21 probe runtime-probes-v1 attraverso le facciate locali; non attesta settlement, durabilita o deployment. ERR-015/016 sono prove di preservazione delle assi, non di pubblicazione terminale.

Decisioni 0015, 0017..0021 confrontate nella copia locale; 0016 non e presente al pin. Interoperabilita Arrow (#34) e adapter REST/Arrow non applicabili al trasporto opaco. #39 e #40 riguardano i gate upstream: non introducono wire o capability nuovi. Nessuna dipendenza Rust aggiunta. Nessun budget di dimensione del codice trovato nei gate o nelle policy locali.

Il pin e ogni copia sono verificati da `python scripts/check-contract-adoption.py`, opzionalmente con `--upstream C:/pg/contracts-main`. Il gate non scrive nella copia upstream e non usa rete. Per la validazione normativa completa: `python C:/pg/contracts-main/tools/validate_specs.py` con `PYTHONDONTWRITEBYTECODE=1`.

### CAP-004

Regola CAP-004; ambito discovery con versioni simultanee. Rientrata il 2026-10-20: operazioni indicizzate per coppia nome/versione, lookup pubblici e dispatch esplicitamente versionati; duplicati della stessa coppia rifiutati. Hazard precedente: un documento valido con piu versioni simultanee non era registrabile. Condizione di rientro soddisfatta dalle prove in `rest_discovery.rs` e `registry_dispatch.rs`: catalogo data-tools-v3 con data.run@2 e @3, superfici distinte, selezione dei contratti input/output per versione, nessun fallback e nessuna sovrascrittura. Il registro handler era gia indicizzato per CapabilityId (nome e versione wire); la correzione riguarda la mappa delle operazioni, la cui versione e indipendente dal wire. Il profilo REST seleziona esplicitamente v1. Le API operation e operation_named richiedono ora OperationVersion: cambiamento di firma intenzionale per eliminare lookup ambigui. Le prove sintetiche con entrambe le versioni su runtime non dichiarano quella superficie per data.run@2 nel catalogo reale. La chiusura locale non attesta una release o la CI Linux/Windows.


Public Security 1.0 e selezionato dal profilo: la protezione del payload opaco e dei riferimenti resta delegata agli adapter (deviazione RT-013); nessun claim di conformita SEC complessiva.

## Bozza catene P1.2: ratifica separata

Regola: nessuna reinterpretazione dei contratti v1 o claim di archi non ratificati.
Ambito: [CHAIN-PLAN-2.0](specs/CHAIN-PLAN-2.0.md), schemi e archi locali in
`contracts/chains-v2`. Stato: specificati e verificati con fixture, **bozze da
proporre**, non adozione centrale. Il profilo restringe a porte obbligatorie
singole e riferimenti a documenti di dominio immutabili, con contratto e digest;
non cambia gli schemi upstream e non presume validati quei documenti esterni.
Rischio: confondere validità della forma o del registro sintetico con disponibilità
e interoperabilità reali. Chiusura: ratifica centrale di piano/esito/eventi/errori,
catalogo e descrittori degli archi, distinta dalla ratifica DB write@2 e dei suoi
input/result v2 e WriteOutcome v3, poi pin adottato e prove P1.3/S3.

## P1.3: validatore Rust su fixture

Regola: ammissione esatta della bozza locale, nessuna reinterpretazione upstream.
Ambito: [chain-contracts](../crates/plenora-runtime-chain-contracts/README.md).
Tipi, parser chiuso, digest e semantica preventiva implementati; 88 verdetti
concordi con l'oracolo. Rust e referenza controllano anche claim del produttore
sugli output pubblici e disponibilità dei rapporti negli errori; regressioni
aggiuntive, nessuna modifica ai verdetti o schemi delle 88 fixture.
Non è una deviazione dal contratto: sono applicazioni delle regole Primary
Geometry e completezza dei rapporti a tutte le uscite e a tutti gli errori.
Rischio: confondere controllo statico con interoperabilità reale. Condizione
di chiusura: ratifica e adapter S3, host con prove su payload, discovery,
fidelity e autorizzazioni reali. Le deviazioni RT dei generici restano aperte.

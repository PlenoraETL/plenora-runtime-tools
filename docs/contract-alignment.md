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
Ambito implementato: JSON raw, decoder worker, carrier NATS `decode_wire`
seguito dal boundary, BrokerDeliveryService Apalis e composizioni di processo.
La parità del carrier isolato non attesta la precedenza end-to-end di receive/replay.
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
un errore wire esplicito; il boundary applica lo stesso materializzatore. Il carrier
`decode_wire` conserva il contesto dei valori decodificabili oltre limite.
WorkerContext separa metadata ammessi e broker_metadata; Apalis non li sovrascrive.
Nel carrier ammesso dal boundary il limite payload segue protocol, unsupported
e timeout. Il profilo raw impone al media type decodificato il budget di 32768 byte; oltre budget il documento di metadati non viene riflesso.
Il budget raw metadata è 196608 byte; i limiti base64 derivano dal budget
decodificato, senza confrontare byte encoded con byte decoded. Sono limiti
locali di allocazione, non estensioni della grammatica dei contratti upstream. Nessun
nuovo client infrastrutturale, nessun arco generico verso il prodotto o engine.

Deviazione esplicita di trasporto (RT-012/016..023): `JetStreamConsumer`, sia
operativo sia replay, deve rifiutare prima di esporre una `Delivery` anche a un
consumer generico senza boundary. `decode_delivery` usa lo stesso decoder wire,
ma rifiuta metadati illeggibili/non portabili, payload oltre quota e content type
oltre il limite portable di 128 byte con Protocol e richiesta TERM. Non produce
un risultato runtime con riflessione selettiva e può anticipare unsupported/timeout.
Rischio: la parità di precedenza/riflessione del carrier non si estende a questi
rifiuti del consumer reale. Condizione di chiusura: un ingresso NATS dedicato al
boundary con ammissione e settlement qualificati end-to-end, senza indebolire il
consumer generico. I test `consumer_transport_parity_*` verificano offline la
conversione condivisa, la sequenza rifiuto/valido e gli errori di terminazione;
non attestano conferma server o durabilità del TERM.

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


## P2.2: primitive durevoli PostgreSQL

Regola: RT-022 e RT-027, registro D.2; nessuna accettazione/autorità di avvio
senza commit confermato, nessuna identità scaduta riutilizzabile. Ambito:
[chain-store](../crates/plenora-runtime-chain-store/README.md), esclusivamente
primitive Rust. Implementati acceptance autorevole, dedup identità/chiave
scoped e passo, alias, fence, report/inbox/outbox atomici, relay fenced e
retention 7/90 giorni riducibile con tombstone permanenti.
Le prove di race/crash/restart PostgreSQL sono scritte e collegate alla CI,
ma non eseguite su questo host: non costituiscono chiusura di RT-022/027.
RT-010/020/024 ricevono solo supporto di persistenza, non finalizzazione P2.4.
Le 21 sequenze/header/forwarding RT-027 e selezione RT-026 restano P2.3.

Scostamento d'interfaccia: il trait OutboxStore generico non trasporta un
fence nella conferma; non viene implementato con una garanzia indebolita.
Si usa claim/confirm con autorità esplicita. InboxStore.record_processed
rifiuta nuovi record isolati: soltanto finish crea inbox, report e outbox
atomicamente. IdempotencyStore.begin non sostituisce il CAS per passo.
Motivo e contratti mancanti nel checkout sono nell'[ADR 0009](adr/0009-chain-postgres.md).
Rischio: usare queste primitive come prova di un host già cablato o di
settlement distribuita. Chiusura: prove PostgreSQL verdi sul candidato,
poi integrazione P2.3/P2.4 e conferma unica P2.5. Nessun claim globale dei
generici, delle cinque superfici o delle librerie ETL viene promosso.


## Profilo negoziato ed evidenze P2.3/P2.4

Questa sezione aggiorna i gate P2.3/P2.4 indicati nelle fotografie dei passi
precedenti; non promuove gli esempi generici o le superfici CLI/SDK dell'host.
`DurableRuntime` collega boundary P2.1, acceptance P2.2 e journal PostgreSQL.
`DurableWorker` e `SettlementRetry` cablano il runner Apalis; `NegotiatedForwarder`
cabla ogni send/retry/DLQ e il redrive passa da Forward. La route del risultato
viene congelata prima dell'invocazione e persistita: il relay non accetta un
producer arbitrario al momento del claim. L'host configura la stessa RouteTable
sul restart e deve associare ciascun nome allo stesso endpoint autorizzato.

| Riga | Implementazione e prove locali | Prove reali |
|---|---|---|
| RT-026 | 21 sequenze enumerate nel profilo prodotto minimo v2; discovery e acceptance controllate a ogni invio, senza downgrade | Header JetStream nei job CI |
| RT-027 | Contesto esatto persistito, Apalis, DLQ/redrive, relay su route immutabile; nessuna ricostruzione da default | Test NATS e test combinato PostgreSQL/NATS ignorati localmente |
| RT-010 | Ricevuta prima dell'ammissione, rifiuti durevoli, retry limitato per numero/tempo ed effetti safe, evidence salvata prima del ritorno wrapper; recupero della sola finalizzazione | Race e restart PostgreSQL in CI |
| RT-019 | Unico finalizzatore per errori d'ingresso e componente; riflessione selettiva, errori senza dati | Percorso reale combinato in CI |
| RT-020 | Identita risultato persistita, replay terminale prima della verifica deadline entro la finestra delivery, risposta/ACK persi senza ripetere dominio | Redelivery reale osservata con attempt >= 2 nel runner; relay reale in CI |
| RT-024 | Enumerazione componente/dispatcher/sink per read/commit/cleanup e none/committed/partial/unknown; rapporto completo separato dal terminale | Journal PostgreSQL in CI; nessuna qualifica di engine reale |

Il journal prenota il massimo serializzato prima di invocare: due copie JSON
dei byte (fino a quattro caratteri per byte ciascuna), metadati/errori riservati e
numero massimo di tentativi. Un writer contatore misura poi esattamente il JSON
prima dell'allocazione e del commit, entro 64 MiB. OutcomeBudget rifiuta un successo
fuori budget trasformandolo in un esito resource_limit completo con fase/effetto;
non tronca un rapporto gia accettato. Il superamento quota header conserva il
rapporto e usa il terminale d'errore riservato.

Ready/Running/Evidence/Finished sono stati durevoli con revisione CAS. Running
senza evidenza non autorizza mai un nuovo invoke; Evidence consente soltanto
Settle, anche dopo crash. Pending recupera la finalizzazione e pubblica con lease
fenced; una risposta broker incerta libera la lease per ripubblicare lo stesso ID.
Il replay finalizzato prevale sulla deadline originaria, ma non estende i 7 giorni.
I rapporti scadono entro 90 giorni anche senza lettura; i tombstone ID restano.
Schema PostgreSQL v2 esclude entrambe le autorita legacy/prodotto nei due ordini;
i vecchi binari v1 falliscono esplicitamente dopo migrazione.

Deviazione delimitata: chiavi di idempotenza d'esecuzione rifiutate con unsupported,
mai ignorate. Il contesto tenant/progetto autenticato richiesto dalla sezione D
arriva dall'host; P1.4/P1.5 introducono esecutore/API host e scope tenant/operazione.
P2.3/P2.4 non inventano tale identita da run/step o metadata del client. Rischio:
indisponibilita intenzionale della deduplicazione per chiave su questa facciata.
Chiusura: scope autenticato host e prove cross-tenant/cross-operazione nel passo
host; le primitive scoped P2.2 restano disponibili senza attestare tale integrazione.

Le prove sono in `tests/integration/tests/chain_delivery.rs` e
`crates/plenora-runtime-chain-store/src/delivery_tests.rs`. I doppi sono soltanto
nei test. La rilettura indipendente ha chiuso i rilievi locali. I servizi reali e
la CI Linux/Windows non sono eseguiti sull'host: restano gate di qualifica remota.

### Rettifica CI #22: acceptance asincrona e guasti recuperabili

RT-010/020/026/027: i percorsi asincroni usano admit_async e lookup su blocking
worker per la SPI AcceptanceStore sincrona. Bloccare il reactor durante
l'acceptance poteva impedire a una transazione concorrente di rilasciare il lock
PostgreSQL; il conseguente errore non deve diventare un terminale unsupported.
Ora Storage resta un errore recuperabile, con ricevuta Ready senza evidenza o
nuova autorita di dominio. Solo rifiuti di ammissione/negoziazione confermati
sono finalizzati. Il boundary ricontrolla deadline e policy dopo l'I/O.
Prove current-thread con handshake verificano avanzamento del reactor in tutti
i percorsi, guasto acceptance isolato e recovery con una sola invocazione.
La race reale mantiene count=2, verifica anche terminali, journal e replay; lo
stesso scenario e eseguito localmente con un doppio, senza attestare il backend.

Storage significa autorita non confermata: la SPI comune aggrega anche assenza
e scadenza oltre a errori transitori. Nessuna promessa di successo con retry;
la finestra durevole e i tombstone continuano a impedire nuove invocazioni.

## Incremento P1.4/P1.5 non qualificato

Regola: stessa semantica e stessi gate su Rust, CLI, SDK sync/async e runtime.
Ambito: coordinatore sequenziale e facciate sperimentali, con adapter fake
esplicitamente dichiarati. Sono presenti journal PostgreSQL, scope host
(tenant, chain.submit@2, chiave), CAS, evidenza originale conservata prima della
classificazione, cancel richiesto distinto da stop e recovery senza replay.
Le prove locali sono in crates/plenora-runtime-chains/tests/sequential.rs e
crates/plenora-runtime-chain-python/tests/test_installed.py. Il passo P1.4a descritto sotto collega ora host e step al runtime durevole;
non chiude la qualifica complessiva delle cinque superfici.

Deviazioni residue: adapter delle facciate ancora scriptati,
qualifica incompleta delle opzioni CLI e del modello errori,
classificazione degli errori delle facciate ancora sperimentale. Rischio:
scambiare la presenza dei metodi per parita qualificata. Chiusura: gate
[P1.4/P1.5](qualification.md#gate-p14p15-sperimentale) e ADR 0010.
Nessuna cella dell'inventario e promossa per queste prove incrementali.

Il validatore locale dell'esito ammette elapsed_ms osservato oltre budget solo
su esito non riuscito con timeout esplicito, per conservare il tempo di arresto
senza clamp. Regola, ambito e rischio: bozza locale, non ratifica upstream; non
autorizza nuovi effetti dopo deadline. Chiusura: ratifica e vettori centrali.

Gli ingressi aggiunti nell'inventario boundary-api sono classificati come:
parse_step_result = decoder chiuso senza I/O; ChainEngine/Store/Submission/Change =
coordinatore e SPI fidate con autorita non costruibile dai caller; ScriptedHost,
CLI scripted-call, NativeClient/NativeRemoteClient e NATS serve = harness
sperimentale di ADR 0010, non ingresso del prodotto qualificato. Le prove di
parita disponibili sono limitate al corpus elencato nel resoconto, e non
chiudono l'enumerazione RT richiesta per ogni superficie.

### Incremento replay e paginazione del candidato P1.4/P1.5

Il lookup autorevole tenant/run e tenant/chain.submit@2/key precede il controllo
della deadline di nuova accettazione; conserva il binding completo e la finestra
di delivery. `create` ripete il confronto atomico per la race lookup/inserimento.
La regressione `expired_deadline_replays_binding_but_never_accepts_new_work`
prova replay scaduto, conflitto, isolamento e rifiuto dopo retention con fake.
Il test PostgreSQL corrispondente resta una prova ignorata da eseguire in CI.

Il loop comune `drive` del listener attende tutti i task gia accettati dopo un
fallimento di pubblicazione; il test sincronizza un task fratello ancora attivo.
Non qualifica abort/drop esterno del server, JetStream o settlement del prodotto.

`EventCursor` valida atomicamente pagine e continuita di sequenza in Rust; CLI
JSON-lines e iteratori Python lo riusano. Il codec Request JSON ammette i controlli
Python senza coercizioni numeriche PyO3. Le nuove forme CLI mappano soltanto le
Request esistenti. Le celle di inventario restano senza promozione: integrazione
retry/worker, risorse, modello completo degli errori,
RT end-to-end e crash matrix completa restano condizioni di chiusura del gate.

### P1.4a: unico percorso durevole di host e step

Regola: ogni invocazione usa ammissione, wrapper e settlement P2.3/P2.4.
Ambito: ScriptedHost possiede soltanto DurableRuntime; ChainEngine possiede un
StepExecutor concreto privato, senza adapter invocabile o executor sostituibile.
Solo i componenti EvidenceComponent registrati possono invocare adapter e
controlli; prepare/artifact restano la vista limitata delle risorse. Guardie AST
e compile-fail impediscono il ritorno al percorso diretto del candidato.

RT-022 usa lookup/CAS primario e binding distinto dall'identita della ricevuta:
(tenant fidato, capability, operazione, versione, chiave). Il replay conserva
contratto/output e ricollega soltanto identita/correlazione/causazione; non concede
un nuovo tentativo. Il tenant non viene dal messaggio. Submit e run con la stessa
chiave sono operazioni distinte. La migrazione aggiunge il registro delle chiavi;
retention 7/90 giorni, puntatore eliminabile dopo retention e tombstone permanenti.

PrimaryResultSink conferma la disponibilita del terminale immutabile sul primario
per lettura locale: non certifica un ack JetStream. La risposta NATS osserva quel
risultato gia persistito; non costituisce un secondo settlement. Il carrier remoto
conserva identita, metadati e negoziazione del client. L'assenza del primario causa
Storage anche nelle funzioni di lettura, senza fallback fuori dal dispatcher.

Prove locali: chains/tests/sequential.rs, chain-host/tests/support/host.rs e
architecture/tests/chain_dispatch.rs nei rispettivi crate/test workspace. Le prove
PostgreSQL/NATS di chain-host/tests/remote.rs sono ignorate localmente e cablate
nel job postgres-integration esistente, su database chain_surfaces.
Rischio residuo: adapter scriptati e sink locale non qualificano librerie reali,
trasporto P2.5 o parita completa RT/catalogo. Un rifiuto durevole dello step dopo
la registrazione dell'intento puo restare Recovery esplicito, senza consultare
il fake o ripetere il dominio. Chiusura: passi successivi, stati completi e CI
Linux/Windows; nessuna promozione delle celle di prodotto in questo passo.

### P1.4b: retry dello step e worker del journal

Regola: retry solo con effetto none/rolled_back e safe/after di ogni errore;
resource_limit, committed/partial/unknown e controlli provider non provati non
concedono tentativi. Ambito: coordinatore unico, tutte le facciate esistenti lo
riusano. Il runtime di ciascun tentativo conserva attempts=1. Max tentativi,
consumo locale/globale e deadline originale limitano il coordinatore; backoff e
retry-after usano il massimo e un due persistito. Nessun reset al resume.

StepResult pubblico espone contatori cumulativi, inclusi fallimenti e attese;
la SPI fidata Adapter restituisce contatori del singolo tentativo (max_attempts=1),
aggregati soltanto dal coordinatore. Il report conserva queste ricevute grezze;
non sono dichiarate risultati cumulativi gia validati. Nuovi tentativi hanno ID distinti e causation persistita
verso il predecessore. I succeeded e i loro manifest restano identici. Resume
selettivo avanza il solo step fallito eleggibile; un terminale esaurito o con retry
vietato non viene riaperto. Stop al primo fallimento definitivo resta invariato.

Il worker host scansiona il primario anche dopo restart, con lease finito e fence
monotono per run. Ogni CAS del worker verifica ownership e scadenza. Solo la
richiesta monotona di cancel puo attraversare un lease altrui, preservando tutto
il resto del journal. Un vecchio owner conserva l'esito nella ricevuta runtime,
ma non puo confermare una nuova revisione del run. Il takeover riconsegna soltanto
il carrier identico: Running non concede un secondo invoke. Unknown classificato
resta quiescente fino a recovery esplicito. Le failure del worker sono esposte al
supervisore; il server interrompe intake e drena i task gia accettati.

Avvio: ScriptedHost.work(stop) per host in-process; serve avvia e supervisiona lo
stesso worker insieme al listener NATS. La durata di una richiesta non possiede
il lavoro persistito. Gli adapter restano fake: attempts nella configurazione
scriptata dichiara ricevute aggiuntive esatte, senza fabbricare esiti di retry.

Prove locali: retry_worker.rs, sequential.rs, test host autonomo e matrice degli
assi in retry.rs. Prova PostgreSQL/NATS worker.rs ignorata localmente e cablata
nel job postgres-integration esistente. Rischio residuo: queste prove non chiudono
risorse/provider, crash matrix completa o enumerazione di ogni operazione sulle
cinque superfici. Chiusura: criteri 3-6 e CI Linux/Windows sul candidato; nessuna
promozione di una cella completa del catalogo in questo passo.

## P1.4c - complete-plan confirmation (partial criterion 3)

The trusted scripted host now supplies authorized_plans: explicit complete plan
documents confirmed once in its tenant context. Missing confirmation denies new
submission. CHAIN-C14N-1 binds destination, inputs, parameters, policy, graph and
scoped loss grants; changing any of them requires a new confirmation. This is a
declared fake of the authorization provider, not authentication of a real user.
All facades share the same native host. Existing exact idempotent receipts remain
readable; new invocation and reconciliation recheck the current trusted profile.

The durable dispatcher repeats read-only preparation on worker restart. Denial
before invoke produces validate/none/never, not unknown: it cannot authorize a
scripted recovery result for work never invoked. A run whose intention already
exists can still require coordinator reconciliation after this known runtime
rejection; no synthetic StepResult is fabricated. Local regression covers both
authorization and resource-limit denial after the persisted intention.

Criterion 3 remains open: actual journal reservations, resource bytes/digest and
schema/fidelity checks, durable artifact leases, bounded fan-out and cleanup are
not qualified by this change. The existing real PostgreSQL/NATS test now includes
rejection of an unconfirmed plan, but was not executed locally. Closure requires
these missing resource guarantees and their tests; real library adoption remains
a separate qualification gate.

## P1.4c-1: serialized journal reservations and complete raw report recovery

Scope: the authoritative run snapshot, not database WAL/index overhead, process
RSS, artifact leases or real provider buffers. `ScriptedConfig.journal_bytes`
(default 64 MiB, finite and no greater than 64 MiB) and Rust
`with_journal_budget` apply to new submissions. The accepted quota is persisted
and cannot be increased or removed by CAS or a host restart. It is a separate
host storage budget; raw receipts also consume the plan/step result-byte budgets
using measured bytes, independently of adapter-supplied counters.

The serializer measures every JSON byte before allocating the output buffer.
An exhaustive Journal destructure accounts for every field and its JSON key,
separator, array expansion and optional-field presence. Each byte in a Vec<u8>
can require four JSON bytes. Fixed slots cover view, intention/prior and current
raw evidence (each at most 1 MiB before byte-array encoding); metadata bounds use
maximum numeric widths and all plan step identities. History slots are at most
1 MiB each, event slots 4 KiB each. Slot counts are bounded independently of the
serialized quota (1024 history, 16384 events); the byte quota can bind sooner.

Before every new invocation/inquiry, the same primary CAS reserves a history
slot and six event/revision positions for admission, raw receipt, classification,
recovery, concurrent cancellation and terminal rejection. No effect can spend
that reservation on another new invocation. Each stored state is checked against
its exact per-field allocation and total quota. The terminal view and current
raw evidence slots remain protected when history capacity is exhausted.

The effective raw-result limit also reserves the aggregate view: current view,
plan alias keys/IDs, measured maximum control envelopes and copies of the raw
result in its step, primary error and named outputs. The limit is fixed in the
new intention. Existing intents omit the optional field without changing their
carrier; an unreserved legacy intent cannot start a new receipt, only consume
proof of work already started. Legacy state remains readable within its old
hard ceiling; migration does not manufacture prior reservations.

A reconciliation has a separately reserved stable carrier. Uncertain delivery
reuses it. Confirmed null or a pre-invocation rejection releases it with a CAS,
allowing a later bounded inquiry. Unknown original effects are never reinvoked.
Reports are attributed to their coordinator finalizing events, not to untrusted
step_id claims inside a report. Compliant aggregate report responses reserve
512 KiB, leaving control-envelope space; no original evidence is truncated to
satisfy that convenience limit.

A defective adapter can violate its announced result limit. Its structurally
accepted raw receipt (within the runtime's 1 MiB input/output ceiling) remains
in the reserved journal slot and the run requires recovery. All surfaces can
read it with the same native byte-page implementation, including when the full
report response is too large. Existing report calls keep their full-response
shape. Optional cursor `{index: 0, offset: 0}` returns `ReportPage`: cursor,
bytes, report_bytes, total_reports and next. Chunks are at most 64 KiB; offsets
are bytes, and report indices remain stable as new evidence is appended.

CLI: `plenora-runtime chain report --run UUID --report-index 0 --offset 0
--config profile.json --json` (or the existing remote endpoint options).
Python sync/async: `client.report(run_id=run_id, cursor={"index": 0, "offset": 0})`,
awaited for the async client. Continue with `next`; concatenate bytes for each
report index. Rust exposes `ReportCursor`, `ReportPage` and `report_page`.
This extends report retrieval only; it does not close criteria 5-6.

Local evidence: exhaustive serializer/slot tests, exact quota and pre-effect
rejection, cumulative recovery exhaustion, cancel/crash regressions, legacy
carrier guard, confirmed-empty/denied inquiry renewal and complete recovery of
a declared defective adapter report over 400 KiB through the durable host.
Real PostgreSQL/NATS tests and installed wheel/bin byte-for-byte reconstruction
are wired in the existing infrastructure job and remain ignored locally.
Resource resolver/digest/schema/fidelity, artifact lease/fan-out/cleanup and
real-library qualification remain open.

## P1.4c-2: risoluzione e prove di schema/fedelta del profilo scriptato

Regola: prima di ogni step verificare riferimenti, digest, schema effettivo e
fedelta; nessuna perdita senza consenso specifico. Ambito: ScriptedHost e tutte
le sue facciate usano verification.rs nello stesso adapter durevole, anche
in recovery. La mappa risorse e il piano confermato sono immutabili per istanza;
al riavvio non si riusa una precedente approvazione dei byte. Autorizzazione
per tenant e digest dell'intero piano precede la lettura dei riferimenti.
Cataloghi, parametri, input, output, rapporti e upstream/error document sono
verificati contro SHA-256 dei byte; manifest confrontati anche per dimensione.
Un riferimento mancante/alterato ferma lo step prima di invoke. Le prove di
schema restano separate dai manifest originali, compreso schema_digest=null.

Deviazione dichiarata: il provider di dominio e un doppio JSON canonico
scripted-v1/scripted-arrow-v1, NON un decoder IPC o schema di libreria reale.
Il suo schema completo include ordine dei campi, ID, tipi, nullabilita e tutti
i metadata; il digest e SHA-256 del JSON canonico del doppio. Il dominio dei
tipi fake e int64/utf8/binary; righe, colonne e celle del doppio sono controllate.
Primary Geometry applica none, ID esplicito e inferenza solo con una geometria.
Il trasporto di piu geometrie senza primaria resta lecito se non richiesta.
Un producer dinamico puo incontrare uno schema consumer fisso soltanto con
barriera obbligatoria: gli altri attributi restano esatti. Rust e oracolo Python
condividono i vettori dynamic-static, inclusi i rifiuti.

La prova fake di fedelta e un documento integro legato a step, operazione/versione,
parametri, porte/riferimenti/digest input-output e destinazione. Exact non ammette
DatabaseLoss; reported lo ammette solo per database.write@2, consenso esplicito
con grant validato e loss_report riferito alla medesima prova. ForbiddenLoss
resta vietato anche con consenso DB. Il documento e un oracolo scriptato fidato,
non una deduzione della fedelta dal solo hash dei byte. I rapporti originali
restano disponibili senza riscrittura. Una verifica fallita in recovery non
trasforma l'effetto unknown in none.

Rischio/chiusura: questa qualifica non attesta schemi parametri reali, Arrow IPC,
CRS/WKB, algoritmi di trasformazione, WriteOutcome/loss_report DB v3 o resolver
di connessioni/segreti dei provider. Occorrono adapter e oracoli delle librerie
adottate. Il profilo non apre file/path/URL del piano. Lease/fan-out/cleanup e
criteri 5-6 restano fuori da questo passo; nessuna casella completa e promossa.


## P1.4c-3: ownership, lease e cleanup del provider dichiarato fake

Regola: ownership prima dell'effetto, scadenza distinta dalla prova di stop,
nessuna cancellazione di evidenze o tombstone. Ambito: il provider di byte
scriptato registra sullo store P2.2 i buffer realmente restituiti tramite
riferimenti opachi per tenant/run. I template immutabili della configurazione
sono risorse prese in prestito; non sono output posseduti e non vengono eliminati.
Gli input tra step leggono gli stessi byte pubblicati, con controllo SHA-256.
La lettura applica clock e retention del primario anche senza sweep.

Il ledger riserva l'encoding JSON peggiore, copie di staging e handle di fan-out
contro memory/spool globali; le copie fisiche consumano anche artifact_bytes.
Admission atomica, lease per identita immutabile di tentativo, rinnovo prima
della scadenza e riconciliazione senza nuova autorita di invoke. Il worker
rinnova il fence di lavoro mentre continua a far avanzare lo step. Il provider
fake termina immediatamente dopo registrazione/rinnovo: non si attesta un
provider reale di lunga durata o un heartbeat di libreria.

Stop noto e cleanup sono parte della CAS del journal: errore prima del commit
conserva la ricevuta durevole; risposta persa dopo commit non duplica l'effetto.
I temporanei si liberano dopo stop; output/evidenze restano fino alla retention
rapporti (90 giorni massimi). A scadenza i consumer mai avviati sono marcati
abbandonati. Unknown conserva i buffer in quarantena, anche con lease scaduto:
solo riconciliazione autenticata autorizza la pulizia. Lo sweep riavviabile del
worker e maintain lavora per chiavi in batch limitati e conserva tombstone,
manifest e ricevute dopo la liberazione dei byte. Delivery resta 7 giorni massimi;
le riduzioni della policy primaria valgono anche per risorse e letture.

Prove: resources.rs, retry_worker.rs, host/tests/support/ownership.rs;
resources_tests.rs nel filtro postgres_real e remote.rs nel job PostgreSQL/NATS
gia esistente. Le prove reali restano ignorate localmente. Rischio residuo:
scambiare i buffer del doppio per ownership di file, processi o librerie reali.
Condizione di chiusura dell'adozione: adapter reali con registrazione prima
dell'effetto, rinnovo durante il lavoro, prova di stop e cleanup idempotente
qualificati su tutte le superfici. Nessuna promozione dei criteri 5-6 o della
conformita complessiva; esiti dei gate locali in target/P1-4-5.md.

Esito P1.4c-3: verde locale nel profilo fake; con c-1 e c-2 chiude localmente
il criterio 3. Suite workspace 496 passati/20 ignorati, suite installata
7 passati/5 saltati. Infrastruttura reale e CI multipiattaforma non eseguite
localmente; i limiti di adozione sopra restano aperti.


## P1.5 criterio 5: protocollo e matrice delle facciate

Regola: una sola semantica su Rust pubblico, CLI, SDK sync/async e remoto.
Ambito: sintassi chiusa con recovery observe/continue/reconcile; Response.failure
calcolato da Rust preserva categoria/fase/effetto/retry del dominio e del boundary,
oltre a contesto e identita della run nota. Gli errori di coordinazione non
attestano assenza di effetti per una run attiva. SDK costruisce fuori dal loop,
rilascia il GIL e traduce uniformemente gli errori; cancellare l'attesa non cancella
il lavoro. Interfaccia e compatibilita in [chain-interface](chain-interface.md).

La matrice eseguibile enumera 37 operazioni (anche condizionali/legacy), 18 righe
RT e 11 controlli su cinque superfici; la sottrazione di ogni cella e testata.
Per le operazioni si usano piani a uno step e provider dichiarati fake, senza
porte di dominio; i contratti solo inventariati restano fixture esplicite.
Corretto il vincolo locale che rifiutava nomi catalogati database.transaction.*:
segmenti canonici multipli, mai vuoti, con vettori Rust/Python condivisi.
Non e un cambiamento ai contratti upstream fissati ne un'attestazione librerie.

Rischio: confondere enumerazione/selezione delle facciate con qualifica della
semantica ETL o con copertura raw-ingress/crash completa. Condizione di chiusura:
adozione provider reali separata e criterio 6 con intera matrice C e controlli
remoti end-to-end. Le nuove prove PostgreSQL/NATS della wheel e del binario sono
cablati nei job esistenti e restano ignorati localmente quando manca infrastruttura.
Esiti locali e stato corrente dei criteri in target/P1-4-5.md.


## P1.5 criterio 6: controlli installati e crash matrix C

Regola: ogni riga C.4 deve avere prove eseguibili, senza confondere assenza di
risultato con assenza di effetto. Ambito: esecutore/host con adapter dichiarati
fake, piu prove PostgreSQL/NATS dei sottosistemi durevoli e del trasporto.
La fonte enumerata e [CHAIN-CRASH-MATRIX-C](specs/CHAIN-CRASH-MATRIX-C.md);
[chain-crash-evidence.json](chain-crash-evidence.json) associa tutte le sette
righe a test locali e reali ignorati. Il gate verifica fonte, righe, test,
classificazione ignored/local e cablaggio CI; omissioni e prove mancanti sono
regressioni negative. Non si attesta un process-kill end-to-end per ogni riga.

I controlli positivi installati usano CLI nominata, Client e AsyncClient sia
sul primario reale sia tramite NATS. La prova remota mantiene lease primarie
rinnovate sui run accettati finche cancel e persistito; quindi le rilascia al
worker. Verifica observe inerte, revisioni obsolete rifiutate, resume fino a
cancelled, nessun tentativo/effetto, pagine eventi complete e duplicate control
senza nuovi eventi. La barriera di scheduling e dichiarata: non si prova stop
di un provider reale. Subprocess e costruttori sync sono fuori dai loop async.

Rischio residuo: attribuire questi esiti a librerie ETL/provider reali o alla
CI non eseguita. Condizione di chiusura infrastrutturale: job esistenti verdi
sullo stesso SHA con wheel/binario installati e container effimeri; adozione
librerie reali resta successiva. Il cablaggio e compilato localmente, i test
infrastrutturali restano ignorati dove mancano i servizi. I criteri 1-6 sono
riportati separatamente in target/P1-4-5.md, con gli esiti realmente osservati.


## PR23: fonte unica dello schema durevole

Ambito P2.2/P1.4: schema.rs associa ogni SQL alla sua revisione e ne fissa
l'ordine; migrate, vincolo SQL corrente, readiness e test usano quel catalogo.
La revisione globale corrente e 3; la versione 1 delle evidenze e un asse distinto.
Il conteggio rigido 12 del test era incompatibile con le 13 tabelle complete:
la qualifica ora confronta i nomi derivati dagli SQL, non un numero mantenuto
separatamente. DB vuoto, ogni prefisso v1/v2, idempotenza, preservazione dati,
riduzione retention e rifiuto versioni sconosciute sono enumerati. Il rifiuto
verifica anche rollback della creazione DDL, non soltanto della versione.
Rischio residuo: prove reali del delta ancora da eseguire nella CI aggregata;
nessuna estensione della qualifica a librerie o provider di dominio.


## PR23: selezione retention e diagnostica del primario

Il journal delivery conserva origine e flag di purge in colonne scalari indicizzate,
aggiornate nella stessa transazione del documento. La manutenzione seleziona al
massimo 32 candidati e verifica documento/metadati prima del purge; la lettura
puntuale applica comunque la retention, anche se il batch non ha raggiunto la riga.
La migrazione V3 recupera i metadati dai documenti esistenti atomicamente. Identita,
fence e tombstone non vengono cancellati. Il backfill resta soggetto al timeout
SQL: la qualifica su database popolati di dimensione operativa resta aperta.

PLENORA_STORAGE_DIAGNOSTICS=1 abilita anche nei binari/wheel la posizione del
fallimento, categoria chiusa, SQLSTATE e stato connessione; non vengono stampati
SQL, messaggi del driver, DSN o valori. Il timeout interno NATS conserva Timeout.
La scansione globale dei payload era un costo non limitato sotto il lock globale;
la sua responsabilita nei cinque fallimenti ci23c resta da confermare in CI.


## Budget di osservazione remoto

Rust, CLI e SDK sync/async inoltrano lo stesso parametro di osservazione al client
Rust: default 10s invariato, limite finito 1ms..1giorno, distinto dai budget e dal
consenso del piano. Il timeout non implica fallimento dello step, cancellazione
o retry. Il driver NATS non applica un secondo timer nascosto. Le prove di catena
usano un budget dichiarato compatibile con la durata autorizzata dalla fixture;
RT-010 continua ad attendere failed, effetto none, senza modificare il verdetto.
Qualifica del delta: nuova campagna reale e coverage 90% sullo SHA finale.


## PR23 ci23e: cancellazione e finalizzazione oltre deadline

La cancellazione prima del primo intento mantiene lo stato cancelled e zero
invocazioni anche quando la finalizzazione supera il budget: elapsed conserva il
valore effettivo e un timeout di finalizzazione esplicito soddisfa il contratto.
La classificazione di evidenza e contatori usa un solo istante dopo la lettura
primaria, anche nei retry CAS. Un commit noto rimane documentato nel report grezzo;
un'attesa oltre deadline non produce da sola recovery o replay.
Lo stop esplicito del listener drena il lavoro accettato senza inventare Storage;
EOF, perdita del controllo e fallimento di pubblicazione restano errori.
Il test NATS nel job PostgreSQL usa product_nats; un gate verifica che ogni output
provenga da uno step precedente dello stesso job. Prove con ManualClock e probe
PostgreSQL di restart/worker inclusi; qualifica infrastrutturale del delta da
confermare nella CI sullo SHA finale, senza attestazione di provider di dominio.


## Conferma integrata P2.5

Il workflow dispatch [boundary-confirmation.yml](../.github/workflows/boundary-confirmation.yml)
richiede lo SHA esatto del ref selezionato. Deriva i comandi dalla CI tramite
[generate_boundary_confirmation.py](../scripts/generate_boundary_confirmation.py):
Linux e Windows, anti-panic, PostgreSQL, NATS, binario/wheel installati e coverage
con soglia invariata del 90%. Tutti i passi indipendenti vengono tentati anche
in presenza di errori; il rapporto conserva gli outcome originali, incluso
skipped/cancelled. I job verdi da soli non sostituiscono le ricevute di esecuzione.

Il [registro D](specs/BOUNDARY-REGISTER-D.md) e il
[manifest delle evidenze](boundary-evidence.json) enumerano RT-010..027 e le
successive regole comuni dell'adoption register. Per ogni riga sono obbligatorie
Rust, CLI, SDK sync, SDK async e client remoto; quest'ultimo richiede sync,
async e CLI su NATS reale. Le ricevute vengono scritte dopo le asserzioni,
vincolate a SHA, run, attempt e digest del corpus. Sono richieste anche le prove
Rust pertinenti del confine, osservate nell'output libtest; ignored e zero test
non sono prove. Le campagne reali aggiungono crash/race, migrazioni, retention,
settlement e copertura, senza sostituire queste prove con semplici nomi di test.

Il rapporto unico e i dati completi sono nell'artefatto
`boundary-confirmation-<SHA>-<attempt>`. Un verde del gate significa raccolta ed
esecuzione complete delle prove dichiarate: **non chiude i requisiti D completi**.
Ogni riga del manifest resta open; una promozione a closed viene rifiutata fino
all'estensione esplicita del modello di evidenza con prove dell'intero requisito.
Nuove righe senza cinque celle fanno fallire l'inventario; la fotografia del
progetto e il suo digest vanno aggiornati insieme al manifest.

Deviazioni residue (regola, ambito, rischio e chiusura):

| Regole | Ambito e rischio residuo | Condizione di chiusura |
|---|---|---|
| RT-010/019/020/024 | Facade scriptate e subsystem di settlement; non dimostrano ogni terminale/limite dopo effetto di un engine reale. Possibile attribuzione indebita di completezza o certezza. | Prove per ogni ingresso e componente reale, conservazione integrale di fase/effetto/report e conferma dello stesso SHA. |
| RT-011/012/016/017/018/021/023/025 | Corpus raw e facade distinti; schemi/cataloghi fixture non qualificano tutti gli ingressi e contratti reali. Rischio di accettazione o rifiuto diverso nel deployment. | Ratifica contratti, adapter S3 e corpus completo su ogni ingresso reale. |
| RT-013 | Riferimenti fixture e rifiuto file:; autorizzazione tenant/scopo/revoca non qualificata per resolver reali. Rischio di accesso non autorizzato. | Resolver reali S3 con zero aperture e sentinelle assenti in errori/log. |
| RT-014/015 | Probe discovery, manifest/risorse e grafo separati; nessuna prova di fedelta Arrow dei cinque engine. Rischio di perdite o dipendenze indebite. | Grafo con adapter S3 e byte/digest/schema/metadata per hop reale S4. |
| RT-022/026/027 | Race e store/broker effimeri; nessuna qualifica dedup dei provider, TLS/ACL, topologia e retention operativa. Rischio di replay o contesto perduto nel deployment. | Profilo operativo reale con policy fissata, forwarding, dedup e recovery qualificati. |

La nuova automazione non e stata eseguita in CI durante questa modifica locale;
non promuove i gate storici ne i consumer legacy. Audit/deny, fuzz prolungato,
memoria e provider S3/S4 conservano i propri gate separati. Una cancellazione
del workflow puo impedire l'upload: assenza dell'artefatto significa mancata
conferma, mai successo.

## S3: adattatore database 11.0.0

Regola: discovery della libreria compilata, nessun alias privato e nessuna
qualifica derivata dal catalogo target. Pin pubblicato `py-v11.0.0`, commit
`f4fdb9b5f09d8babe795f68e894949693aa9d0a1`: `database.read@1` (input/result v1)
e `database.write@2` (input/result v2, contenuto WriteOutcome v3).
Il titolare ha autorizzato queste versioni. **read@2 resta lacuna upstream**:
chiusura solo con un pin che lo esponga realmente, non con un alias locale.

Il nuovo crate adapter dipende dai crate core/engine/PostgreSQL pinnati, usa
Provider/Engine, write_with_consent e settle. Non usa il binding runtime
upstream: budget propri e perdita di read_loss sugli errori sink.finish lo
rendono inadatto a questa integrazione. L'adapter raccoglie read_loss prima
della pubblicazione e conserva il documento originale separato dal risultato
catena, anche con effetto unknown. La query runtime resta summary JSON: non
viene offerta come sostituto di read Arrow.

Profilo ristretto esplicito: IPC non compresso materializzato, 1 MiB per
artefatto/documento, 32 colonne, schema entro 16 KiB, memoria/spool riservati
prima degli effetti. Nessun cast o normalizzazione; schema pubblicato,
metadata, primary geometry e digest sono verificati. Riferimenti transitivi
content-addressed e credenziali risolte dalla configurazione autenticata.
Perdite in read rifiutate; rischi write richiedono fidelity reported e consenso
coperto dall'intero piano. Report assente significa unavailable, mai complete.
Rischio residuo: profili o documenti oltre questi limiti richiedono un altro
adapter qualificato; nessun troncamento. Recovery senza evidenza di transazione
resta requires_recovery e non reinvoca il provider.

Correzione della bozza locale chains-v2: component.identity usa la grammatica
del nome componente (per esempio plenora-database-tools), non quella degli
identificatori di contratto con suffisso vN. Non cambia il pin upstream e non
ratifica la bozza catene. Rischio: consumer di bozze precedenti; chiusura con
ratifica e rigenerazione dei consumer. I contratti common restano al loro pin.

DATA 4.1.5 `2ff92a250ec8921a78d6d842ea92954173b36e63`: run@3 esiste nella API
Rust plenora_cli::artefatti; CLI/SDK run@3 e binding runtime restano lacune,
senza alias run@2/run@3. IO `9c087aa7daf5d9ad10843ebf3027ae8858f66b94` resta
**candidate 9.0.0 non pubblicata**. Questo era il limite del pin DATA 4.1.5; il binding 5.0.0 candidate adottato successivamente e descritto nella sezione S3 DATA sotto.
Le evidenze incrementali non promuovono le cinque superfici dell'inventario:
PostgreSQL reale e parita installata sono gate CI, distinti dalle prove locali.

Il profilo database iniziale e tabellare: campi geometry/geography, metadata
plenora.primary_geometry (anche none) e porte primary_geometry=true sono
rifiutati esplicitamente. Nessun claim Primary Geometry pubblicizzato:
la conservazione semantica spaziale resta da qualificare, non si afferma che
il database perda questi metadati. IO resta il passo successivo previsto.


## S3 IO candidate 9.0.0

Regola: contratti realmente esposti, nessun alias. Pin IO
9c087aa7daf5d9ad10843ebf3027ae8858f66b94 candidate NON pubblicata:
io.read@2 / io.write@2, input v1 e result v2. Discovery, content types e driver
availability provengono dalla libreria collegata. Nessun claim dal catalogo target.

Ambito: adapter Rust e profili host/CLI/wheel, Arrow stream bounded e pubblicazione
bundle atomica dopo fidelity/loss. `reported` non concede perdite IO: chiusura
solo con un contratto comune di consenso IO ratificato e prove relative.
IPC file/compresso/senza EOS fuori profilo vengono rifiutati esplicitamente.

Lacuna upstream verificata: primary_geometry=none multi-geometria e eliminato
in IPC read/write con rapporto lossless. Rischio: perdita metadata silenziosa.
Mitigazione: confronto esatto schema prima di esportare Arrow/pubblicare bundle;
rapporto originale recuperabile, completezza non qualificata. Chiusura: nuovo
pin corretto e regressioni su entrambe le direzioni/formati IPC.

Altre lacune: errori nativi prima della serializzazione possono omettere i
rapporti fidelity/loss e i contatori consumati; si dichiara unavailable, non
rapporto vuoto. I contatori write includono le righe IPC gia verificate; sulle
read fallite il consumo interno non e osservabile e il limite resta riservato.
Richiesta IO non espone clock iniettabile e il binding runtime ha materializzazione
con budget propri: percorso diretto scelto per fornire i tetti. Chiusura con API
che preservino evidenza completa sugli errori e clock/budget governabili.

Contratti catene restano bozza locale; prove locali IO non ratificano la bozza,
non qualificano ogni driver opzionale e non attestano CI Linux/Windows.


## CI25: prerequisito PostgreSQL del provider database 11.0.0

Regola: qualificare solo il profilo eseguito. Ambito: read@1/write@2 del pin
f4fdb9b. Il probe interroga spatial_ref_sys e postgis_typmod_* anche per colonne
non spaziali: PostgreSQL senza PostGIS fallisce not_found/probe. La CI installa
PostGIS e attiva l'estensione nelle basi isolate delle superfici e della coverage.
Rischio: il profilo PostgreSQL senza estensione non e qualificato; nessun bypass
nel provider. Chiusura: correzione upstream e nuovo pin provato senza PostGIS.
Il nuovo job reale e il recupero della soglia coverage 90% devono essere verificati
in CI; la sola raccolta aggiunta non costituisce un gate passato.


## S3 DATA 5.0.0 candidate (eba11205)

Regola: usare le API e i contratti reali, senza alias di versione. Il prodotto
collega plenora-cli/core/pipeline al commit completo
`eba11205df585469a154de3ad76ab22a0cb9cad6`, candidate NON release. Il crate
plenora-cli espone una libreria Rust: nessun subprocess CLI. Discovery runtime
reale data.run@3, input/result v3, artifact_content_types source/sink per le
porte Arrow. CLI e SDK upstream conservano run@2: la parita delle facciate
runtime-tools non cambia le versioni upstream e non qualifica i 146 kernel.

Ambito provato: Arrow stream esatto, porte nominate (1..8 ingressi/uscite),
Primary Geometry e metadata verificati prima/dopo la libreria. Il manifesto
chiuso e C-01 `plenora-data-localized-effects-v1` restano documenti distinti,
con digest e risorse nel journal/rapporto catena. C-01 e associato ai passi
native per numero, ordine, out/op; division_by_zero_rows e verificato dal
manifesto separatamente. `reported` non concede perdite DATA: removed,
metadata_invalidated o divisioni per zero non vengono pubblicati. Chiusura
per profili con perdite: consenso comune ratificato e prove di grant scoped.

Lacuna upstream riprodotta: multi-geometria con primary_geometry=none perde
la chiave durante pubblicazione dello schema anche su piano senza passi;
C-01 ha steps vuoto. Rischio: perdita metadata non riportata. Mitigazione:
confronto conservativo dei metadata dello schema e delle identita dei campi
sopravvissuti; rifiuto prima di pubblicare Arrow, manifesto/C-01 conservati in
upstream_result/diagnostics, fidelity/loss_report unavailable. Chiusura:
nuovo pin che copra anche la riscrittura finale dello schema, test raw metadata.
La guardia puo rifiutare trasformazioni lecite con metadata riscritti: nessun
claim universale di lossless o qualifica derivata dal catalogo.

Errori native possono non restituire C-01; unavailable non significa rapporto
vuoto. Errori pubblici preservano assi e usano messaggio fisso; dettagli native
non vengono esposti. L'effetto native riguarda il resolver privato e resta nel
documento; remote_effect catena resta none per il calcolo, pubblicazione
artefatti solo con conferma del journal. Dopo conferma incerta non si reinvoca.

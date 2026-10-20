# Allineamento ai contratti comuni

Pin adottato: `3aec9f724f5a0ad21366f293895b4d0dbcd0aad1` (precedente `90cffb2b78cace16edad352a19d86f674930c133`).

Il registro sorgente e [contracts/adoption-source.json](../contracts/adoption-source.json); non e un attestato di release v4. Runtime Binding 2.0 e Public Catalogs 2.0 sono adottati con gli ambiti e le deviazioni seguenti. Non si dichiara conformita end-to-end.

Il profilo resta `plenora-runtime-tools-profile-v1`; al pin non esiste alcun catalogo runtime-tools, ne vecchio ne nuovo. Public Catalogs 2.0 governa la selezione del consumer dai documenti capability. Sono copiate le nuove identita database-tools-v2, data-tools-v3, io-tools-v3, senza inventare operazioni runtime.

## Stato per requisito

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
| RT-012 | deviation | runtime: Il codec capability non verifica tutte le identita dell envelope. |
| RT-013 | deviation | runtime: Payload di dominio opachi; nessuna risoluzione autorizzata comune di riferimenti/credenziali. |
| RT-014 | implemented | API Rust discovered: Payload artefatto opaco e metadati risultato preservati, senza reinterpretazione. |
| RT-015 | implemented | API Rust discovered: Resolver e adapter appartengono all applicazione; nessuna dipendenza di dominio. |
| RT-016 | deviation | runtime: Rifiuti preesistenti usano quarantine o prepare anziche validate/none/never. |
| RT-017 | deviation | runtime: Parsing u16 e UUID generici non garantisce tutta la grammatica canonica. |
| RT-018 | deviation | runtime: Ordine globale protocol/unsupported/timeout non qualificato su tutti i probe. |
| RT-019 | deviation | runtime: Serializzazione del rifiuto e riflessione selettiva non integrate nel settlement. |
| RT-020 | deviation | runtime: Identita nuova e causation dei risultati non qualificate end-to-end. |
| RT-021 | deviation | runtime: Il parser RFC3339 esistente non rifiuta tutti gli offset vietati. |
| RT-022 | deviation | runtime: Supporto controllo verificato, ma chiave vuota/limite specifico/conflitto input non qualificati. |
| RT-023 | deviation | runtime: Input opaco: il runtime non riconosce un secondo campo deadline di dominio. |
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

Regola: RT-012. Ambito: runtime. Il codec capability non verifica tutte le identita dell envelope. Hazard: Identita non canoniche possono attraversare un adapter. Rientro: Eseguire tutti i probe sulle vere frontiere codec/trasporto.

### RT-013

Regola: RT-013. Ambito: runtime. Payload di dominio opachi; nessuna risoluzione autorizzata comune di riferimenti/credenziali. Hazard: Percorsi o segreti presenti in input di adapter non qualificati. Rientro: Qualificare adapter con schemi di proprieta del componente.

### RT-016

Regola: RT-016. Ambito: runtime. Rifiuti preesistenti usano quarantine o prepare anziche validate/none/never. Hazard: Semantica pubblica del rifiuto diversa da RT-016. Rientro: Uniformare codec, dispatcher e settlement e provare tutti i probe.

### RT-017

Regola: RT-017. Ambito: runtime. Parsing u16 e UUID generici non garantisce tutta la grammatica canonica. Hazard: Normalizzazione di valori come 01. Rientro: Validazione lessicale esatta prima della conversione in tutto il workspace.

### RT-018

Regola: RT-018. Ambito: runtime. Ordine globale protocol/unsupported/timeout non qualificato su tutti i probe. Hazard: Categoria dipende dal percorso adapter. Rientro: Applicare un unico ordine e provare difetti multipli.

### RT-019

Regola: RT-019. Ambito: runtime. Serializzazione del rifiuto e riflessione selettiva non integrate nel settlement. Hazard: Metadati mancanti o impropri nel risultato. Rientro: Costruire rifiuti al confine del trasporto con probe neri.

### RT-020

Regola: RT-020. Ambito: runtime. Identita nuova e causation dei risultati non qualificate end-to-end. Hazard: Caller non puo correlare ogni rifiuto al messaggio. Rientro: Verificare successi e rifiuti sui trasporti reali.

### RT-021

Regola: RT-021. Ambito: runtime. Il parser RFC3339 esistente non rifiuta tutti gli offset vietati. Hazard: Deadline non UTC accettata. Rientro: Parser esatto UTC e prova ManualClock su <= now.

### RT-022

Regola: RT-022. Ambito: runtime. Supporto controllo verificato, ma chiave vuota/limite specifico/conflitto input non qualificati. Hazard: Deduplicazione non garantita. Rientro: Qualificare grammatica, limiti e store deduplicazione del componente.

### RT-023

Regola: RT-023. Ambito: runtime. Input opaco: il runtime non riconosce un secondo campo deadline di dominio. Hazard: Doppio canale deadline. Rientro: Adapter rifiuta campo payload e metadata insieme.

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

Tutti i 33 vettori runtime-v1 e i due runtime-v2 passano per codec/modello pubblico; i probe runtime-probes-v1 sono copiati e verificati per hash, ma non sono una suite runtime passata (RT-016..023 aperti). ERR-015/016 sono prove di preservazione delle assi, non di pubblicazione terminale.

Decisioni 0015, 0017..0021 confrontate nella copia locale; 0016 non e presente al pin. Interoperabilita Arrow (#34) e adapter REST/Arrow non applicabili al trasporto opaco. #39 e #40 riguardano i gate upstream: non introducono wire o capability nuovi. Nessuna dipendenza Rust aggiunta. Nessun budget di dimensione del codice trovato nei gate o nelle policy locali.

Il pin e ogni copia sono verificati da `python scripts/check-contract-adoption.py`, opzionalmente con `--upstream C:/pg/contracts-main`. Il gate non scrive nella copia upstream e non usa rete. Per la validazione normativa completa: `python C:/pg/contracts-main/tools/validate_specs.py` con `PYTHONDONTWRITEBYTECODE=1`.

### CAP-004

Regola CAP-004; ambito discovery con versioni simultanee. Rientrata il 2026-10-20: operazioni indicizzate per coppia nome/versione, lookup pubblici e dispatch esplicitamente versionati; duplicati della stessa coppia rifiutati. Hazard precedente: un documento valido con piu versioni simultanee non era registrabile. Condizione di rientro soddisfatta dalle prove in `rest_discovery.rs` e `registry_dispatch.rs`: catalogo data-tools-v3 con data.run@2 e @3, superfici distinte, selezione dei contratti input/output per versione, nessun fallback e nessuna sovrascrittura. Il registro handler era gia indicizzato per CapabilityId (nome e versione wire); la correzione riguarda la mappa delle operazioni, la cui versione e indipendente dal wire. Il profilo REST seleziona esplicitamente v1. Le API operation e operation_named richiedono ora OperationVersion: cambiamento di firma intenzionale per eliminare lookup ambigui. Le prove sintetiche con entrambe le versioni su runtime non dichiarano quella superficie per data.run@2 nel catalogo reale. La chiusura locale non attesta una release o la CI Linux/Windows.


Public Security 1.0 e selezionato dal profilo: la protezione del payload opaco e dei riferimenti resta delegata agli adapter (deviazione RT-013); nessun claim di conformita SEC complessiva.

# Revisione di sicurezza: scheletro runtime generico v0.1

- Data della revisione: 2026-08-16
- Ambito: l'intero workspace `runtime-tools`, esempi, script e configurazione della CI
- Stato: revisione tecnica completa in locale; approvazione organizzativa del rilascio in attesa
- Escluso: adapter di fondazione Plenora non ancora completati, autorizzazione di business del
  consumer e configurazione del deployment in produzione

## Confini di fiducia rivisti

| Confine | Rischi principali | Controlli implementati |
|---|---|---|
| Ingresso HTTP | body sovradimensionati, iniezione negli identificatori, header segreti, concorrenza illimitata | body e concorrenza limitati, ID validati, header sensibili, errori comuni senza dati sensibili, shutdown ordinato |
| Configurazione e trasporto NATS | divulgazione delle credenziali, downgrade a testo in chiaro, riconnessione o stato di consegna illimitati | tipi di credenziale mascherati, TLS obbligatorio per default, testo in chiaro solo con opt-in esplicito, limiti finiti su riconnessione e consumer |
| Metadati e payload dei messaggi | abuso delle allocazioni, log di segreti, identità malformate | limiti su voci/chiavi/valori/totale, limiti sulla forma codificata prima della decodifica, `Debug` mascherato, validazione degli UUID canonici |
| Esecuzione del worker | crescita illimitata dei task, cancellazione non cooperativa, handle di controllo scaduti | limite a semaforo, nessuna coda posseduta dall'executor, grazia di pulizia limitata, rimozione RAII dei task attivi, ID di task locali all'executor |
| Controllo operativo | pausa/drain/cancellazione/trigger non autenticati, divulgazione del payload | authorizer applicativo obbligatorio per ogni route HTTP, route di mutazione opzionali, ID validati e limitati, DTO senza payload |
| Contenimento dei sottoprocessi | iniezione nella shell, credenziali ereditate, processo/output/RSS fuori controllo | avvio diretto senza shell, ambiente svuotato per default, input/concorrenza/output/tempo limitati, terminazione e reap dell'albero, tetto RSS opzionale su Linux |
| Osservazione del ciclo di vita | blocco dell'handler, crescita della memoria, perdita silenziosa di osservazioni | `try_send` non bloccante, massimo rigido della coda, contatori espliciti di coda piena/chiusa, proiezione sulla health e hook di metrica |
| Settlement sul broker e DLQ | perdita di messaggi, retry alla cieca dopo un effetto sconosciuto, pubblicazione duplicata in DLQ | pubblicazione confermata prima del TERM, ID deterministico del messaggio in DLQ, `OutcomeUnknown` fail-closed, possesso esplicito dell'ACK |
| Outbox/inbox | transazioni spezzate, perdita del claim, effetti di business duplicati | contratti e fake indipendenti dalla persistenza documentano i requisiti di transazione atomica; non si dichiara alcun database di produzione |
| Osservabilità | etichette ad alta cardinalità, propagazione di PII/segreti | etichette limitate e in allowlist, eventi del ciclo di vita senza payload, propagazione transazionale solo W3C, policy di oscuramento |
| Supply chain | dipendenze vulnerabili, senza licenza o non pinnate | pin diretti esatti, lockfile, policy delle sorgenti, strumenti di audit/deny pinnati, controllo dell'architettura |

## Verifiche eseguite

- Formattazione del workspace, Clippy con warning negati, tutti i test, e rustdoc con warning negati.
- Le scansioni di architettura rifiutano blocchi unsafe, scorciatoie verso il panic, output di debug
  che contiene segreti, canali illimitati noti, dipendenze inverse, dipendenze git e versioni dirette
  non esatte.
- Test su JetStream reale ed effimero coprono la policy TLS/testo in chiaro, i limiti dei metadati,
  il rinnovo degli heartbeat, retry/riconsegna, readiness alla riconnessione, isolamento del replay,
  DLQ confermata e drain.
- `cargo audit` non ha trovato advisory che riguardino il lockfile corrente.
- `cargo deny check` ha passato advisories, bans, licenses e sources. Le versioni duplicate delle
  dipendenze restano warning e vanno riviste a ogni aggiornamento.

## Rischi residui e azioni richieste ai responsabili

1. Runtime-tools di proposito non implementa alcuna policy di identità o di ruolo. L'adapter HTTP di
   controllo chiama un authorizer applicativo per ogni richiesta e nasconde per default le route di
   mutazione, ma il consumer deve autenticare i chiamanti, autorizzare le azioni esatte, limitare la
   frequenza sulla route e registrare in audit le mutazioni accettate.
2. `WorkerTaskId` è locale all'executor. Le API remote devono accoppiarlo all'`instance_id` del
   worker e usare il `MessageId` canonico quando serve un'identità che sopravviva ai riavvii.
3. La cancellazione async di Rust non può fermare task staccati o chiamate native bloccanti. Gli
   adapter di fondazione devono collegare il token fornito e imporre scadenze limitate alle proprie
   operazioni.
4. La saturazione del ciclo di vita scarta l'osservazione più recente. Un deployment che la richiede
   deve scegliere `WorkerLifecycleHealthCriticality::Required` e sorvegliare il ricevitore; un
   deployment opzionale resta ready ma degradato.
5. Gli store in memoria di outbox/inbox sono test double. Gli effetti di business in produzione non
   sono sicuri finché l'adapter del database non fornisce transazioni atomiche business/outbox e
   business/inbox, lease dei claim, recupero dopo crash e risultati di idempotenza memorizzati.
6. NATS in testo in chiaro è solo per test e uso locale e richiede una configurazione esplicita. La
   revisione del deployment in produzione deve validare certificati, credenziali, permessi sui
   subject e modalità dell'infrastruttura.
7. Questa revisione non qualifica un albero di lavoro non committato. L'approvazione finale deve
   riferirsi al commit esatto che ha passato tutti i job di GitHub Actions.

## Checklist di approvazione del rilascio

- [ ] Commit esatto del rilascio creato e pushato.
- [ ] Passati sullo stesso SHA tutti i job di `ci.yml`: qualità su Windows, Linux e macOS (con la
  documentazione), anti-panic, dependency-policy, NATS, coverage e fuzz smoke.
- [ ] Rivista l'autenticazione e l'autorizzazione specifica del consumer.
- [ ] Rivisti TLS e permessi di NATS in produzione.
- [ ] Incluso nell'ambito della revisione ogni nuovo adapter di fondazione.
- [ ] Approvazione del responsabile della sicurezza registrata fuori dal repository, attraverso il
  canale interno approvato.

## Adozione 3aec9f7: gate aperti

- Regola RT-007; ambito runtime; hazard: Retry di mutazioni senza prova di deduplicazione. Rientro: Qualificare adapter e almeno-un-solo-avvio per identita senza chiave.
- Regola RT-010; ambito runtime; hazard: Caller senza risultato terminale. Rientro: Collegare error sink dopo esaurimento retry con pubblicazione confermata.
- Regola RT-012; ambito runtime; hazard: Identita non canoniche possono attraversare un adapter. Rientro: Eseguire tutti i probe sulle vere frontiere codec/trasporto.
- Regola RT-013; ambito runtime; hazard: Percorsi o segreti presenti in input di adapter non qualificati. Rientro: Qualificare adapter con schemi di proprieta del componente.
- Regola RT-016; ambito runtime; hazard: Semantica pubblica del rifiuto diversa da RT-016. Rientro: Uniformare codec, dispatcher e settlement e provare tutti i probe.
- Regola RT-017; ambito runtime; hazard: Normalizzazione di valori come 01. Rientro: Validazione lessicale esatta prima della conversione in tutto il workspace.
- Regola RT-018; ambito runtime; hazard: Categoria dipende dal percorso adapter. Rientro: Applicare un unico ordine e provare difetti multipli.
- Regola RT-019; ambito runtime; hazard: Metadati mancanti o impropri nel risultato. Rientro: Costruire rifiuti al confine del trasporto con probe neri.
- Regola RT-020; ambito runtime; hazard: Caller non puo correlare ogni rifiuto al messaggio. Rientro: Verificare successi e rifiuti sui trasporti reali.
- Regola RT-021; ambito runtime; hazard: Deadline non UTC accettata. Rientro: Parser esatto UTC e prova ManualClock su <= now.
- Regola RT-022; ambito runtime; hazard: Deduplicazione non garantita. Rientro: Qualificare grammatica, limiti e store deduplicazione del componente.
- Regola RT-023; ambito runtime; hazard: Doppio canale deadline. Rientro: Adapter rifiuta campo payload e metadata insieme.
- Regola RT-024; ambito runtime; hazard: Garanzia indebolita: perduta fase/certezza dopo pubblicazione; sink puo avere un limite inferiore. Rientro: Trasportare fase/effetto provati e tradurre tutti i limiti del sink; qualificare trasporto reale.
- Regola RT-026; ambito runtime; hazard: Downgrade implicito possibile nel routing legacy. Rientro: Collegare policy a ogni destinazione scelta prima dell invio.
- Regola RT-027; ambito runtime; hazard: Contesto perso su retry/redrive/resend; nessuna finestra durevole garantita. Rientro: Store atomico autorevole con tombstone e finestra dichiarata; cablare ogni hop e testare riavvio/race.
- Regola ADOPTION-v4; ambito release; hazard: Nessuna evidenza immutabile di release o endpoint conforme. Rientro: Produrre e verificare artefatti con digest reali e manifesto v4, senza claim per contratti con deviazioni.

Stato dettagliato e comportamento osservato: [registro contratti](contract-alignment.md). Nessuna conformita runtime end-to-end o release v4 attestata da queste prove locali.

Regola CAP-004; ambito discovery con versioni simultanee. Rientrata il 2026-10-20: operazioni indicizzate per coppia nome/versione, lookup pubblici e dispatch esplicitamente versionati; duplicati della stessa coppia rifiutati. Rischio precedente: documento multi-versione non registrabile. Condizione di chiusura: lookup e dispatch esatti e prove multi-versione, ora presenti; dettagli nel [registro contratti](contract-alignment.md#cap-004).

## Dipendenze 0.1.1: limiti della verifica locale

Regola: verifica supply-chain corrente e separazione delle fixture dalle
credenziali di deploy. Ambito: entrambi i lock del candidato 0.1.1.
Rischio residuo: la prova offline usa il database RustSec del 2 novembre 2026;
inoltre archivi di dipendenze già presenti distribuiscono chiavi private di test
pubblicamente distribuite, in particolare async-nats e fixture PEM dei crate crittografici.
Non sono credenziali di produzione e non sono state introdotte dal bump.
Condizione di chiusura della verifica: gate supply-chain aggiornati sullo
SHA candidato e controllo dell'artefatto effettivamente distribuito, affinché
le fixture non siano usate come credenziali di deploy. Nessun artefatto di
rilascio è stato prodotto in questa sessione. Inventario dei percorsi e
limiti nel [rapporto 0.1.1](dependency-alignment-0.1.1.md).

## Rischio residuo del contratto catene di riferimento

Regola: nessun resolver/invoke prima dell'ammissione e nessun successo senza prove.
Ambito: [CHAIN-PLAN-2.0](specs/CHAIN-PLAN-2.0.md), P1.2.
Il validatore Python verifica forma e invarianti su un contesto fixture esplicito;
non verifica autorizzazione reale, digest dei documenti esterni, validità del
payload upstream, provider, report disponibili o conservazione Arrow effettiva.
Rischio: usare `reference_only` come ammissione di produzione o considerare
`reports_complete` una prova autenticata. Chiusura: validatore P1.3 e host con
registry congelato, resolver autorizzati, barriere effettive, store e adapter
qualificati. Nessuna promessa runtime o chiusura delle deviazioni RT legacy.

## P1.3: limite della validazione Rust

Regola: nessuna failure silenziosa e nessun invoke non ammesso. Ambito:
`plenora-runtime-chain-contracts`, registro fixture indipendente dal piano.
I tipi wire sono privati: il parser rifiuta duplicati e valori non canonici
prima della conversione; `Debug` ed errori sono senza dati. Non esistono
callback invoke, resolver o client infrastrutturali nel crate.
Rischio residuo: una discovery sintetica non prova payload, fidelity Arrow,
autorizzazione del consenso, disponibilità dei rapporti o quote effettive.
`ValidationReport` dichiara `reference_only`, barriere schema e fidelity da
provare; non è un token di esecuzione. Condizione di chiusura: host e adapter
qualificati che eseguano tali verifiche prima dell'invoke e rifiutino mutazioni
di discovery/input/consenso. Nessuna garanzia runtime è promossa da queste prove.

## P2.1: rischio residuo di composizione del boundary

Regola: nessuna perdita silenziosa e rifiuto prima di effetti.
Ambito: `plenora-runtime-boundary`, policy di contratto, sink e ingressi legacy.
Rischio: una policy host che compie I/O viola il contratto di validazione pura;
un consumer che ignora `Rejection` può lasciare il caller senza esito. Il decoder
generico del runner non equivale a un finalizzatore durevole. Nessuna promessa
di dedup/CAS o replay persistito è implementata da questo crate.
Condizione di chiusura: policy concrete qualificate con resolver strumentati,
wrapper dei trasporti che persistono/pubblicano anche i rifiuti, store P2.2 e
finalizzatore P2.4 con prove di risposta/ACK persi e restart. Gli esempi legacy
non sono ingressi qualificati del prodotto catene. Le prove locali sono
circoscritte in [qualification](qualification.md#gate-p21-ammissione-raw-e-limite-della-consegna).

## P2.1b: ammissione e limiti della prova

Regola RT-012/016..023: il dispatch registrato richiede il tipo ammesso dal
boundary. Ambito: API Rust, wire NATS e delivery Apalis locali. La raccolta
completa precede la migrazione del registro/dispatcher; i metadati operativi del
broker sono separati da quelli ammessi. Rischio residuo: attribuire a questa
prova la durabilità o la pubblicazione terminale, oppure estenderla ai futuri
adapter di dominio. Condizione di chiusura: P2.2 (CAS/digest, race e restart),
P2.4 (settlement terminale confermato), S3 (schemi reali), CI Linux/Windows e
profilo del broker effettivo. Questi gate rimangono aperti; nessuna garanzia
end-to-end è dedotta dalla sola parità locale delle facciate.


## CI PR 21: rifiuto del consumer NATS

Regola: nessuna Delivery esposta prima dell'ammissione di trasporto (RT-012/016..023).
Ambito: consumer JetStream operativo e replay; la parità locale del carrier wire
non copre la precedenza dei rifiuti di trasporto, descritta in
[contract-alignment](contract-alignment.md#p21b-confine-delle-facciate-esistenti).
Rischio residuo: TERM inviato tramite `ack_with` non equivale a conferma durevole
del server; i fake verificano richiesta e propagazione errori, non durabilità.
Gate aperto: real_nats e coverage su Linux con Docker, CI Linux/Windows sulla
correzione e profilo di deployment. Condizione di chiusura: esecuzione di questi
gate e qualificazione del settlement confermato in P2.4. Nessun gate reale è
attestato dalla sola prova offline della sequenza rifiuto/valido.


## P2.2: rischio residuo dello store PostgreSQL

Regola: durabilità e lettura autorevole senza perdita silenziosa. Ambito:
[store catene](../crates/plenora-runtime-chain-store/README.md). Rischio residuo:
NoTls richiede rete fidata/loopback; fsync e synchronous_commit confermano il
primary locale, non disaster recovery n? promozione di una replica aggiornata.
Amministrazione del clock, permessi, backup, crescita dei tombstone permanenti,
quote database e throughput del lock globale richiedono qualifica operativa.
Errori del client non espongono dati o source del driver. Il token di avvio
non viene riemesso dopo restart; un'intenzione pendente richiede riconciliazione.
Condizione di chiusura: profilo deployment autenticato/TLS, failover e recovery
provati, monitoraggio capacità/readiness, gate PostgreSQL sul candidato e
integrazione dell'host P2.3/P2.4. Nessuna qualifica di produzione è attestata.


## P2.3/P2.4: rischi residui di deployment

Regola: nessuna perdita di risposta autorizza nuova mutazione. Ambito:
DurableRuntime, journal PostgreSQL v2 e relay. Ricevuta pre-ammissione, budget JSON,
finalizzazione recuperabile e route persistita chiudono le lacune del candidato.
Running senza evidenza resta recovery esplicito: non si presume un esito remoto.
L'host deve associare il nome persistito della route allo stesso endpoint
configurato e autenticato a ogni restart. Il database e la configurazione host
sono autorita fidate; non si promette tolleranza a loro alterazioni malevole.
Rischio residuo: test con servizi effimeri non qualificano TLS, permessi subject,
backup/restore, failover o adapter di dominio reali. Chiusura: job reali verdi sul
medesimo candidato e qualifica operativa del deploy. Le chiavi d'idempotenza host usano lo scope fidato tenant/operazione P1.4a,
come dichiarato in
[allineamento](contract-alignment.md). Nessuna qualifica di produzione attestata.

## Profilo scriptato catene non qualificato

Regola: nessuna superficie di produzione aggira boundary, autenticazione e
negoziazione. Ambito: ScriptedHost e request/reply NATS di ADR 0010. Rischio:
l'harness usa il tenant e la discovery forniti dalla configurazione fidata e
non offre ancora il profilo protetto end-to-end. Non esporlo come prodotto di
produzione. Chiusura: corpus RT completo su cinque
superfici, store/broker reali e revisione indipendente.

## Worker P1.4b: ownership e arresto

Regola: il lease di coordinamento non prova terminazione del provider. Ambito:
worker host e CAS del journal; il fence impedisce conferme di un owner vecchio,
la ricevuta runtime impedisce un secondo invoke anche prima del takeover.
Rischio residuo: kill/drop esterno del processo e provider che non rispondono
non sono arresti confermati; Running senza prova resta recovery, senza nuovo
writer. Shutdown cooperativo drena il lavoro corrente; nessun claim di hard kill.
Chiusura: qualifica dei provider/OS e deployment reale, separata dal criterio 2
provato con adapter scriptati. PostgreSQL/NATS effimeri in CI non attestano TLS,
permessi o failover di produzione.

### P1.4c-1 journal scope

The finite quota covers serialized run state, including original raw evidence,
not PostgreSQL physical/WAL storage or total process RSS. A defective adapter's
oversized but structurally accepted receipt is retained in a separately reserved
slot and recoverable through bounded native report pages; it cannot be declared
successful merely by understating counters. Legacy intents without a proven
report reservation cannot start new work. Real provider containment, resource
leases and cleanup remain open. Closure of those risks requires their separate
qualification; no library artifact is attested by the scripted profile.

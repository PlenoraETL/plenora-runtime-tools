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

- Regola CAP-004; ambito discovery con versioni simultanee; hazard: Un documento valido con piu versioni simultanee, come data.run, non e registrabile. Rientro: Indicizzare per coppia e rendere ogni lookup e dispatch esplicitamente versionato; provare cataloghi multi-versione.

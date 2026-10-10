# ADR 0003: contratti di messaggistica indipendenti dal broker e sicurezza dei retry

- Stato: accettato
- Data: 2026-08-15

## Contesto

Il runtime ha bisogno di contratti portabili per identità dei messaggi, metadati di propagazione,
conferma di consegna, retry, dead-letter e replay. I broker concreti differiscono in tutte queste
aree. Anche la serializzazione del payload è una scelta dell'applicazione e non deve diventare un
obbligo accidentale del framework.

La pubblicazione può fallire con un esito remoto sconosciuto. Ritentare automaticamente in quello
stato può duplicare un effetto, mentre trattarlo come successo può perdere un messaggio.

## Decisione

Il crate di messaggistica:

- rappresenta i payload codificati come media type più byte opachi e richiede i codec al confine;
- conserva metadati sicuri per i dati binari sotto chiavi portabili, con namespace e validate;
- possiede la capability di acknowledgement dentro ogni consegna, così ACK o NACK consumano la
  consegna;
- dichiara le funzionalità del broker tramite capability esplicite anziché garanzie dedotte;
- classifica separatamente gli errori ritentabili, permanenti, dead-letter e a esito sconosciuto;
- usa un backoff esponenziale validato e con tetto, con jitter deterministico opzionale da seme;
- rifiuta di ritentare gli esiti sconosciuti a meno che la configurazione non lo abiliti
  esplicitamente.

L'output Debug espone identità, dimensioni e chiavi, ma oscura i valori di payload e metadati.

## Conseguenze

Gli adapter traducono la propria semantica nativa senza far trapelare tipi del broker nel codice
applicativo. Le applicazioni scelgono codec e namespace dei metadati. Il jitter deterministico
rende ripetibili i test delle policy. Un chiamante che abilita il retry per gli esiti sconosciuti
si assume il relativo rischio di idempotenza e duplicazione.

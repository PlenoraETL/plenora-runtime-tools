# Runtime HTTP

`plenora-runtime-http` è un adapter di trasporto attorno ai contratti condivisi del runtime. Le
applicazioni compongono le proprie rotte con il router del runtime; autenticazione,
autorizzazione, politica di rate e mappatura degli errori di dominio restano responsabilità
dell'applicazione.

L'ammissione è limitata prima che girino gli handler dell'applicazione. Il limite globale
predefinito delle richieste in corso è 256 e il limite predefinito del corpo è 1 MiB; entrambi
sono limiti validati, configurabili e diversi da zero. I corpi troppo grandi si normalizzano
attraverso l'hook d'errore comune `payload_too_large`, che non espone dati sensibili.

Ogni richiesta ammessa ha due identificatori:

- `x-request-id` identifica un singolo scambio HTTP. Un valore mancante viene generato; un valore
  fornito e malformato viene rifiutato e mai riflesso.
- `x-correlation-id` identifica un'operazione logica attraverso HTTP e messaggistica. Usa
  l'identificatore di correlazione condiviso della messaggistica, viene generato se assente e
  rifiutato se malformato.

Entrambi i valori sono messi nelle estensioni della richiesta, registrati sullo span HTTP e
copiati nella risposta. Sono esclusi di proposito dagli attributi delle metriche perché hanno
cardinalità illimitata.

`/health` riporta la liveness aggregata e `/ready` la readiness all'ammissione. Gli stati
aggregati positivi restituiscono `200`; gli stati non sani o non pronti restituiscono `503`. Le
risposte espongono solo parole di stato stabili. Nomi dei componenti, messaggi per l'operatore,
sorgenti degli errori, credenziali e configurazione restano dentro il processo.

Lo spegnimento è cooperativo e limitato. Il segnale del runtime avvia il drain ordinato delle
connessioni; se il periodo di grazia configurato scade, l'adapter restituisce al suo proprietario
un timeout strutturato. Il proprietario del processo decide se, dopo quel risultato, registrare,
supervisionare o terminare.

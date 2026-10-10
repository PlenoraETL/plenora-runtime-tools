# Operazioni e capacità dei worker

I worker di Plenora sono future asincrone di handler create su richiesta. Non sono un pool fisso di
processi del sistema operativo. Con `max_in_flight = 5`, a riposo non esiste alcun handler, al più
cinque messaggi vengono eseguiti in concorrenza, e il lavoro in più resta in JetStream finché non
c'è capacità disponibile. I thread del runtime Tokio eseguono quelle future; le chiamate di libreria
bloccanti o pesanti sulla CPU devono usare una struttura bloccante/di calcolo limitata a parte.

## Scegliere `max_in_flight`

La RAM da sola non basta a stabilire un numero sicuro di worker. Si parte dal più stretto di
questi limiti:

```text
memory_limit = floor((memory_budget - process_baseline) / peak_memory_per_task)
resource_limit = min(database_pool, remote_api_limit, file_handle_limit, subprocess_limit, ...)
safe_max_in_flight = min(memory_limit, resource_limit, measured_stable_concurrency)
```

Si misura il picco di memoria residente con payload rappresentativi; non si usa la dimensione del
messaggio codificato come approssimazione di dataframe o buffer decodificati. Su un host da 32 GiB,
si riserva memoria per il sistema operativo, NATS, i database, le cache e i picchi dell'allocatore.
Un task con picco di 500 MiB rende `5` sostanzialmente diverso da un task con picco di 10 MiB. Si
comincia in modo prudente, si esegue il soak del consumer reale, poi si aumenta un passo alla volta
osservando latenza, memoria, CPU, attesa sul pool del database, riconsegne e fallimenti.

Questi limiti vanno tenuti allineati:

- `max_in_flight` del worker è il tetto di esecuzione dell'applicazione;
- `max_ack_pending` di JetStream deve essere finito e di norma uguale a quel tetto per ogni
  consumer;
- i pool dei client di database e HTTP non devono permettere al worker di creare una coda nascosta
  più grande;
- i limiti di payload di richieste e messaggi devono riflettere la memoria decodificata nel caso
  peggiore, non la RAM disponibile;
- la grazia di shutdown deve superare il normale tempo di cancellazione cooperativa ma restare
  finita sul piano operativo;
- la finestra di fallimento dell'heartbeat di consegna deve restare più breve dell'`ack_wait` di
  JetStream.

## Readiness all'avvio

Stream e consumer durable si predispongono fuori dal worker. All'avvio:

1. validare ogni limite locale e ogni sorgente di segreti;
2. connettersi a NATS e sondarlo;
3. agganciare il durable esistente e verificarne la configurazione di ACK, consegna, pendenti e
   filtro;
4. costruire e congelare il registro delle capability;
5. avviare il monitor delle risorse fail-closed e ottenerne il primo campione riuscito;
6. ripristinare definizioni e cursori dello scheduler dall'adapter di persistenza
   dell'applicazione;
7. congelare il registro limitato di controllo del runtime e montarne il router dietro
   l'autorizzazione dell'applicazione, abilitando le route di modifica solo quando servono sul
   piano operativo;
8. avviare il runner dei worker, lo scheduler e l'heartbeat dell'istanza come task supervisionati;
9. dichiarare ready solo dopo che tutti i componenti richiesti sono pronti.

Una configurazione non corrispondente è un fallimento all'avvio, non un motivo per modificare
l'infrastruttura di produzione. La creazione di risorse e il trasporto in chiaro restano opt-in
espliciti per lo sviluppo locale.

## Segnali a runtime

Monitorare almeno:

| Segnale | Significato operativo |
|---|---|
| readiness | Se l'istanza può accettare traffico e continuare a consumare. |
| età dell'heartbeat del worker | Se il processo del worker e il monitor periodico sono vivi. |
| `in_flight / max_in_flight` | Saturazione corrente dell'esecuzione. |
| profondità/scarti della coda del ciclo di vita | Se la persistenza osservativa o la telemetria restano indietro. |
| riconnessioni NATS e tentativi di riconsegna | Instabilità del broker o handler che superano i limiti di possesso. |
| disposizioni di retry e DLQ | Pressione dei fallimenti applicativi; allertare sulle variazioni di frequenza, non sul testo del payload. |
| conteggio di `OutcomeUnknown` | Effetti che richiedono riconciliazione invece di un retry alla cieca. |
| durata del drain e arresti forzati | Se i limiti di shutdown corrispondono al comportamento reale dei task. |
| RSS del processo e stato di pressione | Se l'ammissione va in pausa prima di raggiungere il limite del container. |
| saturazione/esecuzioni mancate/riconciliazione dello scheduler | Se il dispatch programmato tiene il passo in modo sicuro. |
| sottoprocessi in corso/timeout/terminazioni per RSS | Se le operazioni isolate rientrano nei loro limiti espliciti. |

Heartbeat ed eventi del ciclo di vita sono osservativi. Il loro passaggio di consegne limitato può
scartare osservazioni sotto pressione, ma non deve mai bloccare l'esecuzione degli handler né
determinare il comportamento di ACK/NAK.

## Procedura di shutdown

Un unico percorso di shutdown coordinato deve fermare prima l'ammissione HTTP e dei worker, poi
lasciare che gli handler attivi osservino la cancellazione e facciano il drain entro il periodo di
grazia configurato. Le consegne del broker ricevono ACK solo dopo il successo; il lavoro non finito
riceve NAK o resta idoneo alla riconsegna del server secondo l'esito dell'adapter. Infine si fa il
drain della connessione NATS e del supervisore del runtime.

Se un'istanza supera ripetutamente il suo limite di drain, non basta aumentare il timeout. Si
individua la chiamata di libreria specifica che ignora la cancellazione cooperativa, le si aggiunge
un ponte limitato, e si prova il percorso di arresto forzato/requeue.

## Cadenza di qualifica

La suite normale del workspace si esegue a ogni modifica. I test reali su JetStream e la suite di
stabilità limitata da 2.048 messaggi si eseguono prima di fare il merge di modifiche a worker,
broker, retry o shutdown. Campagne di fuzz più lunghe e soak simili alla produzione si eseguono
prima di una release candidate e dopo aver cambiato un adapter verso una libreria reale. Vedi
[`testing.md`](testing.md), [`stability.md`](stability.md) e
[`qualification.md`](qualification.md) per i comandi esatti e i gate di rilascio.

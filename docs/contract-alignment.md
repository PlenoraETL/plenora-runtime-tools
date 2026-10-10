# Allineamento ai contratti comuni

Questo repository consuma il confine runtime pubblico definito da
`plenora-contracts`; non è uno dei cinque profili di componente di dominio. La
revisione sorgente immutabile e i vettori di conformità vendorizzati sono
registrati in [`contracts/source.json`](../contracts/source.json).
`plenora-contracts` è un repository pubblico; la CI usa le copie vendorizzate
perché la validazione resti offline e riproducibile alla revisione pinnata.

## Copertura attuale di Runtime Binding 1.0

| Confine | Stato | Prova o lavoro mancante |
|---|---|---|
| Routing delle richieste | Implementato | `CapabilityMessageCodec` richiede nome/versione della capability, nome/versione dell'operazione e contratto di input immutabile. |
| Conservazione dei metadati | Implementata al confine pubblico del codec | La matrice di richieste pinnata fa il giro completo di content type, payload, scadenza, causation e `plenora.trace.correlation_id` attraverso i selettori database, data, IO e REST. I campi di routing si tolgono solo dalla vista di input posseduta dall'adapter e si ripristinano in codifica. |
| Identificatori di contratto | Implementati | `ContractId` ammette solo identificatori `plenora-...-vN` minuscoli e limitati; `OperationVersion` rifiuta lo zero. |
| Risultato di successo | Implementato | I vettori di successo JSON e Arrow esercitano `CapabilityResponse`; si controllano contratto di output e content type scoperti e i limiti in byte, i metadati canonici del risultato conservano versione dell'operazione, contratto di output e UUID di correlazione, e il successo del worker richiede una pubblicazione confermata attraverso `CapabilityResultSink`. |
| Errore pubblico tipizzato | Modello e mappatura implementati; pubblicazione terminale aperta | La matrice degli errori si ricostruisce attraverso `PlenoraError` e si confronta semanticamente come JSON con `plenora-error-v1`, compresi il retry sicuro e il recupero da effetto sconosciuto. `CapabilityFailure::with_public_error` conserva le mappature dei componenti senza dedurle dallo stato HTTP; i fallimenti posseduti dal runtime hanno mappature stabili. Pubblicare l'errore finale solo dopo l'esaurimento dei retry spetta ancora al settlement del worker. |
| Capability Discovery 2.0 | Implementato | Documenti v2 limitati si congelano a ogni registrazione. Il dispatch controlla disponibilità, superficie runtime, versione, contratto di input, content type e controlli supportati prima dell'invocazione. Il profilo REST richiede in più tutte e cinque le operazioni ratificate e la semantica esatta di output e attributi. |
| Scadenza assoluta | Implementata | Le scadenze UTC RFC 3339 si validano prima dell'invocazione, corrono in parallelo all'esecuzione e propagano `TaskCancellationReason::Timeout`; un timeout dopo l'avvio resta `remote_effect: unknown`. |
| Artefatti e segreti REST | In attesa degli schemi di payload posseduti dal componente | Il runtime non indovina campi dentro JSON opachi. Il rifiuto esatto di percorsi e segreti inline e la risoluzione autorizzata di sorgente/destinazione degli artefatti diventano implementabili quando `rest-tools` pubblica gli schemi immutabili nominati dal suo profilo. La diagnostica di messaggi, risultati ed errori omette già il payload. |

La riga ancora aperta è un gate di rilascio esplicito. Runtime-tools di proposito
non inventa campi di payload REST né interpreta in modo euristico JSON posseduto
dai componenti.

## Aggiornare il pin

Non seguire il nome di un branch. Rivedere un commit completo di
`plenora-contracts`, sostituire le copie dei vettori con quelle di quella
revisione esatta, aggiornare lo SHA di 40 caratteri in `contracts/source.json` e
nel test di conformità, poi eseguire la suite di qualifica completa del
workspace.

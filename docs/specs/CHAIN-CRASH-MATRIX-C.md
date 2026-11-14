# Crash matrix C.4: fonte di enumerazione

Estratto invariato da PROGETTO-CATENE.md del progetto runtime-tools, sezione C.4,
letto il 14 novembre 2026. Gli ID C01-C07 seguono l'ordine delle righe.
La matrice e una specifica; non attesta esecuzioni.

| Punto del crash | Azione al restart | Divieto |
|---|---|---|
| Prima dell'intenzione durevole | Il messaggio può essere riammesso secondo inbox/acceptance | Nessun avvio se lo store non conferma |
| Intenzione presente, nessuna prova che invoke sia iniziata | Riprendere solo se una prova/fence esclude l'avvio precedente; altrimenti trattare come potenzialmente avviato | Non dedurre none dall'assenza del risultato |
| Running senza esito dopo scadenza lease | Interrogare recovery del componente/provider o verificare artefatto/chiave remota; in assenza di prova requires_recovery | Nessun secondo writer perché il lease è scaduto |
| Commit remoto noto, journal non completato | Ricostruire da ricevuta/recovery verificabile; altrimenti unknown | Non assumere rollback per risposta persa |
| Esito e outbox durevoli, risposta esterna persa | Ripubblicare lo stesso risultato con lo stesso result_message_id | Non rieseguire dominio |
| ACK perso dopo risultato | Inbox ritrova il terminale e ripete il settlement | Nessun nuovo tentativo di mutazione |
| Artifact temporaneo senza pubblicazione | Cleanup dopo verifica ownership e lease; non è output disponibile | Non rendere pubblico un file incompleto |

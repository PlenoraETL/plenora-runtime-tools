# Contratti pubblici fissati

`source.json` fissa `3aec9f724f5a0ad21366f293895b4d0dbcd0aad1` e registra
per ogni copia percorso upstream e SHA-256. Le copie sono identiche ai byte
del worktree locale dei contratti, senza normalizzazione dei terminatori.

`adoption-source.json` e il registro sorgente di questa adozione: contratti
selezionati, stato di ogni requisito, limiti, hazard e rientro. Non e un
manifesto v4 di release: mancano artefatti immutabili qualificati e relativi
digest. Questa deviazione non viene nascosta con hash di un checkout.

Il profilo comune resta `plenora-runtime-tools-profile-v1`; al pin non esiste
un catalogo runtime-tools. Public Catalogs 2.0 si applica al consumer dei
cataloghi/capability di dominio. Le copie nuove sono database-tools-v2,
data-tools-v3 e io-tools-v3, con il profilo runtime aggiornato nel contenuto.

`runtime-v1/` e `runtime-v2/` sono la matrice completa di richieste, successi
ed errori. `runtime-negotiation-v1/` contiene le 21 sequenze host;
`runtime-probes-v1/` e inventariato ma resta un gate di conformita aperto.
`capabilities-v2/rest-tools-v1.json` copia l'esempio upstream
`examples/valid/capabilities-rest-v2.json`. `upstream/` contiene il sottoinsieme
normativo e gli schemi; non e un checkout completo del gate dei contratti.

Verifica offline senza dipendenze Python aggiuntive:

```powershell
python scripts/check-contract-adoption.py
python scripts/check-contract-adoption.py --upstream C:/pg/contracts-main
cargo test -p plenora-runtime-capabilities --offline --locked
```

Stati e limiti: [registro contratti](../docs/contract-alignment.md).
Non modificare i vettori: aggiornare pin e hash insieme alle copie originali.

# Contratti pubblici fissati

`source.json` fissa la revisione immutabile di `plenora-contracts` consumata da questo
repository. I file sotto `runtime-v1/` e `capabilities-v2/` sono copie JSON
identiche byte per byte della matrice completa di conformità del runtime a
quella revisione. `plenora-contracts` è un repository pubblico; le copie tengono la CI
offline e riproducibile alla revisione fissata.

`runtime-tools` è il consumer di trasporto descritto dal repository comune,
non uno dei cinque profili di componente di dominio. Per questo non dichiara un
manifesto di adozione di dominio. Le prove di conformità appartengono ai test
pubblici del codec delle capability e sono riportate in
[`docs/contract-alignment.md`](../docs/contract-alignment.md).

Non modificare sul posto un vettore copiato. Aggiornare `source.json` a uno SHA di commit
completo e revisionato, sostituire le copie dei vettori da quella revisione e rieseguire l'intera
suite di qualificazione.

# Bozze catene v2

Specifica normativa locale [CHAIN-PLAN-2.0](../../docs/specs/CHAIN-PLAN-2.0.md).
Schemi Draft 2020-12 in `schemas/`, vettori e oracolo in `fixtures/manifest.json`,
archi da proporre in [proposed-edges.json](proposed-edges.json).

I contesti fixture sono registri sintetici, non discovery dei pacchetti indicati
dalle operazioni. I digest ripetuti sono dati di prova, non digest di release.
`draft` non è un quarto modo di COMPOSITION: indica la ratifica ancora assente;
mode rimane direct/adapter_required/provisional. Nessun arco nuovo è annunciabile
in produzione prima della ratifica e qualifica di descrittori/adapter effettivi.

Il validatore usa solo il registro locale degli schemi; non scarica `$ref`.
Le forme dei documenti upstream si conservano per riferimento autenticato e
digest, senza duplicarle in schemi locali. Il validatore non apre tali risorse.
Le fixture non sono autorizzazioni, cataloghi eseguibili o ricevute di pubblicazione.

Per eseguire i test, dalla radice del repository:

```text
python -m pip install -r scripts/requirements-chains.txt
python -m unittest discover -s scripts -p test_chain_contract.py
```

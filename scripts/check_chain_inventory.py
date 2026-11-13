#!/usr/bin/env python3
"""Valida la copertura delle superfici e la vista documentale del prodotto catene."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SURFACES = ("rust", "cli", "python_sync", "python_async", "runtime")
PROOFS = {"positive", "rejection", "limit", "effect"}


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("membro JSON duplicato")
        result[key] = value
    return result


def read_inventory(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)


def validate(data: dict) -> None:
    if data.get("schema_version") != 1 or data.get("surfaces") != list(SURFACES):
        raise ValueError("versione o superfici dell'inventario non valide")
    features = data.get("features")
    if not isinstance(features, list) or not features:
        raise ValueError("inventario vuoto")
    identifiers = set()
    for feature in features:
        for field in ("id", "name", "owner", "source"):
            if not isinstance(feature.get(field), str) or not feature[field].strip():
                raise ValueError("identità, ownership o fonte mancante")
        if feature["id"] in identifiers:
            raise ValueError("funzionalità duplicata")
        identifiers.add(feature["id"])
        cells = feature.get("surfaces", {})
        if set(cells) != set(SURFACES):
            raise ValueError("ogni funzionalità richiede esattamente cinque celle")
        for cell in cells.values():
            if cell.get("status") != "da fare":
                raise ValueError("P1.1 non attesta implementazioni")
            if not isinstance(cell.get("entrypoint"), str) or not cell["entrypoint"].strip():
                raise ValueError("percorso di prova mancante")
            proofs = cell.get("required_tests", {})
            if set(proofs) != PROOFS or any(
                not isinstance(value, str) or not value.strip() for value in proofs.values()
            ):
                raise ValueError("prova positiva, rifiuto, limite ed effetto obbligatori")


def render(data: dict) -> str:
    lines = [
        "# Inventario delle funzionalità catene", "",
        "Vista generata da [chain-features.json](chain-features.json), versione 1. "
        "Tutte le celle sono **da fare**: sono requisiti di prova, non evidenze ottenute.", "",
        "Fonte: PROGETTO-CATENE del 12 novembre 2026, sezioni indicate per riga. "
        "Perimetro e decisioni: [ADR 0008](adr/0008-chain-product-scope.md). "
        "Le operazioni sono proposte da ratificare e provare sulle discovery effettive.", "",
        "Owner indica la responsabilità della funzionalità; CLI e wheel possiedono "
        "i rispettivi binding secondo il grafo architetturale. Le estensioni database "
        "condizionate restano lavoro esplicito di contratto se necessarie al catalogo "
        "prodotto: non sono promesse della prima release. Parallelismo DAG, "
        "continue-on-error, streaming fra step e backend embedded sono profili "
        "successivi distinti, non opzioni abilitate inizialmente. L’APP mantiene "
        "compilatore canvas e inventario legacy; le righe APP provano il supporto "
        "RT ai piani, non una migrazione automatica.", "",
        "Ogni cella richiede quattro prove: positiva (P), rifiuto (R), limite (L), "
        "effetto (E). Il percorso di ogni superficie si applica separatamente a tutte "
        "e quattro le prove della riga; non basta un test del codec. "
        "I dettagli sono conservati anche in ogni cella JSON.", "",
        "| Superficie | Percorso di prova obbligatorio |", "|---|---|",
    ]
    for surface in SURFACES:
        lines.append(f"| {surface} | {data['features'][0]['surfaces'][surface]['entrypoint']} |")
    lines += ["", "| ID / funzione / owner / fonte | Rust | CLI | SDK sync | SDK async | Runtime |",
              "|---|---|---|---|---|---|"]
    for feature in data["features"]:
        label = f"{feature['id']} — {feature['name']} / `{feature['owner']}` / {feature['source']}"
        lines.append("| " + label + " | " + " | ".join(
            feature["surfaces"][surface]["status"] + " (P/R/L/E)" for surface in SURFACES
        ) + " |")
    lines += ["", "## Prove richieste per riga", "",
              "Ogni requisito seguente va provato su ciascuna delle cinque superfici.", ""]
    for feature in data["features"]:
        lines += [f"### {feature['id']}", "", feature["name"], ""]
        for kind, text in feature["surfaces"]["rust"]["required_tests"].items():
            lines.append(f"- **{kind}**: {text}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    try:
        data = read_inventory(ROOT / "docs/chain-features.json")
        validate(data)
        if (ROOT / "docs/chain-features.md").read_text(encoding="utf-8") != render(data):
            raise ValueError("vista documentale non allineata all'inventario")
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(f"chain inventory: {error}")
        return 1
    print(f"chain inventory: {len(data['features'])} funzionalità, cinque celle complete ciascuna")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

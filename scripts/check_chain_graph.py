#!/usr/bin/env python3
"""Estende i test di architettura al grafo risolto dello strato catene proposto."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def violations(metadata: dict, policy: dict) -> list[str]:
    packages = {package["id"]: package["name"] for package in metadata["packages"]}
    resolve = metadata.get("resolve")
    if not resolve:
        raise ValueError("grafo risolto obbligatorio; --no-deps non ammesso")
    graph = {node["id"]: node["dependencies"] for node in resolve["nodes"]}
    if set(graph) != set(packages) or any(
        dependency not in packages for targets in graph.values() for dependency in targets
    ):
        raise ValueError("nodi del grafo risolto incompleti")
    errors = []
    product = policy["allowed_product_edges"]
    protected = set(policy["generics"]) | set(policy["no_domain"])
    for origin, name in packages.items():
        if name not in protected and name not in product:
            continue
        allowed_reachable = set()
        allowed_pending = list(product.get(name, []))
        while allowed_pending:
            allowed = allowed_pending.pop()
            if allowed not in allowed_reachable:
                allowed_reachable.add(allowed)
                allowed_pending.extend(product[allowed])
        for dependency in graph.get(origin, []):
            target = packages[dependency]
            if target in product and (
                name not in product or target not in product[name]
            ):
                errors.append(f"arco prodotto vietato: {name} -> {target}")
        visited = set()
        pending = list(graph.get(origin, []))
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            target = packages[current]
            if target in product and target not in allowed_reachable:
                errors.append(f"dipendenza prodotto transitiva vietata: {name} -> {target}")
            if current == origin:
                errors.append(f"ciclo: {name}")
            if name in protected and (
                target in policy["domain_packages"]
                or any(target.startswith(prefix) for prefix in policy["domain_prefixes"])
                or (name in policy["generics"] and target in product)
            ):
                errors.append(f"dipendenza transitiva vietata: {name} -> {target}")
            if name in policy["no_infrastructure"] and target in policy["infrastructure_packages"]:
                errors.append(f"infrastruttura concreta vietata: {name} -> {target}")
            pending.extend(graph.get(current, []))
    return sorted(set(errors))


def main() -> int:
    try:
        policy = json.loads((ROOT / "docs/chain-crate-graph.json").read_text(encoding="utf-8"))
        result = subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--all-features", "--locked", "--offline"],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8", check=False,
        )
        if result.returncode:
            print("chain graph: cargo metadata non disponibile; controllo NON eseguito")
            return 1
        errors = violations(json.loads(result.stdout), policy)
        for error in errors:
            print(f"chain graph: {error}")
        if errors:
            return 1
    except (OSError, ValueError, KeyError, TypeError):
        print("chain graph: policy o grafo illeggibile/incompleto")
        return 1
    print("chain graph: nessun arco vietato nel grafo risolto; ammessa l'assenza dei crate proposti")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

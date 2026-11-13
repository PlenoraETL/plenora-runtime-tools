"""Il grafo proposto deve intercettare anche archi nascosti da alias o intermediari."""

import json
import unittest

from check_chain_graph import ROOT, violations


def graph(edges):
    names = set(edges) | {target for targets in edges.values() for target in targets}
    return {
        "packages": [{"id": name + "#1", "name": name} for name in names],
        "resolve": {"nodes": [
            {"id": name + "#1", "dependencies": [target + "#1" for target in edges.get(name, [])]}
            for name in names
        ]},
    }


class ChainGraphTests(unittest.TestCase):
    def setUp(self):
        self.policy = json.loads((ROOT / "docs/chain-crate-graph.json").read_text(encoding="utf-8"))

    def test_planned_edges_and_absent_crates_are_valid(self):
        self.assertEqual([], violations(graph(self.policy["allowed_product_edges"]), self.policy))
        self.assertEqual([], violations(graph({"plenora-runtime-core": []}), self.policy))

    def test_reverse_and_transitive_domain_dependencies_fail(self):
        for target in ("plenora-runtime-chains", "plenora-cli", "arrow-array"):
            with self.subTest(target=target):
                self.assertTrue(violations(graph({
                    "plenora-runtime-core": ["innocent-bridge"], "innocent-bridge": [target],
                }), self.policy))

    def test_engine_store_cycle_and_cross_adapter_fail(self):
        for source, target in (
            ("plenora-runtime-chains", "plenora-runtime-chain-host"),
            ("plenora-runtime-boundary", "tokio-postgres"),
            ("plenora-runtime-chain-store", "plenora-database-engine"),
            ("plenora-runtime-adapter-data", "plenora-runtime-adapter-io"),
            ("plenora-runtime-chains", "plenora-runtime-chains"),
        ):
            with self.subTest(source=source, target=target):
                self.assertTrue(violations(graph({source: [target]}), self.policy))

    def test_unresolved_metadata_fails(self):
        with self.assertRaises(ValueError):
            violations({"packages": [], "resolve": None}, self.policy)

    def test_hidden_product_dependency_and_missing_node_fail(self):
        self.assertTrue(violations(graph({
            "plenora-runtime-adapter-data": ["renamed-bridge"],
            "renamed-bridge": ["plenora-runtime-adapter-rest"],
        }), self.policy))
        broken = graph({"plenora-runtime-core": []})
        broken["resolve"]["nodes"] = []
        with self.assertRaises(ValueError):
            violations(broken, self.policy)

"""Regressioni del gate di completezza, incluse omissioni di ogni superficie."""

import copy
import unittest

from check_chain_inventory import ROOT, SURFACES, read_inventory, render, validate, unique_object


class ChainInventoryTests(unittest.TestCase):
    def setUp(self):
        self.data = read_inventory(ROOT / "docs/chain-features.json")

    def test_repository_inventory_and_view(self):
        validate(self.data)
        self.assertEqual(render(self.data), (ROOT / "docs/chain-features.md").read_text(encoding="utf-8"))

    def test_every_feature_requires_each_surface(self):
        for index in range(len(self.data["features"])):
            for surface in SURFACES:
                with self.subTest(feature=index, surface=surface):
                    broken = copy.deepcopy(self.data)
                    del broken["features"][index]["surfaces"][surface]
                    with self.assertRaises(ValueError):
                        validate(broken)

    def test_empty_proof_and_unexpected_surface_fail(self):
        for kind in ("positive", "rejection", "limit", "effect"):
            broken = copy.deepcopy(self.data)
            broken["features"][0]["surfaces"]["rust"]["required_tests"][kind] = ""
            with self.assertRaises(ValueError):
                validate(broken)
        self.data["features"][0]["surfaces"]["python"] = {}
        with self.assertRaises(ValueError):
            validate(self.data)

    def test_duplicate_or_empty_features_fail(self):
        self.data["features"].append(self.data["features"][0])
        with self.assertRaises(ValueError):
            validate(self.data)
        self.data["features"] = []
        with self.assertRaises(ValueError):
            validate(self.data)
        with self.assertRaises(ValueError):
            unique_object([("rust", {}), ("rust", {})])

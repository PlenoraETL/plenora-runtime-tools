"""Exhaustive facade matrix: a missing, duplicate or unrunnable cell fails closed."""
import copy
import json
import re
import unittest
from generate_surface_matrix import ROOT, OUT, SURFACES, operations, generate


def validate_matrix(matrix):
    expected_functions = re.findall(r'"([a-z_]+)"', (ROOT / 'crates/plenora-runtime-chains/src/protocol.rs').read_text(encoding='utf-8').split('pub const FUNCTIONS:')[1].split('];')[0])
    expected = set(operations()) | {f'RT-{n:03}' for n in range(10, 28)} | {'control:'+f for f in expected_functions}
    if matrix['surfaces'] != SURFACES or matrix['functions'] != expected_functions or set(matrix['operations']) != set(operations()):
        raise ValueError('source enumeration mismatch')
    cases = {c['id']:c for c in matrix['cases']}
    if len(cases) != len(matrix['cases']) or set(cases) != expected:
        raise ValueError('case omitted or duplicated')
    for case in cases.values():
        if proof := case["checks"].get("architecture"):
            if proof["symbol"] not in (ROOT / proof["file"]).read_text(encoding="utf-8"):
                raise ValueError("architecture proof missing")
    actual = []
    for cell in matrix['cells']:
        actual.append((cell['item'], cell['surface']))
        case = cases.get(cell['case'])
        if cell['case'] != cell['item'] or not case or not case['requests'] or len(case['requests']) != len(case['errors']) or case['profile'] not in matrix['profiles']:
            raise ValueError('cell without executable case')
    if len(actual) != len(set(actual)) or set(actual) != {(i,s) for i in expected for s in SURFACES}:
        raise ValueError('incomplete Cartesian product')
    return len(actual)


class SurfaceMatrix(unittest.TestCase):
    def test_sources_and_generated_cases_are_exact(self):
        actual = json.loads(OUT.read_text(encoding='utf-8'))
        validate_matrix(actual)
        self.assertEqual(actual, generate())

    def test_every_missing_combination_and_duplicate_is_rejected(self):
        original = json.loads(OUT.read_text(encoding='utf-8'))
        for index in range(len(original['cells'])):
            changed = dict(original, cells=original['cells'][:index] + original['cells'][index+1:])
            with self.assertRaises(ValueError): validate_matrix(changed)
        changed = dict(original, cells=original['cells']+[original['cells'][0]])
        with self.assertRaises(ValueError): validate_matrix(changed)

    def test_unsupported_target_operations_cannot_be_silently_omitted(self):
        original = json.loads(OUT.read_text(encoding='utf-8'))
        for item in ['database.execute@1','database.transaction.begin@1','storage.copy@1']:
            self.assertIn(item, original['operations'])
            changed = copy.deepcopy(original)
            changed['operations'].remove(item)
            with self.assertRaises(ValueError): validate_matrix(changed)

if __name__ == '__main__': unittest.main()

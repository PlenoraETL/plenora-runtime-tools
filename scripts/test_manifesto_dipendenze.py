"""Regressioni del confine tra qualifica descrittiva e autorizzazione."""
import copy
import unittest
from unittest.mock import patch

from scripts import manifesto_dipendenze as manifesto


class ManifestoTests(unittest.TestCase):
    def test_spdx_conjunction_and_alternatives(self):
        self.assertTrue(manifesto.spdx_allowed('MIT OR GPL-3.0', {'MIT'}))
        self.assertFalse(manifesto.spdx_allowed('MIT AND GPL-3.0', {'MIT'}))
        self.assertFalse(manifesto.spdx_allowed('MIT OR Apache-2.0 AND GPL-3.0', {'Apache-2.0'}))
        self.assertTrue(manifesto.spdx_allowed('Apache-2.0 WITH LLVM-exception',
                                             {'Apache-2.0 WITH LLVM-exception'}))
        for expression in ('MIT OR', '(MIT', 'MIT garbage', 'AND MIT'):
            with self.subTest(expression=expression), self.assertRaises(ValueError):
                manifesto.spdx_allowed(expression, {'MIT'})

    def test_lock_must_be_completely_covered(self):
        meta = {'resolve': {'nodes': []}, 'packages': [], 'workspace_members': []}
        with self.assertRaisesRegex(ValueError, 'metadata non copre'):
            manifesto.qualify(meta, 'fuzz/Cargo.lock', {'allow': []})

    def test_snapshot_tampering_fails_closed(self):
        with patch('sys.argv', ['manifesto', '--snapshot', '0' * 64]):
            self.assertEqual(manifesto.main(), 1)

    def test_serializer_preserves_qualification_fields(self):
        import tomllib
        policy = {'qualification_policy': {'features': 'all'},
                  'qualifications': [{'license_allowed': False, 'features': ['std'],
                                      'source': 'git+https://example.invalid/#fixed'}]}
        self.assertEqual(tomllib.loads(manifesto.render(copy.deepcopy(policy))), policy)

    def decisions(self):
        import json
        return json.loads((manifesto.ROOT / 'scripts/manifesto_deroghe.json').read_text(encoding='utf-8'))

    def test_exact_approvals_do_not_cover_new_parent_or_source(self):
        decisions = self.decisions()
        entry = next(e for g in decisions['crates'] for e in g['entries'] if e['category'] == 'b')
        approved = copy.deepcopy(entry['finding'])
        changed_parent = dict(approved, parent='unreviewed@1.0.0')
        changed_parent['key'] = manifesto.gate.fingerprint(changed_parent)
        changed_source = dict(approved, source='git+https://example.invalid/#new')
        changed_source['key'] = manifesto.gate.fingerprint(changed_source)
        result = manifesto.approved_exceptions([approved, changed_parent, changed_source], decisions)
        self.assertEqual([e['key'] for e in result], [approved['key']])

    def test_io_remains_open_and_regressed_alignment_is_rejected(self):
        decisions = self.decisions()
        entries = [e for g in decisions['crates'] for e in g['entries']]
        residual = [e['finding'] for e in entries if e['category'] == 'c']
        self.assertEqual(manifesto.approved_exceptions(residual, decisions), [])
        regressed = next(e['finding'] for e in entries if e['category'] == 'a')
        with self.assertRaisesRegex(ValueError, 'riallineamento'):
            manifesto.approved_exceptions([regressed], decisions)

    def test_broken_path_and_duplicate_decision_are_rejected(self):
        decisions = self.decisions()
        group = next(g for g in decisions['crates'] if any(e['category'] == 'b' for e in g['entries']))
        entry = next(e for e in group['entries'] if e['category'] == 'b')
        raw = [entry['finding']]
        entry['chain'] = 'missing-parent -> ' + entry['chain'].split(' -> ')[-1]
        with self.assertRaisesRegex(ValueError, 'percorso'):
            manifesto.approved_exceptions(raw, decisions)
        decisions = self.decisions()
        decisions['crates'].append(copy.deepcopy(decisions['crates'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicata'):
            manifesto.approved_exceptions([], decisions)

    def test_expired_approval_is_still_blocked_by_family_gate(self):
        import datetime
        decisions = self.decisions()
        entry = next(e for g in decisions['crates'] for e in g['entries'] if e['category'] == 'b')
        raw = dict(entry['finding'], sample=manifesto.REPO)
        exceptions = manifesto.approved_exceptions([raw], decisions)
        with patch.object(manifesto.gate, 'collect', return_value=([raw], {})):
            result = manifesto.gate.evaluate({'exceptions': exceptions, 'alignments': [],
                                               'baseline': 'test'}, {},
                                              today=datetime.date(2027, 2, 14))
        self.assertEqual(result['counts']['errors'], 1)
        self.assertEqual(result['errors'][0]['error'], 'eccezione_scaduta')


if __name__ == '__main__':
    unittest.main()

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

    def policy(self):
        import tomllib
        return tomllib.loads((manifesto.HOME / 'plenora-dependencies.toml').read_text(encoding='utf-8'))

    def test_deroghe_document_matches_manifest_and_covers_every_entry(self):
        policy = self.policy()
        manifesto.check_deroghe(policy)
        document = manifesto.render_deroghe(policy)
        entries = [e for e in policy['exceptions'] if e['repo'] == manifesto.REPO]
        for entry in entries + policy['open_deviations']:
            self.assertEqual(document.count(entry['key']), 1)
        self.assertEqual(sum(e['rule'] == 'fork' for e in entries), 2)
        self.assertEqual({e['crate'] for e in policy['open_deviations']}, {'encoding_rs', 'zip'})
        self.assertTrue(all(e['removal'] == 'chiude con IO 10.0.0'
                            for e in policy['open_deviations']))
        self.assertTrue({e['key'] for e in entries}.isdisjoint(
            {e['key'] for e in policy['open_deviations']}))
        reordered = copy.deepcopy(policy)
        reordered['exceptions'].reverse()
        reordered['open_deviations'].reverse()
        reordered['qualifications'].reverse()
        self.assertEqual(document, manifesto.render_deroghe(reordered))

    def test_deroghe_rejects_missing_empty_and_invalid_required_fields(self):
        for section in ('exceptions', 'open_deviations'):
            for field in ('reason', 'chain', 'removal'):
                for bad in (None, '', '   ', [], 42):
                    policy = self.policy()
                    entry = next(e for e in policy[section] if e['repo'] == manifesto.REPO)
                    if bad is None:
                        del entry[field]
                    else:
                        entry[field] = bad
                    with self.subTest(section=section, field=field, bad=bad):
                        with self.assertRaisesRegex(ValueError, 'deroga incompleta'):
                            manifesto.render_deroghe(policy)
        for bad in (None, '', '   ', '20261119', '2026-02-30'):
            policy = self.policy()
            policy['decision_date'] = bad
            with self.subTest(date=bad), self.assertRaises(ValueError):
                manifesto.render_deroghe(policy)

    def test_deroghe_main_rejects_tampered_or_missing_document(self):
        from pathlib import Path
        import tempfile
        policy = self.policy()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'docs').mkdir()
            path = root / 'docs/deroghe-dipendenze.md'
            content = manifesto.render_deroghe(policy).encode('utf-8')
            with patch.object(manifesto, 'ROOT', root), patch.object(manifesto, 'generate', return_value=policy), patch('sys.argv', ['manifesto']):
                for changed in (content + b'alterato', content.replace(b'\n', b'\r\n')):
                    path.write_bytes(changed)
                    with patch.object(manifesto.gate, 'evaluate') as evaluate:
                        self.assertEqual(manifesto.main(), 1)
                        evaluate.assert_not_called()
                path.unlink()
                self.assertEqual(manifesto.main(), 1)

    def test_io_paths_are_validated_without_authorizing_exceptions(self):
        decisions = self.decisions()
        entry = next(e for g in decisions['crates'] for e in g['entries'] if e['category'] == 'c')
        entry['chain'] = 'nonexistent -> bogus'
        with self.assertRaisesRegex(ValueError, 'percorso'):
            manifesto.approved_exceptions([entry['finding']], decisions)

    def test_deroghe_escapes_table_cells(self):
        policy = self.policy()
        entry = next(e for e in policy['exceptions'] if e['repo'] == manifesto.REPO)
        entry['reason'] = 'a | b\n<script>'
        self.assertIn('a &#124; b<br>&lt;script&gt;', manifesto.render_deroghe(policy))

    def test_decision_fields_reject_whitespace_before_expansion(self):
        for field in ('reason', 'removal'):
            decisions = self.decisions()
            decisions['crates'][0][field] = '   '
            with self.subTest(field=field), self.assertRaises(ValueError):
                manifesto.approved_exceptions([], decisions)
        decisions = self.decisions()
        decisions['crates'][0]['entries'][0]['chain'] = '   '
        with self.assertRaises(ValueError):
            manifesto.approved_exceptions([], decisions)

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

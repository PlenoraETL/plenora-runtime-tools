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


if __name__ == '__main__':
    unittest.main()

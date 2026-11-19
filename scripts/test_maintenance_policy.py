"""Le campagne richiedono dispatch e le piattaforme proprie sono esplicite."""
from pathlib import Path
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]


class MaintenancePolicyTests(unittest.TestCase):
    def test_campaigns_are_manual_and_smoke_remains_automatic(self):
        # BaseLoader conserva la chiave YAML on come testo.
        ci = yaml.load((ROOT / '.github/workflows/ci.yml').read_text(encoding='utf-8'),
                       Loader=yaml.BaseLoader)
        self.assertIn('pull_request', ci['on'])
        self.assertIn('push', ci['on'])
        self.assertEqual(ci['jobs']['quality']['strategy']['matrix']['os'],
                         ['ubuntu-latest', 'windows-latest'])
        for job in ('coverage', 'nats-integration', 'postgres-integration'):
            self.assertEqual(ci['jobs'][job]['if'], "github.event_name == 'workflow_dispatch'")
        smoke = next(j for j in ci['jobs'].values() if j.get('name') == 'Fuzz smoke suite')
        self.assertNotIn('if', smoke)
        self.assertIn('-runs=2000', str(smoke))
        for name in ('fuzz', 'boundary-confirmation'):
            workflow = yaml.load((ROOT / f'.github/workflows/{name}.yml').read_text(encoding='utf-8'),
                                 Loader=yaml.BaseLoader)
            self.assertEqual(set(workflow['on']), {'workflow_dispatch'})

    def test_platform_specific_crates_reject_unsupported_targets(self):
        for crate in ('core', 'subprocess', 'resources'):
            source = (ROOT / f'crates/plenora-runtime-{crate}/src/lib.rs').read_text(encoding='utf-8')
            self.assertIn('#[cfg(not(any(target_os = "linux", target_os = "windows")))]\ncompile_error!', source)


if __name__ == '__main__':
    unittest.main()

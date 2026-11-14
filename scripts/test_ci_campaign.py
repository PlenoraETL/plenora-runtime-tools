"""Campaign inventory and failure aggregation regressions without infrastructure."""
import copy
from pathlib import Path
import re
import unittest
from ci_campaign import failures
from test_crash_matrix import ci_job

ROOT = Path(__file__).resolve().parents[1]


class Campaign(unittest.TestCase):
    def test_original_failure_skips_missing_and_cancellation_are_never_green(self):
        expected = ['setup', 'migration', 'store', 'installed', 'settlement', 'cleanup']
        good = {name: {'outcome':'success', 'conclusion':'success'} for name in expected}
        self.assertEqual(failures(good,expected), [])
        for name in expected:
            for outcome in ['failure','cancelled','skipped',None]:
                actual = copy.deepcopy(good)
                actual[name]['outcome'] = outcome
                self.assertEqual(failures(actual,expected),[name])
            actual = copy.deepcopy(good)
            del actual[name]
            self.assertEqual(failures(actual,expected),[name])

    def test_real_host_coverage_is_collected_without_lowering_threshold(self):
        workflow = (ROOT/'.github/workflows/ci.yml').read_text(encoding='utf-8')
        coverage = ci_job(workflow, 'coverage')
        for fragment in [
            '--test remote --locked --no-report -- nats_real_scripted --ignored',
            '--test worker --locked --no-report -- postgres_nats_real --ignored',
            '--test transport --locked --no-report -- --ignored',
            '--test database --locked --no-report -- --ignored',
            '--test io --locked --no-report -- --ignored',
            'cargo llvm-cov report --summary-only --fail-under-lines 90',
        ]:
            self.assertIn(fragment, coverage)
        self.assertIn('--test transport --locked -- --ignored', ci_job(workflow, 'postgres-integration'))

    def test_output_references_have_a_producer_in_the_same_job(self):
        workflow = (ROOT/'.github/workflows/ci.yml').read_text(encoding='utf-8')
        for job in ['nats-integration', 'postgres-integration', 'coverage']:
            available = set()
            for part in re.split(r'^      - name: ', ci_job(workflow, job), flags=re.M)[1:]:
                for producer in re.findall(r'steps\.(\w+)\.outputs\.', part):
                    self.assertIn(producer, available, (job, producer))
                name = re.search(r'^        id: (\w+)', part, re.M)
                if name:
                    available.add(name[1])

    def test_all_campaign_steps_are_attempted_and_aggregated(self):
        workflow = (ROOT/'.github/workflows/ci.yml').read_text(encoding='utf-8')
        for job in ['nats-integration','postgres-integration','coverage']:
            parts = re.split(r'^      - name: ',ci_job(workflow,job),flags=re.M)[1:]
            final = parts.pop()
            self.assertIn('if: always()',final)
            self.assertNotIn('continue-on-error:',final)
            expected = re.search(r'run: python scripts/ci_campaign.py (.+)',final)[1].split()
            actual = []
            for part in parts:
                name = re.search(r'^        id: (\w+)',part,re.M)[1]
                self.assertIn('continue-on-error: true',part)
                self.assertIn('if: always()',part)
                if not part.startswith('Collect NATS logs'):
                    actual.append(name)
                if 'cargo test ' in part or ('cargo llvm-cov ' in part and '--no-report' in part):
                    self.assertIn('--no-fail-fast',part)
                # One test invocation per step: a failure cannot suppress the next binary/tool.
                invocations = re.findall(r'(?:cargo test |cargo llvm-cov (?!clean|report)|python -m unittest)',part)
                self.assertLessEqual(len(invocations),1)
            self.assertCountEqual(actual,expected)
            # Inject failure at each preceding step; later successes cannot mask it.
            for failed in actual:
                outcomes = {name:{'outcome':'failure' if name==failed else 'success', 'conclusion':'success'} for name in actual}
                self.assertEqual(failures(outcomes,expected),[failed])

    def test_real_adapter_prerequisites_are_explicit_in_both_campaigns(self):
        for name in ('ci.yml', 'boundary-confirmation.yml'):
            workflow = (ROOT/'.github/workflows'/name).read_text(encoding='utf-8')
            for job in ('postgres-integration', 'coverage'):
                text = ci_job(workflow, job)
                self.assertIn('scripts/postgres-adapter-ci.Dockerfile', text)
                self.assertIn('CREATE EXTENSION postgis', text)
                self.assertRegex(text, r'--example io_fixture --locked -- "\$RUNNER_TEMP/[^"]+"')
            coverage = ci_job(workflow, 'coverage')
            for adapter in ('database', 'io', 'data'):
                self.assertIn(f'--test {adapter} --locked --no-report -- --ignored', coverage)
                self.assertIn(f'campaign_adapter_{adapter}', coverage.split('run: python scripts/ci_campaign.py')[-1])

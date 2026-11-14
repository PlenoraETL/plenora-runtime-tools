"""Counterexamples to same-SHA confirmation without infrastructure or compilation."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
import sys
from unittest.mock import patch

import boundary_confirmation as gate
from generate_boundary_confirmation import generate, OUT
from run_confirmation_tests import passed_tests, capture_command, main as run_tests


class Confirmation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.identity = {'GITHUB_SHA': 'a'*40, 'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}
        self.manifest = gate.inventory()
        self.digest = gate.matrix_digest()
        self.steps = {j: gate.expected_steps(j) for j in gate.JOBS}
        self.labels = {j: gate.expected_logs(j) for j in gate.JOBS}
        self.needs = {j: {'result': 'success'} for j in gate.JOBS}
        for job in gate.JOBS:
            folder = self.directory/job
            folder.mkdir()
            self.write(folder/'job.json', dict(self.identity, job=job, steps={s:'success' for s in self.steps[job]}))
        for rule in self.manifest['rows']:
            for mode in ['rust','cli','python_sync','python_async','remote_sync','remote_async','remote_cli']:
                job = 'quality-linux' if mode == 'rust' else 'postgres-integration'
                self.write(self.directory/job/f'cell-{mode}-{rule}.json', dict(self.identity,
                           matrix_sha256=self.digest, rule=rule, mode=mode, status='passed'))
        tests = {proof['test'] for row in self.manifest['rows'].values() for proof in row['subsystem_proofs']}
        self.write(self.directory/'quality-linux/rust-tests.json', dict(self.identity, exit_code=0, passed=sorted(tests)))
        self.write(self.directory/'coverage/coverage.json', {'data':[{'totals':{'lines':{'percent':90}}}]})
        for job, labels in self.labels.items():
            for label in labels:
                passed = sorted(tests) if label == 'workspace-tests' else ['example']
                (self.directory/job/(label+'.log')).write_text(''.join('test '+test+' ... ok\n' for test in passed))
                name = 'rust-tests.json' if label == 'workspace-tests' else label+'.json'
                self.write(self.directory/job/name, dict(self.identity, exit_code=0, passed=passed))

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value), encoding='utf-8')

    def errors(self):
        # Corpus/source validation has its own mutation tests; reuse it for receipt counterexamples.
        with patch.object(gate, 'inventory', return_value=self.manifest), patch.object(gate, 'matrix_digest', return_value=self.digest), patch.object(gate, 'expected_steps', side_effect=self.steps.__getitem__), patch.object(gate, 'expected_logs', side_effect=self.labels.__getitem__):
            return gate.evaluate(self.directory, self.identity, self.needs)[0]

    def test_complete_evidence_and_generated_workflow(self):
        self.assertEqual(self.errors(), [])
        self.assertEqual(OUT.read_text(encoding='utf-8'), generate())

    def test_every_executed_cell_is_required_including_all_remote_modes(self):
        for path in self.directory.rglob('cell-*.json'):
            original = path.read_text()
            path.unlink()
            self.assertTrue(self.errors(), path.name)
            path.write_text(original)

    def test_wrong_sha_run_attempt_corpus_and_skipped_receipt_fail(self):
        path = next(self.directory.rglob('cell-*.json'))
        original = gate.load(path)
        for key in ['GITHUB_SHA','GITHUB_RUN_ID','GITHUB_RUN_ATTEMPT','matrix_sha256','status']:
            self.write(path, dict(original, **{key:'invalid'}))
            self.assertTrue(self.errors(), key)
        self.write(path, original)

    def test_missing_step_original_failure_and_job_cancellation_fail(self):
        for job in gate.JOBS:
            path = self.directory/job/'job.json'
            original = gate.load(path)
            for step in original['steps']:
                for outcome in ['failure','skipped','cancelled',None]:
                    record = copy.deepcopy(original)
                    if outcome is None:
                        del record['steps'][step]
                    else:
                        record['steps'][step] = outcome
                    self.write(path, record)
                    self.assertTrue(self.errors(), (job, step, outcome))
            self.write(path, original)
            self.needs[job]['result'] = 'cancelled'
            self.assertTrue(self.errors())
            self.needs[job]['result'] = 'success'

    def test_empty_directory_and_duplicate_jobs_fail(self):
        with tempfile.TemporaryDirectory() as empty:
            self.assertTrue(gate.evaluate(Path(empty), self.identity, self.needs)[0])
        folder = self.directory/'duplicate'
        folder.mkdir()
        self.write(folder/'job.json', gate.load(self.directory/'quality-linux/job.json'))
        self.assertTrue(self.errors())

    def test_ignored_filtered_and_failed_tests_are_not_execution_proof(self):
        self.assertEqual(passed_tests('test a ... ignored\ntest b ... FAILED\ntest c ... ok\n'), ['c'])
        path = self.directory/'quality-linux/rust-tests.json'
        record = gate.load(path)
        record['passed'].pop()
        self.write(path, record)
        self.assertTrue(self.errors())

    def test_diagnostics_cannot_interleave_libtest_outcome_lines(self):
        command = ['cargo','test','--','--ignored','--nocapture','--test-threads=1']
        self.assertEqual(capture_command(command), ['cargo','test','--','--ignored','--show-output','--test-threads=1'])
        self.assertEqual(passed_tests('test probe ... diagnostic\nok\n'), [])
        self.assertEqual(passed_tests('test probe ... ok\n\nsuccesses:\nprobe stdout:\ndiagnostic\n'), ['probe'])

    def test_missing_coverage_export_fails(self):
        (self.directory/'coverage/coverage.json').unlink()
        self.assertTrue(self.errors())

    def test_command_artifacts_are_required_and_bound_to_the_log(self):
        for path in self.directory.rglob('*.log'):
            content = path.read_text()
            path.unlink()
            self.assertTrue(self.errors(), path)
            path.write_text(content)
        path = next((self.directory/'coverage').glob('campaign_*.json'))
        record = gate.load(path)
        self.write(path, dict(record, passed=['invented']))
        self.assertTrue(self.errors())

    def test_real_command_exit_and_zero_tests_cannot_be_success(self):
        env = dict(self.identity, PLENORA_CONFIRMATION_DIR=str(self.directory/'command'))
        for line, exit_code, expected in [('test a ... ignored',0,1), ('test a ... ok',1,1), ('test a ... ok',0,0)]:
            command = ['run_confirmation_tests.py','--label','probe','--',sys.executable,'-c',
                       f'print({line!r}); raise SystemExit({exit_code})']
            with patch.dict(os.environ, env), patch.object(sys, 'argv', command):
                self.assertEqual(run_tests(), expected)

    def test_row_omission_addition_and_unproved_closure_fail(self):
        original = gate.load(gate.ROOT/'docs/boundary-evidence.json')
        real_load = gate.load
        for mutation in ['missing', 'new', 'closed']:
            changed = copy.deepcopy(original)
            if mutation == 'missing': del changed['rows']['RT-010']
            elif mutation == 'new': changed['rows']['RT-028'] = changed['rows']['RT-010']
            else: changed['rows']['RT-010']['status'] = 'closed'
            with patch.object(gate, 'load', side_effect=lambda p: changed if Path(p).name == 'boundary-evidence.json' else real_load(p)):
                with self.assertRaises(ValueError): gate.inventory()

    def test_new_adoption_rule_cannot_hide_in_compact_json(self):
        adoption = gate.load(gate.ROOT/'contracts/adoption-source.json')
        adoption['requirements'].append({'rule':'RT-028'})
        real_load = gate.load
        with patch.object(gate, 'load', side_effect=lambda p: adoption if Path(p).name == 'adoption-source.json' else real_load(p)):
            with self.assertRaises(ValueError): gate.inventory()

    def test_corrupt_evidence_still_publishes_failure_report(self):
        path = next(self.directory.rglob('cell-*.json'))
        env = dict(self.identity, CONFIRMATION_NEEDS=json.dumps(self.needs))
        for content in ['[]', '{', '{"status":"passed"}']:
            path.write_text(content)
            with patch.dict(os.environ, env), patch.object(sys, 'argv', ['boundary_confirmation.py','report','--directory',str(self.directory)]):
                self.assertEqual(gate.main(), 1)
            self.assertIn('FAIL', (self.directory/'P2-5.md').read_text())


if __name__ == '__main__':
    unittest.main()

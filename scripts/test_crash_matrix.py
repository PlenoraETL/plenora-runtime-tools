"""Fail-closed enumeration of every source C.4 row and executable layered evidence."""
import copy
import hashlib
import json
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'docs/chain-crash-evidence.json'
GATES = {
    'workspace': ('cargo test --workspace --all-targets --all-features --locked', '', ''),
    'postgres_store': ('cargo test --no-fail-fast -p plenora-runtime-chain-store --locked postgres_real -- --ignored --nocapture --test-threads=1', 'crates/plenora-runtime-chain-store/src/', 'postgres_real'),
    'postgres_delivery': ('cargo test --no-fail-fast -p plenora-runtime-integration-tests --test chain_delivery --locked postgres_real_product -- --ignored --nocapture --test-threads=1', 'tests/integration/tests/chain_delivery.rs', 'postgres_real_product'),
}

INSTALLED = [
    "python -m unittest discover -s crates/plenora-runtime-chain-python/tests -p 'test_*.py'",
    'cargo test --no-fail-fast -p plenora-runtime-chain-host --test remote --test worker --test controls --test database --test io --test data --locked -- --ignored --nocapture',
]


def ci_job(workflow, name):
    match = re.search(r'^  ' + re.escape(name) + r':\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)', workflow, re.M | re.S)
    if not match: raise ValueError('required CI job missing')
    return match[1]


def validate(matrix, read=None):
    read = read or (lambda path: (ROOT/path).read_text(encoding='utf-8'))
    source = read(matrix['source'])
    table = source[source.index('| Punto del crash |'):].strip()
    if hashlib.sha256(table.encode()).hexdigest() != matrix['table_sha256']:
        raise ValueError('source changed without evidence review')
    required = {f'C{i+1:02}': row.split('|')[1].strip()
                for i,row in enumerate(table.splitlines()[2:])}
    points = matrix['points']
    if len(points) != len(required) or {p['id']:p['requirement'] for p in points} != required:
        raise ValueError('missing, repeated or altered crash point')
    workflow = read('.github/workflows/ci.yml')
    real_job = ci_job(workflow, 'postgres-integration')
    local_job = ci_job(workflow, 'quality')
    for point in points:
        kinds = set()
        seen = set()
        for proof in point['proofs']:
            key = (proof['path'], proof['test'])
            if key in seen: raise ValueError('duplicate proof')
            seen.add(key)
            code = read(proof['path'])
            matches = re.findall(r'((?:#\[[^\]]*\]\s*)+)(?:async\s+)?fn\s+' + re.escape(proof['test']) + r'\s*\(',code)
            if len(matches) != 1 or not re.search(r'#\[(?:tokio::)?test(?:\(|\])',matches[0]):
                raise ValueError('proof is not an executable Rust test')
            gate = proof['gate']
            if gate not in GATES or GATES[gate][0] not in (local_job if gate == 'workspace' else real_job):
                raise ValueError('test gate not wired')
            ignored = '#[ignore' in matches[0]
            if (gate == 'workspace') == ignored:
                raise ValueError('ignored/local proof mismatch')
            if not proof['path'].startswith(GATES[gate][1]) or GATES[gate][2] not in proof['test']:
                raise ValueError('real filter does not select test')
            kinds.add('local' if gate == 'workspace' else 'real')
        if kinds != {'local','real'}:
            raise ValueError('crash point lacks local or real subsystem proof')
    for command in INSTALLED:
        if command not in real_job: raise ValueError('installed controls not wired in real job')
    return len(points)


class CrashMatrix(unittest.TestCase):
    def setUp(self):
        self.matrix = json.loads(MANIFEST.read_text(encoding='utf-8'))

    def test_all_source_points_have_local_and_real_executable_proofs(self):
        self.assertEqual(validate(self.matrix),7)

    def test_every_omitted_point_and_every_empty_proof_set_fails(self):
        for index in range(len(self.matrix['points'])):
            missing = copy.deepcopy(self.matrix)
            del missing['points'][index]
            with self.assertRaises(ValueError): validate(missing)
            empty = copy.deepcopy(self.matrix)
            empty['points'][index]['proofs'] = []
            with self.assertRaises(ValueError): validate(empty)

    def test_deleted_test_ignore_gate_and_source_drift_fail(self):
        for gate in GATES:
            missing = copy.deepcopy(self.matrix)
            for point in missing['points']:
                point['proofs'] = [p for p in point['proofs'] if p['gate'] != gate]
            with self.assertRaises(ValueError): validate(missing)
        for command, _, _ in GATES.values():
            def removed_job(path):
                value = (ROOT/path).read_text(encoding='utf-8')
                return value.replace(command, '') if path.endswith('ci.yml') else value
            with self.assertRaises(ValueError): validate(self.matrix, removed_job)
        missing = copy.deepcopy(self.matrix)
        missing['points'][0]['proofs'][0]['test'] = 'nonexistent_proof'
        with self.assertRaises(ValueError): validate(missing)
        changed = copy.deepcopy(self.matrix)
        changed['table_sha256'] = '0'*64
        with self.assertRaises(ValueError): validate(changed)
        for command in INSTALLED:
            def removed_real_command(path):
                value = (ROOT/path).read_text(encoding='utf-8')
                if path.endswith('ci.yml'):
                    real = ci_job(value, 'postgres-integration')
                    # Retain any same command in the non-infrastructure quality job.
                    value = value.replace(real, real.replace(command, ''))
                return value
            with self.assertRaises(ValueError): validate(self.matrix, removed_real_command)

"""Same-run evidence gate; facade coverage never implies full D conformance."""
import argparse
import hashlib
from functools import cache
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
MATRIX = 'crates/plenora-runtime-chain-host/tests/fixtures/surfaces.json'
SURFACES = ('rust', 'cli', 'python_sync', 'python_async', 'runtime')
JOBS = ('quality-linux', 'quality-windows', 'anti-panic', 'nats-integration',
        'postgres-integration', 'coverage')


def load(path):
    value = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('expected evidence object')
    return value


@cache
def definitions():
    names = {}
    for folder in ('crates', 'adapters', 'examples', 'tests'):
        for path in (ROOT/folder).rglob('*.rs'):
            for name in re.findall(r'\bfn\s+(\w+)\s*\(', path.read_text(encoding='utf-8')):
                names.setdefault(name, []).append(path.relative_to(ROOT).as_posix())
    return names


def inventory():
    source = (ROOT / 'docs/specs/BOUNDARY-REGISTER-D.md').read_text(encoding='utf-8')
    ids = re.findall(r'^\| (RT-\d{3}) ', source, re.M)
    # The adoption register independently detects new common rules beyond the project snapshot.
    adoption = load(ROOT / 'contracts/adoption-source.json')
    required = set(ids) | {r['rule'] for r in adoption['requirements']
                           if re.fullmatch(r'RT-\d{3}', r['rule']) and int(r['rule'][3:]) >= 10}
    manifest = load(ROOT / 'docs/boundary-evidence.json')
    if len(ids) != len(set(ids)) or set(manifest['rows']) != required:
        raise ValueError('D register omitted or duplicated')
    if manifest['source_sha256'] != hashlib.sha256(source.encode()).hexdigest():
        raise ValueError('D source changed without evidence review')
    matrix = load(ROOT / MATRIX)
    cells = [(c['item'], c['surface']) for c in matrix['cells'] if c['item'].startswith('RT-')]
    if len(cells) != len(set(cells)) or set(cells) != {(r, s) for r in required for s in SURFACES}:
        raise ValueError('D surface enumeration mismatch')
    for row in manifest['rows'].values():
        if row['status'] != 'open' or not row['residual']:
            raise ValueError('closure without full D evidence: facade receipts cannot close a rule')
        if not row['subsystem_proofs']:
            raise ValueError('missing relevant subsystem proof')
        for proof in row['subsystem_proofs']:
            if definitions().get(proof['test']) != [proof['path']]:
                raise ValueError('ambiguous subsystem test identity')
            code = (ROOT/proof['path']).read_text(encoding='utf-8')
            if not re.search(r'#\[(?:tokio::)?test\]\s*(?:async )?fn '+re.escape(proof['test'])+r'\s*\(', code):
                raise ValueError('subsystem proof is not an executable test')
    return manifest


def identity():
    values = {k: os.environ[k] for k in ('GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT')}
    if not re.fullmatch(r'[0-9a-f]{40}', values['GITHUB_SHA']):
        raise ValueError('noncanonical SHA')
    return values


def job_section(job):
    workflow = (ROOT/'.github/workflows/boundary-confirmation.yml').read_text(encoding='utf-8')
    section = re.search(r'^  '+re.escape(job)+r':\n(.*?)(?=^  [a-z][a-z0-9-]*:|\Z)', workflow, re.M | re.S)
    return section[1]


def expected_steps(job):
    return set(re.findall(r'^        id: (\w+)', job_section(job), re.M))


def expected_logs(job):
    section = job_section(job)
    labels = re.findall(r'run_confirmation_tests.py --label (\w+) --', section)
    if 'run: python scripts/run_confirmation_tests.py\n' in section:
        labels.append('workspace-tests')
    return labels


def matrix_digest():
    return hashlib.sha256((ROOT/MATRIX).read_text(encoding='utf-8').encode()).hexdigest()


def collect(destination, job):
    if job not in JOBS:
        raise ValueError('unknown job')
    steps = json.loads(os.environ['CAMPAIGN_STEPS'])
    record = dict(identity(), job=job, steps={k: v['outcome'] for k, v in steps.items()})
    destination.mkdir(parents=True, exist_ok=True)
    (destination / 'job.json').write_text(json.dumps(record, indent=2)+'\n', encoding='utf-8')


def evaluate(directory, expected_identity, needs):
    manifest = inventory()
    digest = matrix_digest()
    errors, jobs, receipts = [], {}, []
    folders = {}
    for path in sorted(directory.rglob('job.json')):
        record = load(path)
        job = record['job']
        if job in jobs or job not in JOBS:
            errors.append('duplicate or unknown job')
        jobs[job] = record
        folders[job] = path.parent
        if any(record.get(k) != v for k, v in expected_identity.items()):
            errors.append('job identity mismatch: '+job)
        if job not in JOBS or not expected_steps(job).issubset(record['steps']) or any(v != 'success' for v in record['steps'].values()):
            errors.append('unsuccessful steps: '+job)
        for proof in sorted(path.parent.glob('cell-*.json')):
            receipt = load(proof)
            if any(receipt.get(k) != v for k, v in expected_identity.items()):
                errors.append('receipt identity mismatch: '+job)
                continue
            if receipt.get('matrix_sha256') != digest:
                errors.append('receipt corpus mismatch: '+job)
                continue
            receipts.append((job, receipt))
    for job in JOBS:
        if job not in jobs or needs.get(job, {}).get('result') != 'success':
            errors.append('missing or unsuccessful job: '+job)
        for label in expected_logs(job):
            folder = folders.get(job, directory/'missing')
            log = folder/(label+'.log')
            record = folder/('rust-tests.json' if label == 'workspace-tests' else label+'.json')
            if not log.exists() or not record.exists():
                errors.append('missing command evidence: '+job+'/'+label)
                continue
            execution = load(record)
            passed = sorted(re.findall(r'^test ([\w:]+) \.\.\. ok\s*$', log.read_text(encoding='utf-8'), re.M))
            if (any(execution.get(k) != v for k, v in expected_identity.items())
                    or execution.get('exit_code') != 0 or not passed or execution.get('passed') != passed):
                errors.append('invalid command evidence: '+job+'/'+label)
    coverage_path = folders.get('coverage', directory/'missing')/'coverage.json'
    if not coverage_path.exists() or not load(coverage_path).get('data'):
        errors.append('missing complete coverage export')
    record_path = folders.get('quality-linux', directory/'missing')/'rust-tests.json'
    observed = load(record_path) if record_path.exists() else {}
    rows = []
    for rule, metadata in sorted(manifest['rows'].items()):
        if (any(observed.get(k) != v for k, v in expected_identity.items())
                or observed.get('exit_code') != 0):
            errors.append('missing successful Rust suite evidence: '+rule)
        for proof in metadata['subsystem_proofs']:
            if sum(t.split('::')[-1] == proof['test'] for t in observed.get('passed', [])) != 1:
                errors.append('missing executed subsystem proof: '+rule+'/'+proof['test'])
        for surface in SURFACES:
            modes = ['remote_sync', 'remote_async', 'remote_cli'] if surface == 'runtime' else [surface]
            job = 'quality-linux' if surface == 'rust' else 'postgres-integration'
            matched = [r for j, r in receipts if j == job and r.get('rule') == rule
                       and r.get('mode') in modes and r.get('status') == 'passed']
            if {r['mode'] for r in matched} != set(modes):
                errors.append('missing executed proof: '+rule+'/'+surface)
            rows.append((rule, surface, 'executed' if {r['mode'] for r in matched} == set(modes) else 'MISSING', metadata['residual']))
    return errors, rows, jobs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['inventory', 'collect', 'report'])
    parser.add_argument('--directory', type=Path, default=ROOT/'target/p25-evidence')
    parser.add_argument('--job', choices=JOBS)
    args = parser.parse_args()
    if args.action == 'inventory':
        inventory()
        return 0
    if args.action == 'collect':
        collect(args.directory, args.job)
        return 0
    try:
        errors, rows, jobs = evaluate(args.directory, identity(), json.loads(os.environ['CONFIRMATION_NEEDS']))
    except (ValueError, KeyError, TypeError, OSError, AttributeError, IndexError):
        errors, rows, jobs = ['Invalid or unreadable evidence; confirmation rejected.'], [], {}
    report = ['# P2.5 integrated boundary confirmation', '',
              'Identity: '+json.dumps(identity(), sort_keys=True), '',
              'Enumeration/execution gate: '+('FAIL' if errors else 'PASS'), '',
              'Full P2.5 qualification: OPEN (see residual requirements).', '',
              'Executed facade probes and subsystem campaigns; all full D claims remain OPEN.', '',
              '| Rule | Surface | Proof | Residual / closure requirement |', '|---|---|---|---|']
    report += ['| '+' | '.join(row)+' |' for row in rows]
    if rows:
        report += ['', '## Required subsystem observations', '']
        for rule, metadata in sorted(inventory()['rows'].items()):
            report += ['- '+rule+': '+', '.join(p['path']+'::'+p['test'] for p in metadata['subsystem_proofs'])]
    report += ['', '## Campaign outcomes', '', '```json', json.dumps(jobs, indent=2), '```', '', '## Failures', '']
    report += ['- '+e for e in errors] or ['None.']
    args.directory.mkdir(parents=True, exist_ok=True)
    (args.directory/'P2-5.md').write_text('\n'.join(report)+'\n', encoding='utf-8')
    (args.directory/'result.json').write_text(json.dumps({'identity': identity(), 'errors': errors, 'cells': rows}, indent=2)+'\n', encoding='utf-8')
    print('\n'.join(errors) if errors else 'All executed cells and campaign jobs accounted for; residuals remain open.')
    return int(bool(errors))


if __name__ == '__main__':
    sys.exit(main())

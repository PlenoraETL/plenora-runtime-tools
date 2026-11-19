#!/usr/bin/env python3
"""Genera e verifica la qualifica completa: lock + cargo metadata --all-features.

La baseline di famiglia resta immutabile. --write e una proposta revisionabile,
mai un passo CI; il default confronta gli artefatti senza aggiornarli.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
HOME = ROOT / '.ci/plenora-dependencies'
sys.path.insert(0, str(HOME))
import controlla_dipendenze as gate

REPO = 'plenora-runtime-tools'
BASELINE_SHA = '09f47749e59fdf518e49386975b2f58fb8a61b9ade91da7db3136ab5f3bc78d0'
DRIVERS = {'io': 'plenora-runtime-adapter-io',
           'data': 'plenora-runtime-adapter-data',
           'database': 'plenora-runtime-adapter-database',
           'boundary': 'plenora-runtime-boundary'}
# Pin di prodotto: docs/adr/0011-database-chain-adapter.md,
# docs/adr/0012-io-chain-adapter.md e docs/adr/0013-data-chain-adapter.md.
GIT_SOURCES = {
    'git+https://github.com/PlenoraETL/plenora-IO-tools.git?rev=9c087aa7daf5d9ad10843ebf3027ae8858f66b94#9c087aa7daf5d9ad10843ebf3027ae8858f66b94',
    'git+https://github.com/PlenoraETL/plenora-data-tools.git?rev=eba11205df585469a154de3ad76ab22a0cb9cad6#eba11205df585469a154de3ad76ab22a0cb9cad6',
    'git+https://github.com/PlenoraETL/plenora-database-tools.git?rev=f4fdb9b5f09d8babe795f68e894949693aa9d0a1#f4fdb9b5f09d8babe795f68e894949693aa9d0a1',
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def spdx_allowed(expression, allowed):
    """Fail closed; AND precede OR, WITH mantiene l'eccezione nell'identita."""
    tokens = re.findall(r'\(|\)|[^\s()]+', expression.replace('/', ' OR '))
    pos = 0

    def atom():
        nonlocal pos
        if pos >= len(tokens):
            raise ValueError('SPDX incompleto')
        token = tokens[pos]
        pos += 1
        if token == '(':
            value = disjunction()
            if pos >= len(tokens) or tokens[pos] != ')':
                raise ValueError('SPDX parentesi')
            pos += 1
            return value
        if token in ('AND', 'OR', ')', 'WITH'):
            raise ValueError('SPDX operatore')
        if pos < len(tokens) and tokens[pos] == 'WITH':
            pos += 1
            if pos >= len(tokens):
                raise ValueError('SPDX eccezione')
            token += ' WITH ' + tokens[pos]
            pos += 1
        return token in allowed

    def conjunction():
        nonlocal pos
        value = atom()
        while pos < len(tokens) and tokens[pos] == 'AND':
            pos += 1
            right = atom()
            value = value and right
        return value

    def disjunction():
        nonlocal pos
        value = conjunction()
        while pos < len(tokens) and tokens[pos] == 'OR':
            pos += 1
            right = conjunction()
            value = value or right
        return value

    value = disjunction()
    if pos != len(tokens):
        raise ValueError('SPDX non riconosciuto')
    return value


def toml_value(value):
    if isinstance(value, dict):
        return '{ ' + ', '.join(json.dumps(k) + ' = ' + toml_value(v) for k, v in value.items()) + ' }'
    if isinstance(value, list):
        return '[' + ', '.join(toml_value(v) for v in value) + ']'
    return json.dumps(value, ensure_ascii=False)


def render(policy):
    lines = ['# Generato da scripts/manifesto_dipendenze.py --write; revisionare il diff.']
    for key, value in policy.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            for item in value:
                lines += ['', '[[' + key + ']]']
                lines += [k + ' = ' + toml_value(v) for k, v in item.items()]
        else:
            # Dotted keys impediscono che scalari successivi entrino nell'ultima tabella.
            # Tutte le sezioni scalari precedono gli array di tabelle nella baseline.
            lines.append(key + ' = ' + toml_value(value))
    return '\n'.join(lines) + '\n'


def metadata(manifest):
    result = subprocess.run(['cargo', 'metadata', '--locked', '--all-features',
                             '--format-version', '1', '--manifest-path', str(ROOT / manifest)],
                            capture_output=True)
    if result.returncode:
        raise ValueError('cargo metadata fallito: ' + manifest)
    return json.loads(result.stdout)


def qualify(meta, lock, deny):
    nodes = {n['id']: n for n in meta['resolve']['nodes']}
    packages = {p['id']: p for p in meta['packages']}
    scopes = {}
    for driver, name in DRIVERS.items():
        starts = [p['id'] for p in packages.values() if p['name'] == name]
        if lock == 'Cargo.lock' and not starts:
            raise ValueError('driver assente: ' + driver)
        seen, todo = set(), starts
        while todo:
            ident = todo.pop()
            if ident in seen:
                continue
            seen.add(ident)
            todo.extend(nodes[ident]['dependencies'])
        scopes[driver] = seen
    locked = tomllib.loads((ROOT / lock).read_text(encoding='utf-8'))['package']
    identities = {(p['name'], p['version'], p.get('source') or 'path') for p in packages.values()}
    if identities != {(p['name'], p['version'], p.get('source', 'path')) for p in locked}:
        raise ValueError('metadata non copre il lock completo: ' + lock)
    rows = []
    members = tomllib.loads((ROOT / 'Cargo.toml').read_text(encoding='utf-8'))['workspace']['members']
    own = {p.resolve() for pattern in members for p in ROOT.glob(pattern)}
    for ident, p in sorted(packages.items(), key=lambda pair: (pair[1]['name'], pair[1]['version'])):
        if ident in meta['workspace_members'] or (not p['source'] and
                Path(p['manifest_path']).parent.resolve() in own):
            continue
        source = p['source'] or 'path:' + Path(p['manifest_path']).parent.relative_to(ROOT).as_posix()
        if p['source'] and p['source'] not in {gate.REGISTRY, *GIT_SOURCES}:
            raise ValueError('provenienza esterna non ammessa')
        expression = p['license'] or ''
        clarify = next((c for c in deny.get('clarify', []) if c['crate'] == p['name']), None)
        if clarify:
            expression = clarify['expression']
        allowed = set(deny['allow'])
        for exception in deny.get('exceptions', []):
            if exception['crate'] == p['name']:
                allowed.update(exception['allow'])
        private_ignored = (deny.get('private', {}).get('ignore', False)
                           and p.get('publish') == [] and p['source'] != gate.REGISTRY)
        drivers = sorted(d for d, ids in scopes.items() if ident in ids)
        parents = []
        for parent_id, node in nodes.items():
            for edge in node['deps']:
                if edge['pkg'] == ident:
                    parent = packages[parent_id]
                    requirements = sorted({d['req'] for d in parent['dependencies'] if d['name'] == p['name']})
                    parents.append(parent['name'] + '@' + parent['version'] + ': ' + ', '.join(requirements))
        lp = next(x for x in locked if (x['name'], x['version'], x.get('source', 'path')) ==
                  (p['name'], p['version'], p['source'] or 'path'))
        rows.append(dict(repo=REPO, file=lock, crate=p['name'], version=p['version'], source=source,
                         checksum=lp.get('checksum', ''), license_declared=p['license'] or '',
                         license=expression, features=sorted(nodes[ident]['features']),
                         license_private_ignored=private_ignored,
                         license_file_sha256=(sha(Path(p['manifest_path']).parent / p['license_file'])
                                              if p.get('license_file') else ''),
                         license_allowed=bool(expression) and spdx_allowed(expression, allowed),
                         drivers=drivers, parents=sorted(parents),
                         reason=('Supporto driver ' + ', '.join(drivers) if drivers else
                                 'Supporto workspace/test/fuzz') + '; archi Cargo qualificati in parents'))
    return rows


def generate():
    baseline = HOME / 'family-baseline.toml'
    if sha(baseline) != BASELINE_SHA:
        raise ValueError('baseline famiglia modificata')
    policy = tomllib.loads(baseline.read_text(encoding='utf-8'))
    deny = tomllib.loads((ROOT / 'deny.toml').read_text(encoding='utf-8'))['licenses']
    policy['baseline'] += '-runtime-2026-11-19'
    policy['decision_date'] = '2026-11-19'
    # Inseriti prima degli array di tabelle per mantenere lo scope TOML radice.
    policy = {'qualification_policy': {'license_policy': 'deny.toml', 'sha256': sha(ROOT / 'deny.toml'),
                                      'features': 'workspace --all-features; fuzz --all-features'}, **policy}
    qualifications = []
    for manifest, lock in [('Cargo.toml', 'Cargo.lock'), ('fuzz/Cargo.toml', 'fuzz/Cargo.lock')]:
        qualifications.extend(qualify(metadata(manifest), lock, deny))
    # La qualifica descrive il grafo, non concede deroghe ne cambia i riferimenti.
    # Ogni scostamento resta rosso nel guardiano fino a una decisione esplicita.
    raw, _ = gate.collect(policy, {REPO: ROOT})
    raw_keys = {f['key'] for f in raw}
    # Rimuovere deroghe cessate non autorizza alcuno scostamento nuovo.
    policy['exceptions'] = [e for e in policy['exceptions']
                            if e['repo'] != REPO or e['key'] in raw_keys]
    policy['qualifications'] = qualifications
    return policy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--snapshot', metavar='SHA256')
    args = parser.parse_args()
    os.environ.update(CARGO_BUILD_JOBS='4', CARGO_INCREMENTAL='0')
    try:
        if args.snapshot is not None:
            if args.write or sha(HOME / 'snapshot.sha256') != args.snapshot:
                raise ValueError('snapshot non corrispondente')
            expected = {'plenora-dependencies.toml', 'plenora-dependencies.sha256',
                        'controlla_dipendenze.py', 'family-baseline.toml'}
            rows = [row.split() for row in (HOME / 'snapshot.sha256').read_text(encoding='ascii').splitlines()]
            if len(rows) != len(expected) or any(len(row) != 2 for row in rows):
                raise ValueError('snapshot non valido')
            if {row[1] for row in rows} != expected:
                raise ValueError('snapshot incompleto')
            if any(sha(HOME / name) != digest for digest, name in rows):
                raise ValueError('snapshot modificato')
            print('Snapshot verificato')
            return 0
        policy = generate()
        content = render(policy)
        # Controllo del serializzatore e dello scope delle sezioni.
        if tomllib.loads(content) != policy:
            raise ValueError('serializzazione incoerente')
        path = HOME / 'plenora-dependencies.toml'
        if args.write:
            path.write_text(content, encoding='utf-8', newline='\n')
            (HOME / 'plenora-dependencies.sha256').write_text(sha(path) + '  ' + path.name + '\n', encoding='ascii', newline='\n')
            files = ['plenora-dependencies.toml', 'plenora-dependencies.sha256',
                     'controlla_dipendenze.py', 'family-baseline.toml']
            (HOME / 'snapshot.sha256').write_text(
                ''.join(sha(HOME / f) + '  ' + f + '\n' for f in files),
                encoding='ascii', newline='\n')
        elif path.read_text(encoding='utf-8') != content:
            raise ValueError('qualifica obsoleta: rigenerare e revisionare il diff')
        denied = [q for q in policy['qualifications'] if not q['license_allowed'] and not q['license_private_ignored']]
        report = gate.evaluate(policy, {REPO: ROOT})
        print('qualifiche:', len(policy['qualifications']), 'licenze da decidere:', len(denied),
              'gate:', report['counts'])
        for q in denied:
            print('LICENZA DA DECIDERE:', q['crate'], q['version'], q['license'])
        return int(bool(denied or report['counts']['errors']))
    except (OSError, ValueError, KeyError, TypeError) as error:
        # Solo errori applicativi controllati, senza stderr Cargo o payload esterni.
        print('Qualifica non verificabile:', type(error).__name__)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Genera e verifica la qualifica completa: lock + cargo metadata --all-features.

La baseline di famiglia resta immutabile. --write e una proposta revisionabile,
mai un passo CI; il default confronta gli artefatti senza aggiornarli.
"""
from __future__ import annotations

import argparse
import datetime
import html
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


def required_text(record, field):
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError('deroga incompleta: ' + field)
    return value


def render_deroghe(policy):
    """Registro derivato esclusivamente dal manifesto, senza clock o accessi Cargo."""
    date = required_text(policy, 'decision_date')
    if datetime.date.fromisoformat(date).isoformat() != date:
        raise ValueError('data di registrazione non canonica')
    lines = [
        '# Deroghe del manifesto dipendenze', '',
        '<!-- Generato da scripts/manifesto_dipendenze.py --write; non modificare a mano. -->', '',
        'Regola: una versione per crate. La deroga e ammessa solo quando la versione',
        'vecchia e imposta da una dipendenza esterna senza versione compatibile, oppure',
        'per un fork. Ogni deroga deve avere motivazione, percorso nel grafo e condizione',
        'di uscita; una deroga priva di questi campi e rifiutata dal guardiano.', '',
        'Fonte: [manifesto](../.ci/plenora-dependencies/plenora-dependencies.toml).',
        'La data registra la decisione nel manifesto runtime, non la generazione.',
        'Il titolare ha accettato 68 nuove deroghe per archi incompatibili e due fork',
        'il 19/11/2026. Il registro comprende le deroghe tuttora presenti nel manifesto,',
        'incluse quelle ereditate dalla baseline; le deroghe cessate vengono rimosse.',
        'Le versioni elencate sono quelle qualificate nel lock indicato; la linea',
        'preferita e il riferimento di famiglia. Nuovi archi richiedono una nuova decisione.', '',
    ]

    def cell(value):
        return (html.escape(value, quote=False).replace('|', '&#124;')
                .replace('\r\n', '\n').replace('\r', '\n').replace('\n', '<br>'))

    def table(entries):
        for entry in entries:
            for field in ('crate', 'version', 'file', 'key', 'reason', 'chain', 'removal'):
                required_text(entry, field)
            chain = entry['chain'].split(' -> ')
            if len(chain) < 2 or any(not part.strip() for part in chain):
                raise ValueError('percorso deroga incompleto')
        lines.extend([
            '| Crate | Versioni nel lock; linea preferita | Chi impone la versione (percorso) | Motivazione | Condizione di uscita | Data di registrazione |',
            '| --- | --- | --- | --- | --- | --- |',
        ])
        for entry in sorted(entries, key=lambda e: (e['crate'], e['file'], e['key'])):
            versions = sorted({q['version'] for q in policy['qualifications']
                               if q['repo'] == REPO and q['file'] == entry['file']
                               and q['crate'] == entry['crate']} | {entry['version']})
            values = [entry['crate'] + ' (' + entry['file'] + '; ' + entry['key'] + ')',
                      ', '.join(versions) + '; preferita: ' + policy['preferred'].get(entry['crate'], 'identita fork distinta'),
                      entry['chain'], entry['reason'], entry['removal'], date]
            lines.append('| ' + ' | '.join(cell(v) for v in values) + ' |')
        lines.append('')

    entries = [e for e in policy['exceptions'] if e['repo'] == REPO]
    for title, rules in [('Versioni duplicate accettate', {'duplicate'}),
                         ('Fork accettati', {'fork'}),
                         ('Altre deroghe ereditate dalla baseline', {'reference'})]:
        lines.extend(['## ' + title, ''])
        table([e for e in entries if e['rule'] in rules])
    if any(e['rule'] not in {'duplicate', 'fork', 'reference'} for e in entries):
        raise ValueError('tipo deroga non documentato')
    lines.extend(['## Scostamenti imposti da IO', '',
                  'Restano errori bloccanti senza deroga; ciascuno **chiude con IO 10.0.0**.', ''])
    table(policy['open_deviations'])
    return '\n'.join(lines)


def check_deroghe(policy):
    expected = render_deroghe(policy).encode('utf-8')
    if (ROOT / 'docs/deroghe-dipendenze.md').read_bytes() != expected:
        raise ValueError('registro deroghe obsoleto: rigenerare e revisionare il diff')


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


def approved_exceptions(raw, decisions):
    """Espande solo gli archi deliberati, mai i nuovi rilievi del guardiano."""
    current = {f['key']: f for f in raw}
    seen, crates, result = set(), set(), []
    for group in decisions['crates']:
        for field in ('crate', 'reason', 'removal'):
            required_text(group, field)
        if group['crate'] in crates:
            raise ValueError('decisione duplicata o incompleta')
        crates.add(group['crate'])
        for entry in group['entries']:
            required_text(entry, 'chain')
            f = entry['finding']
            key = f['key']
            category = entry['category']
            if (key in seen or key != gate.fingerprint(f) or f['repo'] != REPO
                    or f['crate'] != group['crate']
                    or f['file'] not in ('Cargo.lock', 'fuzz/Cargo.lock')
                    or (category, f['rule']) not in {('a', 'duplicate'), ('b', 'duplicate'),
                                                   ('c', 'reference'), ('fork', 'fork')}):
                raise ValueError('decisione non valida')
            seen.add(key)
            actual = current.get(key)
            if actual is None:
                continue
            if {k: v for k, v in actual.items() if k != 'sample'} != f:
                raise ValueError('identita decisione cambiata')
            if category == 'a':
                raise ValueError('riallineamento compatibile non applicato')
            packages = tomllib.loads((ROOT / f['file']).read_text(encoding='utf-8'))['package']
            parents = gate.graph(packages)
            labels = {gate.label(p): p for p in packages}
            chain = entry['chain'].split(' -> ')
            target = dict(name=f['crate'], version=f['version'], source=f['source'])
            if (len(labels) != len(packages) or len(chain) < 2
                    or chain[-1] != gate.label(target)
                    or any(label not in labels for label in chain)
                    or labels[chain[0]].get('source') is not None
                    or (f['rule'] == 'duplicate' and chain[-2] != f['parent'])):
                raise ValueError('percorso decisione non valido')
            for parent, child in zip(chain, chain[1:]):
                if labels[parent] not in parents[gate.identity(labels[child])]:
                    raise ValueError('arco decisione assente')
            if category == 'c':
                continue
            result.append(dict(f, id='E-' + key, owner=decisions['owner'],
                               reason=group['reason'], chain=entry['chain'],
                               profiles=['normal', 'build', 'dev'],
                               target='tutti i target del lock; attivazione non determinata',
                               feature_scope='lock completo, nessuna nuova attivazione',
                               review_by=decisions['review_by'], removal=group['removal'],
                               upstream='scripts/manifesto_deroghe.json; requisiti genitore: '
                                        + ', '.join(entry['requirements'])
                                        + '; baseline famiglia immutabile del 2026-11-19'))
    return result


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
    # Decisioni per crate e archi esatti: docs/dependency-policy.md.
    # I nuovi rilievi restano rossi; nessuna deroga automatica per nome di crate.
    raw, _ = gate.collect(policy, {REPO: ROOT})
    raw_keys = {f['key'] for f in raw}
    # Rimuovere deroghe cessate non autorizza alcuno scostamento nuovo.
    policy['exceptions'] = [e for e in policy['exceptions']
                            if e['repo'] != REPO or e['key'] in raw_keys]
    decisions = json.loads((ROOT / 'scripts/manifesto_deroghe.json').read_text(encoding='utf-8'))
    additions = approved_exceptions(raw, decisions)
    if {e['key'] for e in additions} & {e['key'] for e in policy['exceptions']}:
        raise ValueError('deroga gia presente nella baseline')
    policy['exceptions'].extend(additions)
    # Scostamenti descrittivi: non entrano mai nelle eccezioni autorizzate.
    policy['open_deviations'] = [
        dict(entry['finding'], reason=group['reason'], chain=entry['chain'],
             removal=group['removal'])
        for group in decisions['crates'] for entry in group['entries']
        if entry['category'] == 'c' and entry['finding']['key'] in raw_keys
    ]
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
        document = render_deroghe(policy)
        # Controllo del serializzatore e dello scope delle sezioni.
        if tomllib.loads(content) != policy:
            raise ValueError('serializzazione incoerente')
        path = HOME / 'plenora-dependencies.toml'
        if args.write:
            (ROOT / 'docs/deroghe-dipendenze.md').write_text(document, encoding='utf-8', newline='\n')
            path.write_text(content, encoding='utf-8', newline='\n')
            (HOME / 'plenora-dependencies.sha256').write_text(sha(path) + '  ' + path.name + '\n', encoding='ascii', newline='\n')
            files = ['plenora-dependencies.toml', 'plenora-dependencies.sha256',
                     'controlla_dipendenze.py', 'family-baseline.toml']
            (HOME / 'snapshot.sha256').write_text(
                ''.join(sha(HOME / f) + '  ' + f + '\n' for f in files),
                encoding='ascii', newline='\n')
        elif path.read_text(encoding='utf-8') != content:
            raise ValueError('qualifica obsoleta: rigenerare e revisionare il diff')
        check_deroghe(policy)
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

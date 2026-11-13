"""Generate facade qualification doubles, never provider/library attestations."""
from pathlib import Path
import copy
import json
import hashlib

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / 'crates/plenora-runtime-chain-host/tests/fixtures'
OUT = FIX / 'surfaces.json'
SURFACES = ['rust', 'cli', 'python_sync', 'python_async', 'runtime']

def encode(v):
    return json.dumps(v, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

def digest(v):
    return hashlib.sha256(encode(v).encode()).hexdigest()

def operations():
    result = {}
    for path in sorted((ROOT / 'contracts/upstream/catalogs').glob('*.json')):
        for op in json.loads(path.read_text(encoding='utf-8'))['operations']:
            result[f"{op['id']}@{op['version']}"] = (op['input']['contract'], op['output']['contract'])
    inventory = json.loads((ROOT / 'docs/chain-features.json').read_text(encoding='utf-8'))
    for feature in inventory['features']:
        if '@' in feature['id']:
            result.setdefault(feature['id'], ('plenora-fixture-read-input-v1', 'plenora-fixture-read-result-v1'))
    return result

def generate():
    plan = json.loads((FIX / 'plan-valid.json').read_text(encoding='utf-8'))
    result = json.loads((FIX / 'result-valid.json').read_text(encoding='utf-8'))
    config = {'database':'host=127.0.0.1 port=1', 'tenant':'surface-fixture',
              'registry':(FIX/'context.json').read_text(encoding='utf-8'),
              'outcomes':encode(result), 'authorized_plans':[encode(plan)],
              'recovery':None, 'attempts':[],
              'artifacts':json.loads((FIX/'artifacts.json').read_text(encoding='utf-8'))}
    profiles = {'standard':config}
    # Qualification aliases avoid claiming provider-specific discovery profiles (e.g. REST).
    all_plan = copy.deepcopy(plan)
    all_plan.update(steps=[], outputs={}, components={'fixture-'+name:copy.deepcopy(plan['components']['io']) for name in sorted({key.split('.')[0] for key in operations()})})
    all_plan['budgets'] = {k: 100_000_000 for k in all_plan['budgets']}
    all_result = copy.deepcopy(result)
    all_result.update(steps=[], outputs={}, remote_effect='none')
    registry = json.loads(config['registry'])
    registry.update(components=all_plan['components'], operations=[], boundaries=[], adapters=[])
    full = copy.deepcopy(config)
    def document(contract, body, name):
        raw = encode({'fixture':'scripted-v1', 'contract':contract, 'body':body}).encode()
        full['artifacts']['ref:'+name] = list(raw)
        return {'contract':contract,'artifact_ref':'ref:'+name,'sha256':hashlib.sha256(raw).hexdigest()}
    for index, (key, contracts) in enumerate(sorted(operations().items())):
        name, version = key.rsplit('@',1)
        step = copy.deepcopy(plan['steps'][0]);sid = f'op{index:03}'
        step.update(id=sid, component='fixture-'+name.split('.')[0], operation=name, operation_version=int(version), input_contract=contracts[0], output_contract=contracts[1], outputs={}, depends_on=[])
        step['parameters'] = document(contracts[0], {'kind':'parameters','operation':name,'version':int(version)},'parameters-'+sid)
        outcome = copy.deepcopy(result['steps'][0]);outcome.update(step_id=sid,operation=name,operation_version=int(version),outputs={})
        outcome['upstream_result'] = document(contracts[1], {'kind':'upstream'},'upstream-'+sid)
        proof = {'kind':'fidelity','step':sid,'operation':name,'version':int(version),'parameters_sha256':step['parameters']['sha256'],'destination':None,'inputs':{},'outputs':{},'verdict':'exact'}
        outcome['reports'] = [{'kind':'fidelity','availability':'available','document':document('plenora-fixture-fidelity-v1',proof,'fidelity-'+sid)}]
        outcome['counters']['request_bytes'] = len(full['artifacts'][step['parameters']['artifact_ref']])
        registry['operations'].append({'component':'fixture-'+name.split('.')[0],'operation':name,'version':int(version),'input_contract':contracts[0],'output_contract':contracts[1],'inputs':{},'outputs':{}})
        all_plan['steps'].append(step);all_result['steps'].append(outcome)
    all_result['counters'] = {k:sum(s['counters'][k] for s in all_result['steps']) for k in all_result['counters']}
    all_result['plan_digest'] = digest(all_plan)
    catalog_plans = []
    for step in all_plan['steps']:
        one = copy.deepcopy(all_plan);one['steps'] = [step];catalog_plans.append(one)
    full.update(registry=encode(registry), outcomes=encode(all_result), authorized_plans=[encode(p) for p in catalog_plans])
    profiles['catalog'] = full
    failed = copy.deepcopy(config);bad = copy.deepcopy(result)
    bad['steps'][0].update(state='failed',reason='upstream_error',outputs={},error={'contract':'plenora-chain-error-v2','schema_version':2,'category':'io','phase':'read','code':'CHAIN_UPSTREAM','remote_effect':'none','retry':{'kind':'never'},'message':'Chain operation rejected or incomplete.','step_id':'read','upstream_error':None,'reports':[]})
    # The source result remains a syntactic container for scripted outcomes, as in retry fixtures.
    failed['outcomes']=encode(bad);profiles['failed']=failed
    limited=copy.deepcopy(config);limit_plan=copy.deepcopy(plan);limit_plan['steps'][0]['budgets']['result_bytes']=1
    limited['authorized_plans']=[encode(limit_plan)];profiles['limit']=limited
    def opts(key=None):return {'run_id':'00000000-0000-0000-0000-000000000010','idempotency_key':key,'deadline_ms':None}
    def run(p=plan,key=None):return {'function':'run','plan':encode(p),'options':opts(key)}
    def case(row, requests, errors, profile='standard', **checks):
        return {'id':row,'profile':profile,'requests':requests,'errors':errors,'checks':checks}
    unknown=copy.deepcopy(plan);unknown['steps'][0]['operation_version']=999
    conflicting=copy.deepcopy(plan);conflicting['revision']=2
    double_deadline=copy.deepcopy(plan);double_deadline['deadline_ms']=1
    deadline=run();deadline['options']['deadline_ms']=1
    uppercase=run();uppercase['options']['run_id']='ABCDEF00-0000-0000-0000-000000000010'
    rows=[
      case('RT-010',[run()],['failed'],'failed',state='failed',effect='none'),
      case('RT-011',[{'function':'validate','plan':encode(unknown)}],['invalid']),
      case('RT-012',[uppercase],['invalid']),
      case('RT-013',[{'function':'artifact','reference':'file:forbidden'}],['invalid']),
      case('RT-014',[{'function':'capabilities'}],[None],architecture={'file':'scripts/check_chain_graph.py','symbol':'def violations('}),
      case('RT-015',[{'function':'capabilities'}],[None],architecture={'file':'tests/architecture/tests/boundaries.rs','symbol':'fn runtime_core_has_no_adapter_or_engine_dependency('}),
      case('RT-016',[{'function':'validate','plan':'{}'}],['invalid'],effect='none'),
      case('RT-017',[{'function':'events','run_id':opts()['run_id'],'cursor':'01','limit':1}],['invalid']),
      case('RT-018',[{'function':'absent','deadline_ms':0}],['invalid'],effect='none'),
      case('RT-019',[{'function':'validate','plan':'secret-payload-sentinel'}],['invalid'],no_data='secret-payload-sentinel'),
      case('RT-020',[run(key='identity'),run(key='identity')],[None,None],same=True),
      case('RT-021',[deadline],['timeout']),
      case('RT-022',[run(key='binding'),run(conflicting,key='binding')],[None,'conflict']),
      case('RT-023',[run(double_deadline)],['invalid']),
      case('RT-024',[run(limit_plan)],['invalid'],'limit',state='rejected',category='resource_limit'),
      case('RT-025',[{'function':'capabilities'}],[None],operation_version=3),
      case('RT-026',[run(conflicting)],['invalid']),
      case('RT-027',[run(key='persisted'),run(key='persisted')],[None,None],same=True),
    ]
    catalog = [case(key,[run(p)],[None],'catalog',operations=[key]) for key,p in zip(sorted(operations()),catalog_plans)]
    functions=['capabilities','validate','submit','run','status','result','events','cancel','resume','artifact','report']
    controls = {
        'capabilities': {'function':'capabilities'},
        'validate': {'function':'validate','plan':'{}'},
        'submit': {'function':'submit','plan':'{}','options':opts()},
        'run': {'function':'run','plan':'{}','options':opts()},
        'status': {'function':'status','run_id':'bad'},
        'result': {'function':'result','run_id':'bad'},
        'events': {'function':'events','run_id':'bad','cursor':0,'limit':1},
        'cancel': {'function':'cancel','run_id':'bad','revision':1},
        'resume': {'function':'resume','run_id':'bad','revision':1,'recovery':'observe'},
        'artifact': {'function':'artifact','reference':'file:forbidden'},
        'report': {'function':'report','run_id':'bad'},
    }
    rows += [case(f'control:{name}', [controls[name]], [None if name == 'capabilities' else 'invalid']) for name in functions]
    cells=[{'item':item,'surface':surface,'case':item} for item in [*sorted(operations()),*[r['id'] for r in rows]] for surface in SURFACES]

    return {'scope':'Declared control/operation-selection doubles only; RT raw-ingress/crash qualification remains separate. No domain library implementation.', 'surfaces':SURFACES,'operations':sorted(operations()),'functions':functions,'cells':cells,'profiles':profiles,'cases':rows+catalog}

if __name__ == '__main__':
    OUT.write_text(json.dumps(generate(),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')

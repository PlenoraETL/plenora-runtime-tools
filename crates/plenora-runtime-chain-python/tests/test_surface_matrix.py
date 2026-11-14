"""Installed facade matrix. Real primary/broker are required; domain providers are declared doubles."""
import asyncio
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import sys
import unittest
import uuid

from plenora_runtime import Client, AsyncClient, ChainError, FUNCTIONS

ROOT = Path(__file__).resolve().parents[3]
MATRIX = json.loads((ROOT / 'crates/plenora-runtime-chain-host/tests/fixtures/surfaces.json').read_text(encoding='utf-8'))


def scoped(request, label):
    request = copy.deepcopy(request)
    if 'options' in request:
        try: canonical = str(uuid.UUID(request['options']['run_id'])) == request['options']['run_id']
        except (ValueError, TypeError, AttributeError): canonical = False
        if canonical:
            request['options']['run_id'] = str(uuid.uuid5(uuid.NAMESPACE_URL, label + request['options']['run_id']))
        if request['options']['idempotency_key'] is not None:
            request['options']['idempotency_key'] = label + request['options']['idempotency_key']
    return request


def response(call):
    try:
        return {'error':None,'value':call(),'failure':None}
    except ChainError as error:
        return {'error':error.code,'value':error.result,'failure':error.failure}


class Matrix(unittest.IsolatedAsyncioTestCase):
    def test_installed_catalog_and_matrix_have_no_missing_cells(self):
        self.assertEqual(list(FUNCTIONS), MATRIX['functions'])
        items = set(MATRIX['operations']) | {f'RT-{n:03}' for n in range(10,28)} | {'control:'+f for f in FUNCTIONS}
        cells = [(c['item'],c['surface']) for c in MATRIX['cells']]
        self.assertEqual(len(cells), len(set(cells)))
        self.assertEqual(set(cells), {(i,s) for i in items for s in MATRIX['surfaces']})
        self.assertEqual({c['id'] for c in MATRIX['cases']}, items)

    def check_response(self, case, index, reply, previous):
        self.assertEqual(reply['error'], case['errors'][index], case['id'])
        checks = case['checks']
        if checks.get('same') and previous is not None: self.assertEqual(reply, previous)
        if 'state' in checks: self.assertEqual(reply['value']['state'], checks['state'])
        if 'category' in checks: self.assertEqual(reply['failure']['category'], checks['category'])
        if 'effect' in checks: self.assertEqual(reply['failure']['remote_effect'], checks['effect'])
        if 'no_data' in checks: self.assertNotIn(checks['no_data'], json.dumps(reply))
        if 'operation_version' in checks: self.assertTrue(any(o['version'] == checks['operation_version'] for o in reply['value']['operations']))
        if 'operations' in checks:
            self.assertEqual(reply['value']['state'], 'succeeded')
            actual = [f"{s['operation']}@{s['operation_version']}" for s in reply['value']['steps']]
            self.assertEqual(actual, checks['operations'])
            self.assertEqual(reply['value']['counters']['max_attempts'], len(actual))

    @unittest.skipUnless(os.environ.get('PLENORA_POSTGRES_URL') and os.environ.get('PLENORA_CHAIN_BIN'), 'requires installed binary/wheel and real primary')
    async def test_native_installed_every_rt_and_operation(self):
        await self.exercise(['cli','python_sync','python_async'])

    @unittest.skipUnless(os.environ.get('PLENORA_MATRIX_ENDPOINTS') and os.environ.get('PLENORA_CHAIN_BIN'), 'requires running real NATS hosts from CI')
    async def test_remote_installed_every_rt_and_operation(self):
        await self.exercise(['remote_sync','remote_async','remote_cli'])

    async def exercise(self, modes):
        endpoints = json.loads(os.environ.get('PLENORA_MATRIX_ENDPOINTS','{}'))
        for mode in modes:
            visited = set()
            for case in MATRIX['cases']:
                config = copy.deepcopy(MATRIX['profiles'][case['profile']])
                config.update(database=os.environ['PLENORA_POSTGRES_URL'], tenant='matrix-'+str(uuid.uuid4()))
                label = mode + '-' + case['id'] + '-'
                if mode.startswith('remote'):
                    endpoint = endpoints[case['profile']]
                    client = await AsyncClient.remote(endpoint['server'], endpoint['subject'], observation_timeout_ms=endpoint['observation_timeout_ms']) if mode == 'remote_async' else await asyncio.to_thread(Client.remote, endpoint['server'], endpoint['subject'], observation_timeout_ms=endpoint['observation_timeout_ms'])
                elif mode == 'python_async':
                    client = await AsyncClient.create(config)
                    await client.migrate()
                else:
                    client = await asyncio.to_thread(Client, config)
                    await asyncio.to_thread(client.migrate)
                if mode in ['remote_sync','remote_async'] and case['id'] == 'RT-010':
                    with self.assertRaises(ChainError) as unsupported:
                        if mode == 'remote_async': await client.migrate()
                        else: await asyncio.to_thread(client.migrate)
                    self.assertEqual(unsupported.exception.code,'unsupported')
                previous = None
                for index, raw in enumerate(case['requests']):
                    request = scoped(raw, label)
                    started = time.monotonic()
                    if mode in ['cli','remote_cli']:
                        def binary():
                            with tempfile.TemporaryDirectory(dir=ROOT/'target') as folder:
                                path = Path(folder)/'config.json'
                                path.write_text(json.dumps(config),encoding='utf-8')
                                command = [os.environ['PLENORA_CHAIN_BIN'], 'remote-call', endpoint['server'],endpoint['subject']] if mode == 'remote_cli' else [os.environ['PLENORA_CHAIN_BIN'],'scripted-call',str(path)]
                                if request['function'] == 'run':
                                    plan = Path(folder)/'plan.json'
                                    plan.write_text(request['plan'],encoding='utf-8')
                                    options = request['options']
                                    command = [os.environ['PLENORA_CHAIN_BIN'],'chain','run','--plan',str(plan),'--run',options['run_id'],'--json']
                                    command += ['--server',endpoint['server'],'--subject',endpoint['subject'],'--observation-timeout-ms',str(endpoint['observation_timeout_ms'])] if mode == 'remote_cli' else ['--config',str(path)]
                                    if options['idempotency_key'] is not None: command += ['--idempotency-key',options['idempotency_key']]
                                    if options['deadline_ms'] is not None: command += ['--deadline-ms',str(options['deadline_ms'])]
                                run = subprocess.run(command,input=json.dumps(request),capture_output=True,text=True,encoding='utf-8',check=False)
                                reply = json.loads(run.stdout)
                                self.assertEqual(run.stderr,'')
                                self.assertEqual(run.returncode, 0 if reply['error'] is None else {'failed':3,'recovery':4,'cancelled':5,'timeout':6,'storage':6,'pending':6,'expired':6,'not_found':6}.get(reply['error'],2))
                                return reply
                        reply = await asyncio.to_thread(binary)
                    elif mode in ['python_async','remote_async']:
                        try: reply = {'value':await client.call(**request),'error':None,'failure':None}
                        except ChainError as error: reply = {'value':error.result,'error':error.code,'failure':error.failure}
                    else:
                        reply = await asyncio.to_thread(response, lambda: client.call(**request))
                    if mode.startswith("remote"):
                        print(f"Qualification {mode} {case['id']} {request['function']}: elapsed={time.monotonic()-started:.3f}s result={reply['error']}", file=sys.stderr)
                    self.check_response(case,index,reply,previous)
                    previous = reply
                visited.add(case['id'])
            surface = 'runtime' if mode.startswith('remote') else mode
            self.assertEqual(visited,{c['item'] for c in MATRIX['cells'] if c['surface']==surface})

if __name__ == '__main__': unittest.main()

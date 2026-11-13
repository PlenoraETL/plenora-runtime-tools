"""Installed positive controls; real primary/broker, explicitly scripted domain adapters."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
import uuid

from plenora_runtime import AsyncClient, Client, ChainError
from test_installed import CONFIG, PLAN, ROOT


class Controls(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(os.environ.get('PLENORA_POSTGRES_URL') and os.environ.get('PLENORA_CHAIN_BIN'),
                         'requires real PostgreSQL and installed artifacts')
    async def test_native_installed_cancel_resume_events(self):
        await self.exercise(None)

    @unittest.skipUnless(os.environ.get('PLENORA_CONTROL_ENDPOINT') and os.environ.get('PLENORA_CHAIN_BIN'),
                         'requires real PostgreSQL/NATS host and installed artifacts')
    async def test_remote_installed_cancel_resume_events(self):
        await self.exercise(json.loads(os.environ['PLENORA_CONTROL_ENDPOINT']))

    async def exercise(self, endpoint):
        for mode in ['sync', 'async', 'cli']:
            config = dict(CONFIG, tenant='controls-' + str(uuid.uuid4()))
            if endpoint:
                sync = await asyncio.to_thread(Client.remote, endpoint['server'], endpoint['subject'])
                asynchronous = await AsyncClient.remote(endpoint['server'], endpoint['subject'])
                receipt = endpoint['runs'][mode]
            else:
                sync = await asyncio.to_thread(Client, config)
                asynchronous = await AsyncClient.create(config)
                await asynchronous.migrate()
                receipt = await asynchronous.submit(plan=PLAN, options={
                    'run_id':str(uuid.uuid4()), 'idempotency_key':None, 'deadline_ms':None})
            def binary(function, arguments, lines):
                with tempfile.TemporaryDirectory(dir=ROOT/'target') as folder:
                    profile = Path(folder)/'config.json'
                    profile.write_text(json.dumps(config), encoding='utf-8')
                    command = [os.environ['PLENORA_CHAIN_BIN'], 'chain', function,
                               '--run', arguments['run_id']]
                    for name in ['revision', 'cursor', 'limit', 'recovery']:
                        if name in arguments: command += ['--'+name, str(arguments[name])]
                    command += ['--server', endpoint['server'], '--subject', endpoint['subject']] if endpoint else ['--config', str(profile)]
                    command += ['--json-lines' if lines else '--json']
                    completed = subprocess.run(command, capture_output=True, text=True,
                                               encoding='utf-8', check=False, timeout=30)
                    self.assertEqual(completed.stderr, '')
                    if lines:
                        self.assertEqual(completed.returncode, 0, completed.stdout)
                        return [json.loads(row) for row in completed.stdout.splitlines()]
                    response = json.loads(completed.stdout)
                    if response['error']:
                        self.assertEqual(completed.returncode, 2 if response['error']=='conflict' else 6)
                        raise ChainError(response['error'], response['value'], response['failure'])
                    self.assertEqual(completed.returncode, 0)
                    return response['value']
            async def call(function, **arguments):
                if mode == 'cli': return await asyncio.to_thread(binary, function, arguments, False)
                if mode == 'sync': return await asyncio.to_thread(sync.call, function, **arguments)
                return await asynchronous.call(function, **arguments)
            run = receipt['run_id']
            observed = await call('resume', run_id=run, revision=receipt['revision'], recovery='observe')
            self.assertEqual(observed, receipt)
            cancelled = await call('cancel', run_id=run, revision=receipt['revision'])
            self.assertGreater(cancelled['revision'], receipt['revision'])
            with self.assertRaises(ChainError) as stale:
                await call('resume', run_id=run, revision=receipt['revision'], recovery='reconcile')
            self.assertEqual(stale.exception.code, 'conflict')
            # Only a stale version may race the host worker after cancellation is durable.
            # The real fixture holds a renewed primary lease until that exact transition.
            deadline = time.monotonic() + 30
            while True:
                current = await call('status', run_id=run)
                try:
                    final = await call('resume', run_id=run, revision=current['revision'], recovery='reconcile')
                except ChainError as error:
                    self.assertEqual(error.code, 'conflict')
                else:
                    if final['state'] == 'cancelled': break
                self.assertLess(time.monotonic(), deadline, 'cancel/resume did not converge')
                await asyncio.sleep(0.01)
            result = await call('result', run_id=run)
            self.assertEqual(result['state'], 'cancelled')
            self.assertEqual(result['remote_effect'], 'none')
            self.assertEqual(result['counters']['max_attempts'], 0)
            expected = [e async for e in asynchronous.iter_events(run, limit=1)]
            self.assertTrue(expected)
            self.assertEqual([e['sequence'] for e in expected], list(range(1,len(expected)+1)))
            self.assertEqual(await asyncio.to_thread(lambda: list(sync.iter_events(run,limit=2))), expected)
            if mode == 'cli':
                self.assertEqual(await asyncio.to_thread(binary,'events',dict(run_id=run,cursor=0,limit=1),True), expected)
            # Stable terminal observations, no new attempt or event on duplicate control.
            before = await call('status', run_id=run)
            self.assertEqual(await call('cancel', run_id=run, revision=before['revision']), before)
            self.assertEqual(await call('resume', run_id=run, revision=before['revision'], recovery='reconcile'), before)
            self.assertEqual([e async for e in asynchronous.iter_events(run,limit=2)], expected)


if __name__ == '__main__':
    unittest.main()

"""Installed error projection and wait ownership; delayed calls below are declared doubles."""
import asyncio
import json
import threading
import unittest
from unittest.mock import patch
from pathlib import Path
from plenora_runtime import Client, AsyncClient, ChainError

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT/'crates/plenora-runtime-chain-host/tests/fixtures'
CONFIG = {'database':'host=127.0.0.1 port=1 connect_timeout=1','tenant':'model-errors',
          'registry':(FIXTURES/'context.json').read_text(encoding='utf-8'),
          'outcomes':(FIXTURES/'result-valid.json').read_text(encoding='utf-8'),
          'authorized_plans':[(FIXTURES/'plan-valid.json').read_text(encoding='utf-8')],
          'artifacts':json.loads((FIXTURES/'artifacts.json').read_text(encoding='utf-8'))}

class ErrorsAndWaits(unittest.IsolatedAsyncioTestCase):
    async def test_construction_migration_connection_and_calls_share_safe_errors(self):
        for invoke in [lambda: Client({}),lambda: Client.remote('nats://127.0.0.1:1','bad.*'),lambda: Client(CONFIG).migrate()]:
            with self.assertRaises(ChainError) as caught: await asyncio.to_thread(invoke)
            self.assertIn(caught.exception.code, ['invalid','storage'])
            self.assertEqual(str(caught.exception),caught.exception.code)
            self.assertIsNotNone(caught.exception.category)
            self.assertIsNone(caught.exception.run_id)
        with self.assertRaises(ChainError) as caught: await AsyncClient.create({})
        self.assertEqual(caught.exception.code,'invalid')
        with self.assertRaises(ChainError) as caught: await AsyncClient.remote('nats://127.0.0.1:1','bad.*')
        self.assertEqual(caught.exception.code,'invalid')
        client = await AsyncClient.create(CONFIG)
        with self.assertRaises(ChainError) as caught: await client.events(run_id='bad',cursor=object(),limit=1)
        self.assertEqual(caught.exception.code,'invalid')

    def test_full_domain_context_is_separate_from_exception_display(self):
        result = {'run_id':'00000000-0000-0000-0000-000000000001','remote_effect':'partial',
                  'retry':{'kind':'never'},'error':{'category':'io','phase':'cleanup','code':'CHAIN_UPSTREAM','retry':{'kind':'never'}},
                  'reports':['secret-sentinel']}
        error = ChainError('failed',result)
        self.assertEqual((error.category,error.phase,error.remote_effect,error.retry,error.run_id),('io','cleanup','partial',{'kind':'never'},result['run_id']))
        self.assertIs(error.result,result)
        self.assertNotIn('secret-sentinel',str(error))
        self.assertNotIn('secret-sentinel',repr(error))

    async def test_native_initialization_smoke(self):
        matrix = await asyncio.to_thread(lambda: json.loads((FIXTURES/'surfaces.json').read_text(encoding='utf-8')))
        self.assertIsInstance(await AsyncClient.create(matrix['profiles']['catalog']), AsyncClient)

    async def test_initialization_allows_loop_progress_with_declared_barrier_double(self):
        entered, release = threading.Event(), threading.Event()
        def controlled(config):
            entered.set()
            if not release.wait(10): raise AssertionError('event loop did not release initialization')
            return Client(config)
        with patch('plenora_runtime.Client', side_effect=controlled):
            task = asyncio.create_task(AsyncClient.create(CONFIG))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 10))
                # The initializer cannot finish until this event-loop turn releases it.
                self.assertFalse(task.done())
                await asyncio.sleep(0)
            finally:
                release.set()
            self.assertIsInstance(await task, AsyncClient)

    async def test_cancelled_wait_does_not_call_cancel_or_abort_the_native_work(self):
        client = await AsyncClient.create(CONFIG)
        sync = await client._ready()
        entered,release,finished = threading.Event(),threading.Event(),threading.Event()
        calls = []
        def delayed(function, **arguments):
            calls.append(function)
            entered.set()
            if not release.wait(10): raise AssertionError('test release missing')
            finished.set()
            return {'run_id':'00000000-0000-0000-0000-000000000001','state':'succeeded'}
        with patch.object(sync,'call',side_effect=delayed):
            task = asyncio.create_task(client.run(plan='declared-double'))
            self.assertTrue(await asyncio.to_thread(entered.wait,10))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertFalse(finished.is_set())
            release.set()
            self.assertTrue(await asyncio.to_thread(finished.wait,10))
        self.assertEqual(calls,['run'])

if __name__ == '__main__': unittest.main()

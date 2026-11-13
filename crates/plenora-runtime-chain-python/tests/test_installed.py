"""Run against an installed wheel, never against a Python implementation of the engine."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import unittest

from plenora_runtime import AsyncClient, ChainError, Client, FUNCTIONS

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "contracts/chains-v2/fixtures"
PLAN = (FIXTURES / "plan-valid.json").read_text(encoding="utf-8")
CONFIG = {
    "database": os.environ.get("PLENORA_POSTGRES_URL", "host=127.0.0.1 port=1 dbname=not_running connect_timeout=1"),
    "tenant": "wheel-qualification",
    "registry": (FIXTURES / "context.json").read_text(encoding="utf-8"),
    "outcomes": (FIXTURES / "result-valid.json").read_text(encoding="utf-8"),
    "recovery": None,
    "artifacts": {"ref:fixture": [0, 1, 2, 255]},
}
OPTIONS = {"run_id": "00000000-0000-0000-0000-000000000011", "idempotency_key": None, "deadline_ms": None}
CASES = {
    "capabilities": {},
    "validate": {"plan": "{}"},
    "submit": {"plan": "{}", "options": OPTIONS},
    "run": {"plan": "{}", "options": OPTIONS},
    "status": {"run_id": "bad"},
    "result": {"run_id": "bad"},
    "events": {"run_id": "bad", "cursor": 0, "limit": 1},
    "cancel": {"run_id": "bad", "revision": 1},
    "resume": {"run_id": "bad", "revision": 1},
    "artifact": {"reference": "file:forbidden"},
    "report": {"run_id": "bad"},
}


class Installed(unittest.IsolatedAsyncioTestCase):
    def test_catalog_is_enumerated_exactly(self):
        self.assertEqual(set(FUNCTIONS), set(CASES))
        client = Client(CONFIG)
        self.assertEqual(client.artifact(reference="ref:fixture"), [0, 1, 2, 255])
        self.assertEqual(client.validate(plan=PLAN)["order"], ["read", "transform", "write"])

    async def test_sync_async_error_parity_for_every_function(self):
        sync = Client(CONFIG)
        asynchronous = AsyncClient(CONFIG)
        for name, arguments in CASES.items():
            with self.subTest(function=name):
                try:
                    expected = getattr(sync, name)(**arguments)
                except ChainError as error:
                    with self.assertRaises(ChainError) as caught:
                        await getattr(asynchronous, name)(**arguments)
                    self.assertEqual(caught.exception.code, error.code)
                    self.assertEqual(caught.exception.result, error.result)
                else:
                    self.assertEqual(await getattr(asynchronous, name)(**arguments), expected)

    async def test_event_loop_heartbeat_during_native_work(self):
        client = AsyncClient(CONFIG)
        beats = 0
        running = True

        async def heartbeat():
            nonlocal beats
            while running:
                beats += 1
                await asyncio.sleep(0)

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            for _ in range(20):
                await client.validate(plan=PLAN)
        finally:
            running = False
            await heartbeat_task
        self.assertGreater(beats, 20)

    @unittest.skipUnless(os.environ.get("PLENORA_CHAIN_BIN"), "requires installed binary path")
    def test_installed_binary_same_catalog_calls(self):
        config_path = ROOT / "target/p1-5-scripted-config.json"
        config_path.write_text(json.dumps(CONFIG), encoding="utf-8")
        for name, arguments in CASES.items():
            completed = subprocess.run(
                [os.environ["PLENORA_CHAIN_BIN"], "scripted-call", str(config_path)],
                input=json.dumps({"function": name, **arguments}), text=True,
                encoding="utf-8", capture_output=True, check=False,
            )
            reply = json.loads(completed.stdout)
            self.assertEqual(completed.stderr, "")
            self.assertEqual(completed.returncode, 0 if name == "capabilities" else 2)
            self.assertEqual(reply["error"], None if name == "capabilities" else "invalid")


if __name__ == "__main__":
    unittest.main()

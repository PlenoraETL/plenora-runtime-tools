"""Run against an installed wheel, never against a Python implementation of the engine."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

from plenora_runtime import AsyncClient, ChainError, Client, FUNCTIONS

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "crates/plenora-runtime-chain-host/tests/fixtures"
PLAN = (FIXTURES / "plan-valid.json").read_text(encoding="utf-8")
EVENT = json.loads((ROOT / "contracts/chains-v2/fixtures/event-valid.json").read_text(encoding="utf-8"))
CONFIG = {
    "database": os.environ.get("PLENORA_POSTGRES_URL", "host=127.0.0.1 port=1 dbname=not_running connect_timeout=1"),
    "tenant": "wheel-qualification",
    "registry": (FIXTURES / "context.json").read_text(encoding="utf-8"),
    "outcomes": (FIXTURES / "result-valid.json").read_text(encoding="utf-8"),
    "authorized_plans": [PLAN],
    "recovery": None,
    "artifacts": {**json.loads((FIXTURES / "artifacts.json").read_text(encoding="utf-8")), "ref:fixture": [0, 1, 2, 255]},
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
        if os.environ.get("PLENORA_POSTGRES_URL"):
            client.migrate()
            self.assertEqual(client.artifact(reference="ref:fixture"), [0, 1, 2, 255])
            self.assertEqual(client.validate(plan=PLAN)["order"], ["read", "transform", "write"])
        else:
            for function, arguments in [("artifact", {"reference":"ref:fixture"}), ("validate", {"plan":PLAN})]:
                with self.assertRaises(ChainError) as caught:
                    client.call(function, **arguments)
                self.assertEqual(caught.exception.code, "storage")

    async def test_report_cursor_on_installed_bindings(self):
        sync, asynchronous = Client(CONFIG), AsyncClient(CONFIG)
        for cursor, expected in [({"index": 0, "offset": 0}, "not_found" if os.environ.get("PLENORA_POSTGRES_URL") else "storage"),
                                 ({"index": 0, "offset": -1}, "invalid")]:
            with self.assertRaises(ChainError) as error:
                await asyncio.to_thread(sync.report, run_id=OPTIONS["run_id"], cursor=cursor)
            self.assertEqual(error.exception.code, expected)
            with self.assertRaises(ChainError) as error:
                await asynchronous.report(run_id=OPTIONS["run_id"], cursor=cursor)
            self.assertEqual(error.exception.code, expected)
            if os.environ.get("PLENORA_CHAIN_BIN"):
                def binary():
                    with tempfile.TemporaryDirectory(dir=ROOT / "target") as folder:
                        config = Path(folder) / "config.json"
                        config.write_text(json.dumps(CONFIG), encoding="utf-8")
                        return subprocess.run([os.environ["PLENORA_CHAIN_BIN"], "chain", "report",
                            "--run", OPTIONS["run_id"], "--report-index", str(cursor["index"]),
                            "--offset", str(cursor["offset"]), "--config", str(config), "--json"],
                            capture_output=True, text=True, encoding="utf-8", check=False)
                result = await asyncio.to_thread(binary)
                self.assertEqual(result.stderr, "")
                self.assertEqual(json.loads(result.stdout)["error"], expected)

    async def test_sync_async_error_parity_for_every_function(self):
        sync = Client(CONFIG)
        asynchronous = AsyncClient(CONFIG)
        for name, arguments in CASES.items():
            with self.subTest(function=name):
                try:
                    expected = await asyncio.to_thread(getattr(sync, name), **arguments)
                except ChainError as error:
                    with self.assertRaises(ChainError) as caught:
                        await getattr(asynchronous, name)(**arguments)
                    self.assertEqual(caught.exception.code, error.code)
                    self.assertEqual(caught.exception.result, error.result)
                else:
                    self.assertEqual(await getattr(asynchronous, name)(**arguments), expected)

    async def test_event_iterators_use_native_cursor_and_reject_gaps(self):
        event = EVENT
        events = [dict(event, sequence=i) for i in range(1, 4)]
        def page(function, **arguments):
            self.assertEqual(function, "events")
            self.assertEqual(arguments["run_id"], event["run_id"])
            start = arguments["cursor"]
            return events[start:start + arguments["limit"]]
        sync = Client(CONFIG)
        asynchronous = AsyncClient(CONFIG)
        with patch.object(sync, "call", side_effect=page):
            self.assertEqual(list(sync.iter_events(event["run_id"], limit=2)), events)
        async def async_page(function, **arguments):
            await asyncio.sleep(0)
            return page(function, **arguments)
        with patch.object(asynchronous, "call", side_effect=async_page):
            self.assertEqual([e async for e in asynchronous.iter_events(event["run_id"], limit=2)], events)
        class Indexable:
            def __index__(self):
                return 1
        for field in ["cursor", "limit"]:
            for invalid in [True, False, 1.5, "1", -1, Indexable()]:
                with self.subTest(field=field, invalid=invalid):
                    with self.assertRaises(ChainError):
                        list(sync.iter_events(event["run_id"], **{field: invalid}))
                    with self.assertRaises(ChainError):
                        [e async for e in asynchronous.iter_events(event["run_id"], **{field: invalid})]
        events[1]["sequence"] = 4
        with patch.object(sync, "call", side_effect=page):
            with self.assertRaises(ChainError) as caught:
                list(sync.iter_events(event["run_id"], limit=2))
            self.assertEqual(caught.exception.code, "conflict")
        with patch.object(asynchronous, "call", side_effect=async_page):
            with self.assertRaises(ChainError) as caught:
                [e async for e in asynchronous.iter_events(event["run_id"], limit=2)]
            self.assertEqual(caught.exception.code, "conflict")

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
                try:
                    await client.validate(plan=PLAN)
                except ChainError as error:
                    self.assertFalse(os.environ.get("PLENORA_POSTGRES_URL"))
                    self.assertEqual(error.code, "storage")
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
            expected_error = (None if name == "capabilities" else "invalid") if os.environ.get("PLENORA_POSTGRES_URL") else "storage"
            self.assertEqual(completed.returncode, 6 if expected_error == "storage" else (0 if expected_error is None else 2))
            self.assertEqual(reply["error"], expected_error)


    @unittest.skipUnless(os.environ.get("PLENORA_CHAIN_BIN"), "requires installed binary path")
    def test_installed_binary_named_commands(self):
        config_path = ROOT / "target/p1-5-command-config.json"
        invalid_plan = ROOT / "target/p1-5-command-invalid-plan.json"
        config_path.write_text(json.dumps(CONFIG), encoding="utf-8")
        invalid_plan.write_text("{}", encoding="utf-8")
        commands = {
            "capabilities": ["capabilities"],
            "validate": ["chain", "validate", "--plan", str(invalid_plan)],
            "submit": ["chain", "submit", "--plan", str(invalid_plan), "--run", OPTIONS["run_id"]],
            "run": ["chain", "run", "--plan", str(invalid_plan), "--run", OPTIONS["run_id"]],
            "status": ["chain", "status", "--run", "bad"],
            "result": ["chain", "result", "--run", "bad"],
            "events": ["chain", "events", "--run", "bad", "--cursor", "0"],
            "cancel": ["chain", "cancel", "--run", "bad", "--revision", "1"],
            "resume": ["chain", "resume", "--run", "bad", "--revision", "1"],
            "artifact": ["artifact", "get", "--ref", "file:forbidden"],
            "report": ["report", "get", "--run", "bad"],
        }
        self.assertEqual(set(commands), set(FUNCTIONS))
        try:
            for name, command in commands.items():
                completed = subprocess.run([os.environ["PLENORA_CHAIN_BIN"], *command,
                    "--config", str(config_path), "--json"], text=True, encoding="utf-8",
                    capture_output=True, check=False)
                self.assertEqual(completed.stderr, "")
                expected_error = (None if name == "capabilities" else "invalid") if os.environ.get("PLENORA_POSTGRES_URL") else "storage"
                self.assertEqual(completed.returncode, 6 if expected_error == "storage" else (0 if expected_error is None else 2))
                self.assertEqual(json.loads(completed.stdout)["error"], expected_error)
        finally:
            config_path.unlink()
            invalid_plan.unlink()


@unittest.skipUnless(os.environ.get("PLENORA_POSTGRES_URL") and os.environ.get("PLENORA_CHAIN_BIN"),
                     "requires real PostgreSQL and installed binary in the infrastructure CI job")
class InstalledPostgres(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_resume_and_events_on_installed_surfaces(self):
        config = dict(CONFIG, tenant="installed-" + str(uuid.uuid4()))
        client = AsyncClient(config)
        await client.migrate()
        controls = dict(OPTIONS, run_id=str(uuid.uuid4()), idempotency_key=str(uuid.uuid4()))
        receipt = await client.submit(plan=PLAN, options=controls)
        sync = Client(config)
        cancelled = await asyncio.to_thread(sync.cancel, run_id=receipt["run_id"], revision=receipt["revision"])
        with self.assertRaises(ChainError) as stale:
            await client.resume(run_id=receipt["run_id"], revision=receipt["revision"])
        self.assertEqual(stale.exception.code, "conflict")
        final = await client.resume(run_id=receipt["run_id"], revision=cancelled["revision"])
        self.assertEqual(final["state"], "cancelled")
        expected = await client.result(run_id=receipt["run_id"])
        self.assertEqual(await asyncio.to_thread(sync.result, run_id=receipt["run_id"]), expected)
        events = [e async for e in client.iter_events(receipt["run_id"], limit=1)]
        self.assertEqual(await asyncio.to_thread(lambda: list(sync.iter_events(receipt["run_id"], limit=2))), events)
        self.assertEqual([e["sequence"] for e in events], list(range(1, len(events) + 1)))
        config_path = ROOT / "target" / (config["tenant"] + ".json")
        plan_path = FIXTURES / "plan-valid.json"
        await asyncio.to_thread(config_path.write_text, json.dumps(config), encoding="utf-8")
        async def binary(*arguments, lines=False):
            result = await asyncio.to_thread(subprocess.run,
                [os.environ["PLENORA_CHAIN_BIN"], *arguments, "--config", str(config_path),
                 "--json-lines" if lines else "--json"], capture_output=True, text=True,
                encoding="utf-8", check=False)
            self.assertEqual(result.stderr, "")
            self.assertEqual(result.returncode, 0, result.stdout)
            return [json.loads(line) for line in result.stdout.splitlines()] if lines else json.loads(result.stdout)["value"]
        try:
            started = await binary("chain", "submit", "--plan", str(plan_path), "--run", str(uuid.uuid4()))
            stopped = await binary("chain", "cancel", "--run", started["run_id"], "--revision", str(started["revision"]))
            finished = await binary("chain", "resume", "--run", started["run_id"], "--revision", str(stopped["revision"]))
            self.assertEqual(finished["state"], "cancelled")
            actual = await binary("chain", "events", "--run", started["run_id"], "--cursor", "0", "--limit", "1", lines=True)
            self.assertEqual(actual, [e async for e in client.iter_events(started["run_id"])])
            self.assertEqual((await binary("chain", "result", "--run", started["run_id"]))["state"], "cancelled")
        finally:
            await asyncio.to_thread(config_path.unlink)

    async def test_resource_integrity_before_effect_on_installed_surfaces(self):
        config = json.loads(json.dumps(CONFIG))
        config["tenant"] = "resolver-" + str(uuid.uuid4())
        config["artifacts"]["ref:fidelity-write"].append(0)
        client = AsyncClient(config)
        await client.migrate()
        sync = Client(config)
        failures = []
        with self.assertRaises(ChainError) as rejected:
            await asyncio.to_thread(sync.run, plan=PLAN, options=dict(OPTIONS, run_id=str(uuid.uuid4())))
        failures.append(rejected.exception)
        with self.assertRaises(ChainError) as rejected:
            await client.run(plan=PLAN, options=dict(OPTIONS, run_id=str(uuid.uuid4())))
        failures.append(rejected.exception)
        def binary():
            with tempfile.TemporaryDirectory(dir=ROOT / "target") as folder:
                profile = Path(folder) / "config.json"
                profile.write_text(json.dumps(config), encoding="utf-8")
                return subprocess.run([os.environ["PLENORA_CHAIN_BIN"], "chain", "run",
                    "--plan", str(FIXTURES / "plan-valid.json"), "--run", str(uuid.uuid4()),
                    "--config", str(profile), "--json"], capture_output=True, text=True,
                    encoding="utf-8", check=False)
        completed = await asyncio.to_thread(binary)
        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(completed.stderr, "")
        response = json.loads(completed.stdout)
        for failure in failures:
            self.assertEqual(failure.code, response["error"])
        for result in [f.result for f in failures] + [response["value"]]:
            self.assertEqual([s["state"] for s in result["steps"][:2]], ["succeeded", "succeeded"])
            self.assertEqual(result["steps"][2]["remote_effect"], "none")
            reports = await client.report(run_id=result["run_id"])
            self.assertEqual(len(reports), 2)

    async def test_journal_budget_rejects_on_installed_surfaces(self):
        config = dict(CONFIG, tenant="journal-" + str(uuid.uuid4()), journal_bytes=1)
        client = AsyncClient(config)
        await client.migrate()
        sync = Client(config)
        with self.assertRaises(ChainError) as rejected:
            await asyncio.to_thread(sync.submit, plan=PLAN, options=dict(OPTIONS, run_id=str(uuid.uuid4())))
        self.assertEqual(rejected.exception.code, "resource_limit")
        with self.assertRaises(ChainError) as rejected:
            await client.submit(plan=PLAN, options=dict(OPTIONS, run_id=str(uuid.uuid4())))
        self.assertEqual(rejected.exception.code, "resource_limit")
        def binary():
            with tempfile.TemporaryDirectory(dir=ROOT / "target") as folder:
                profile = Path(folder) / "config.json"
                profile.write_text(json.dumps(config), encoding="utf-8")
                return subprocess.run([os.environ["PLENORA_CHAIN_BIN"], "chain", "submit",
                    "--plan", str(FIXTURES / "plan-valid.json"), "--run", str(uuid.uuid4()),
                    "--config", str(profile), "--json"], capture_output=True, text=True,
                    encoding="utf-8", check=False)
        rejected = await asyncio.to_thread(binary)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertEqual(rejected.stderr, "")
        self.assertEqual(json.loads(rejected.stdout)["error"], "resource_limit")

    async def test_retry_accounting_from_installed_sync_async_and_binary(self):
        plan = json.loads(PLAN)
        plan["steps"][1]["retry_policy"] = {"max_attempts": 2, "backoff_ms": 20}
        outcomes = json.loads(CONFIG["outcomes"])
        second = json.loads(json.dumps(outcomes["steps"][1]))
        second["attempt"] = 2
        for output in second["outputs"].values():
            output["attempt"] = 2
        first = outcomes["steps"][1]
        first.update(state="failed", reason="upstream_error", outputs={}, remote_effect="none",
                     retry={"kind": "safe"}, error={
                         "contract": "plenora-chain-error-v2", "schema_version": 2,
                         "category": "io", "phase": "read", "code": "CHAIN_UPSTREAM",
                         "remote_effect": "none", "retry": {"kind": "safe"},
                         "message": "Chain operation rejected or incomplete.",
                         "step_id": "transform", "upstream_error": None, "reports": []})
        config = dict(CONFIG, tenant="retry-" + str(uuid.uuid4()),
                      outcomes=json.dumps(outcomes), attempts=[json.dumps(second)],
                      authorized_plans=[json.dumps(plan)])
        client = AsyncClient(config)
        await client.migrate()
        sync = Client(config)
        def options():
            return dict(OPTIONS, run_id=str(uuid.uuid4()))
        results = [await asyncio.to_thread(sync.run, plan=json.dumps(plan), options=options()),
                   await client.run(plan=json.dumps(plan), options=options())]
        def binary():
            with tempfile.TemporaryDirectory(dir=ROOT / "target") as folder:
                config_path, plan_path = Path(folder) / "config.json", Path(folder) / "plan.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                plan_path.write_text(json.dumps(plan), encoding="utf-8")
                completed = subprocess.run([os.environ["PLENORA_CHAIN_BIN"], "chain", "run",
                    "--plan", str(plan_path), "--run", str(uuid.uuid4()), "--config", str(config_path),
                    "--json"], capture_output=True, text=True, encoding="utf-8", check=False)
                self.assertEqual(completed.returncode, 0, completed.stdout)
                self.assertEqual(completed.stderr, "")
                return json.loads(completed.stdout)["value"]
        results.append(await asyncio.to_thread(binary))
        for result in results:
            self.assertEqual(result["state"], "succeeded")
            self.assertEqual(result["steps"][1]["attempt"], 2)
            self.assertEqual(result["counters"]["max_attempts"], 4)
            reports = await client.report(run_id=result["run_id"])
            self.assertEqual(len(reports), 4)

if __name__ == "__main__":
    unittest.main()

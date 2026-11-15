"""Installed surfaces invoke the pinned Rust PostgreSQL provider, not scripted outcomes."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid
from plenora_runtime import Client, AsyncClient, ChainError


@unittest.skipUnless(os.environ.get("PLENORA_DATABASE_FIXTURE") and os.environ.get("PLENORA_POSTGRES_URL") and os.environ.get("PLENORA_CHAIN_BIN"), "requires real PostgreSQL campaign and installed CLI/wheel")
class DatabaseReal(unittest.IsolatedAsyncioTestCase):
    async def test_cli_sync_async_share_real_database_contracts(self):
        fixture = json.loads(Path(os.environ["PLENORA_DATABASE_FIXTURE"]).read_text(encoding="utf-8-sig"))
        for scenario in ("success", "missing", "denied"):
            case = fixture if scenario == "success" else fixture[scenario]
            config = {"database": os.environ["PLENORA_POSTGRES_URL"], "adapter": case["adapter"],
                      "connections": {"DB": {"destination": "ref:destination", "secret": os.environ["PLENORA_POSTGRES_URL"], "allow_plaintext": True}}}
            plan = json.dumps(case["plan"])
            client = Client(config, profile="database")
            client.migrate()
            async_client = await AsyncClient.create(config, profile="database")
            results = []
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "config.json"
                path.write_text(json.dumps(config), encoding="utf-8")
                for surface in ("cli", "sync", "async"):
                    options = {"run_id": str(uuid.uuid4()), "idempotency_key": None, "deadline_ms": None}
                    if surface == "cli":
                        process = await asyncio.to_thread(subprocess.run, [os.environ["PLENORA_CHAIN_BIN"], "database-call", str(path)], input=json.dumps({"function": "run", "plan": plan, "options": options}), text=True, capture_output=True, check=False)
                        if scenario == "success":
                            self.assertEqual(process.returncode, 0, process.stdout)
                        else:
                            self.assertNotEqual(process.returncode, 0)
                        self.assertEqual(process.stderr, "")
                        result = json.loads(process.stdout)["value"]
                    else:
                        try:
                            if surface == "sync":
                                result = await asyncio.to_thread(client.run, plan=plan, options=options)
                            else:
                                result = await async_client.run(plan=plan, options=options)
                        except ChainError as error:
                            self.assertNotEqual(scenario, "success")
                            result = error.result
                    self.assertEqual(result["state"], {"success":"succeeded", "missing":"failed", "denied":"partial"}[scenario])
                    if scenario == "success":
                        self.assertEqual(result["remote_effect"], "committed")
                        self.assertTrue(result["reports_complete"])
                        for step in result["steps"]:
                            raw = client.artifact(reference=step["upstream_result"]["artifact_ref"])
                            document = json.loads(bytes(raw))
                            if step["operation"] == "database.read":
                                self.assertEqual(step["operation_version"], 1)
                                self.assertEqual(document["read_loss"]["status"], "complete")
                            else:
                                self.assertEqual(step["operation_version"], 2)
                                self.assertEqual(document["schema_version"], 3)
                    else:
                        self.assertNotEqual(result["steps"][1]["remote_effect"], "committed")
                    results.append([(s["state"], s["remote_effect"], s["counters"]["rows_read"], s.get("error", {}).get("category") if s.get("error") else None) for s in result["steps"]])
            self.assertEqual(results[0], results[1], scenario)
            self.assertEqual(results[1], results[2], scenario)


@unittest.skipUnless(os.environ.get("PLENORA_DATABASE_FIXTURE") and os.environ.get("PLENORA_CHAIN_BIN"), "requires installed surfaces and Rust-generated database fixture")
class DatabaseAdmission(unittest.IsolatedAsyncioTestCase):
    async def test_database_profile_rejects_same_invalid_call_and_read2_without_connection(self):
        fixture = json.loads(Path(os.environ["PLENORA_DATABASE_FIXTURE"]).read_text(encoding="utf-8-sig"))
        secret = "host=127.0.0.1 port=1 dbname=not_running connect_timeout=1"
        config = {"database": secret, "adapter": fixture["adapter"], "connections": {"DB": {"destination": "ref:destination", "secret": secret}}}
        client = Client(config, profile="database")
        async_client = await AsyncClient.create(config, profile="database")
        with self.assertRaises(ChainError) as sync_error:
            client.call("not-a-function")
        with self.assertRaises(ChainError) as async_error:
            await async_client.call("not-a-function")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            process = await asyncio.to_thread(subprocess.run, [os.environ["PLENORA_CHAIN_BIN"], "database-call", str(path)], input=json.dumps({"function":"not-a-function"}), text=True, capture_output=True, check=False)
            self.assertEqual(process.returncode, 2)
            self.assertEqual(process.stderr, "")
            self.assertEqual(json.loads(process.stdout)["error"], sync_error.exception.code)
        self.assertEqual(sync_error.exception.code, async_error.exception.code)
        plan = fixture["plan"]
        plan["steps"][0]["operation_version"] = 2
        config["adapter"]["authorized_plans"] = [json.dumps(plan)]
        with self.assertRaises(ChainError):
            Client(config, profile="database")

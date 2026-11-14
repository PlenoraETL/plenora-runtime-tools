"""Installed CLI/wheel invoke IO candidate Rust, with real PostgreSQL journal in CI."""
import asyncio
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid
from plenora_runtime import Client, AsyncClient, ChainError

FIXTURE = os.environ.get("PLENORA_IO_FIXTURE")
BIN = os.environ.get("PLENORA_CHAIN_BIN")

@unittest.skipUnless(FIXTURE and BIN, "requires installed CLI/wheel and generated IO fixture")
class IoAdmission(unittest.IsolatedAsyncioTestCase):
    async def test_io_profile_same_admission_and_no_private_versions(self):
        fixture = json.loads(Path(FIXTURE).read_text(encoding="utf-8-sig"))
        config = {"database":"host=127.0.0.1 port=1 dbname=absent connect_timeout=1", "adapter":fixture["adapter"]}
        client = Client(config, profile="io")
        other = await AsyncClient.create(config, profile="io")
        with self.assertRaises(ChainError) as sync_error:
            client.call("not-a-function")
        with self.assertRaises(ChainError) as async_error:
            await other.call("not-a-function")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            process = await asyncio.to_thread(subprocess.run, [BIN,"io-call",str(path)], input=json.dumps({"function":"not-a-function"}), text=True, capture_output=True, check=False)
            self.assertEqual(process.returncode,2)
            self.assertEqual(process.stderr,"")
            self.assertEqual(json.loads(process.stdout)["error"],sync_error.exception.code)
        self.assertEqual(sync_error.exception.code,async_error.exception.code)
        fixture["plan"]["steps"][1]["operation_version"]=1
        config["adapter"]["authorized_plans"]=[json.dumps(fixture["plan"])]
        with self.assertRaises(ChainError):
            Client(config, profile="io")

@unittest.skipUnless(FIXTURE and BIN and os.environ.get("PLENORA_POSTGRES_URL"), "requires real PostgreSQL CI service and installed CLI/wheel")
class IoReal(unittest.IsolatedAsyncioTestCase):
    async def test_cli_sync_async_real_io_same_results_and_conflict(self):
        fixture = json.loads(Path(FIXTURE).read_text(encoding="utf-8-sig"))
        for conflict in (False,True):
            results=[]
            for surface in ("cli","sync","async"):
                with tempfile.TemporaryDirectory() as directory:
                    root=Path(directory)
                    config={"database":os.environ["PLENORA_POSTGRES_URL"],"adapter":copy.deepcopy(fixture["adapter"])}
                    config["adapter"]["staging_directory"]=str(root)
                    config["adapter"]["destinations"]["ref:destination"]["directory"]=str(root/"bundle")
                    if conflict:
                        (root/"bundle").mkdir()
                        (root/"bundle"/"sentinel").write_bytes(b"untouched")
                    client=Client(config,profile="io");client.migrate()
                    other=await AsyncClient.create(config,profile="io")
                    plan=json.dumps(fixture["plan"])
                    options={"run_id":str(uuid.uuid4()),"idempotency_key":None,"deadline_ms":None}
                    if surface=="cli":
                        path=root/"config.json";path.write_text(json.dumps(config),encoding="utf-8")
                        process=await asyncio.to_thread(subprocess.run,[BIN,"io-call",str(path)],input=json.dumps({"function":"run","plan":plan,"options":options}),text=True,capture_output=True,check=False)
                        self.assertEqual(process.stderr,"")
                        self.assertEqual(process.returncode==0,not conflict)
                        result=json.loads(process.stdout)["value"]
                    else:
                        try:
                            result=await asyncio.to_thread(client.run,plan=plan,options=options) if surface=="sync" else await other.run(plan=plan,options=options)
                        except ChainError as error:
                            self.assertTrue(conflict);result=error.result
                    self.assertEqual(result["state"]=="succeeded",not conflict)
                    self.assertEqual((root/"bundle"/"data.ipc").exists(),not conflict)
                    if conflict:
                        self.assertEqual((root/"bundle"/"sentinel").read_bytes(),b"untouched")
                    for step in result["steps"]:
                        raw=client.artifact(reference=step["upstream_result"]["artifact_ref"])
                        document=json.loads(bytes(raw))
                        self.assertEqual(step["operation_version"],2)
                        self.assertEqual(len(step["reports"]),2)
                        self.assertIsInstance(document,dict)
                    results.append([(s["state"],s["remote_effect"],s["counters"]["rows_read"],s["error"]["category"] if s["error"] else None) for s in result["steps"]])
            self.assertEqual(results[0],results[1]);self.assertEqual(results[1],results[2])

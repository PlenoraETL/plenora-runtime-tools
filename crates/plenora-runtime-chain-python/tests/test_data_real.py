"""Installed runtime wrappers execute the pinned Rust data.run@3 binding."""
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

@unittest.skipUnless(os.environ.get("PLENORA_DATA_FIXTURE") and os.environ.get("PLENORA_POSTGRES_URL") and os.environ.get("PLENORA_CHAIN_BIN"), "requires PostgreSQL and installed wrappers")
class DataReal(unittest.IsolatedAsyncioTestCase):
    async def test_real_data_has_same_result_and_effect_evidence_on_all_wrappers(self):
        fixture=json.loads(Path(os.environ["PLENORA_DATA_FIXTURE"]).read_text(encoding="utf-8-sig"))
        for corrupt in (False, True):
            adapter=copy.deepcopy(fixture["adapter"])
            if corrupt:
                adapter["artifacts"]["ref:source"]=[0]
            config={"database":os.environ["PLENORA_POSTGRES_URL"],"adapter":adapter}
            client=Client(config,profile="data")
            client.migrate()
            asynchronous=await AsyncClient.create(config,profile="data")
            results=[]
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/"config.json"
                path.write_text(json.dumps(config),encoding="utf-8")
                for surface in ("cli","sync","async"):
                    options={"run_id":str(uuid.uuid4()),"idempotency_key":None,"deadline_ms":None}
                    plan=json.dumps(fixture["plan"])
                    if surface=="cli":
                        proc=await asyncio.to_thread(subprocess.run,[os.environ["PLENORA_CHAIN_BIN"],"data-call",str(path)],input=json.dumps({"function":"run","plan":plan,"options":options}),text=True,capture_output=True,check=False)
                        self.assertEqual(proc.returncode==0,not corrupt)
                        self.assertEqual(proc.stderr,"")
                        result=json.loads(proc.stdout)["value"]
                    else:
                        try:
                            result=await asyncio.to_thread(client.run,plan=plan,options=options) if surface=="sync" else await asynchronous.run(plan=plan,options=options)
                        except ChainError as error:
                            self.assertTrue(corrupt)
                            result=error.result
                    self.assertEqual(result["state"],"failed" if corrupt else "succeeded")
                    if not corrupt:
                        for report in result["steps"][0]["reports"]:
                            document=report["document"]
                            raw=client.artifact(reference=document["artifact_ref"])
                            decoded=json.loads(bytes(raw))
                            if report["kind"]=="fidelity":
                                self.assertEqual(decoded["contract"],"plenora-data-localized-effects-v1")
                    results.append([(s["state"],s["remote_effect"],s["counters"]["rows_read"],s["counters"]["rows_produced"]) for s in result["steps"]])
            self.assertEqual(results[0],results[1])
            self.assertEqual(results[1],results[2])

@unittest.skipUnless(os.environ.get("PLENORA_DATA_FIXTURE") and os.environ.get("PLENORA_CHAIN_BIN"), "requires generated DATA fixture and installed CLI")
class DataAdmission(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_call_is_rejected_without_a_primary(self):
        fixture=json.loads(Path(os.environ["PLENORA_DATA_FIXTURE"]).read_text(encoding="utf-8-sig"))
        config={"database":"host=127.0.0.1 port=1 dbname=absent connect_timeout=1","adapter":fixture["adapter"]}
        client=Client(config,profile="data")
        asynchronous=await AsyncClient.create(config,profile="data")
        with self.assertRaises(ChainError) as sync_error:
            client.call("not-a-function")
        with self.assertRaises(ChainError) as async_error:
            await asynchronous.call("not-a-function")
        self.assertEqual(sync_error.exception.code,async_error.exception.code)

        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"config.json"
            path.write_text(json.dumps(config),encoding="utf-8")
            proc=await asyncio.to_thread(subprocess.run,[os.environ["PLENORA_CHAIN_BIN"],"data-call",str(path)],input=json.dumps({"function":"not-a-function"}),text=True,capture_output=True,check=False)
            self.assertEqual(proc.returncode,2)
            self.assertEqual(proc.stderr,"")
            self.assertEqual(json.loads(proc.stdout)["error"],sync_error.exception.code)

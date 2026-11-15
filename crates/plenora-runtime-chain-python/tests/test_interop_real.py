"""Installed composition admission; full five-surface execution is the Rust CI campaign."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from plenora_runtime import Client, AsyncClient, ChainError

@unittest.skipUnless(os.environ.get("PLENORA_INTEROP_FIXTURE") and os.environ.get("PLENORA_CHAIN_BIN"), "requires generated interop fixture and installed binary")
class InteropAdmission(unittest.IsolatedAsyncioTestCase):
    async def test_same_real_composite_profile_on_installed_surfaces(self):
        fixture=json.loads(Path(os.environ["PLENORA_INTEROP_FIXTURE"]).read_text(encoding="utf-8-sig"))
        config={"database":"host=127.0.0.1 port=1 dbname=absent connect_timeout=1","adapter":fixture["adapter"]}
        sync=Client(config,profile="interop")
        asynchronous=await AsyncClient.create(config,profile="interop")
        with self.assertRaises(ChainError) as first: sync.call("not-a-function")
        with self.assertRaises(ChainError) as second: await asynchronous.call("not-a-function")
        self.assertEqual(first.exception.code,second.exception.code)
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/"config.json";path.write_text(json.dumps(config),encoding="utf-8")
            response=subprocess.run([os.environ["PLENORA_CHAIN_BIN"],"interop-call",str(path)],input=json.dumps({"function":"not-a-function"}),text=True,encoding="utf-8",capture_output=True,check=False,timeout=30)
            self.assertEqual(response.returncode,2);self.assertEqual(response.stderr,"")
            self.assertEqual(json.loads(response.stdout)["error"],first.exception.code)

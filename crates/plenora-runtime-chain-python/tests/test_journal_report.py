"""Exact raw-byte recovery from installed surfaces after the real Rust fault-injection probe."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from plenora_runtime import AsyncClient, Client

@unittest.skipUnless(os.environ.get("PLENORA_JOURNAL_REPORT") and os.environ.get("PLENORA_CHAIN_BIN"),
                     "requires the real PostgreSQL journal fixture and installed surfaces from CI")
class InstalledJournal(unittest.IsolatedAsyncioTestCase):
    async def test_large_original_report_sync_async_and_binary(self):
        source = await asyncio.to_thread(Path(os.environ["PLENORA_JOURNAL_REPORT"]).read_text, encoding="utf-8")
        fixture = json.loads(source)
        config, run_id, expected = fixture["config"], fixture["run_id"], fixture["expected"]
        self.assertGreater(len(expected), 400 * 1024)
        sync, asynchronous = Client(config), AsyncClient(config)
        def binary(cursor):
            with tempfile.TemporaryDirectory(dir=Path(os.environ["PLENORA_JOURNAL_REPORT"]).parent) as folder:
                profile = Path(folder) / "config.json"
                profile.write_text(json.dumps(config), encoding="utf-8")
                result = subprocess.run([os.environ["PLENORA_CHAIN_BIN"], "chain", "report", "--run", run_id,
                    "--report-index", str(cursor["index"]), "--offset", str(cursor["offset"]),
                    "--config", str(profile), "--json"], text=True, encoding="utf-8", capture_output=True, check=False)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.stderr, "")
                response = json.loads(result.stdout)
                self.assertIsNone(response["error"])
                return response["value"]
        for surface in ("sync", "async", "binary"):
            cursor, actual = {"index": 0, "offset": 0}, []
            while cursor is not None:
                if surface == "sync":
                    page = await asyncio.to_thread(sync.report, run_id=run_id, cursor=cursor)
                elif surface == "async":
                    page = await asynchronous.report(run_id=run_id, cursor=cursor)
                else:
                    page = await asyncio.to_thread(binary, cursor)
                self.assertEqual(page["cursor"], cursor)
                self.assertEqual(page["report_bytes"], len(expected))
                self.assertEqual(page["total_reports"], 1)
                self.assertLessEqual(len(page["bytes"]), 64 * 1024)
                actual.extend(page["bytes"])
                cursor = page["next"]
            self.assertEqual(actual, expected, surface)

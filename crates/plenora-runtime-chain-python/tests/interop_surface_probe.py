"""Called by the real PostgreSQL/NATS S4 campaign against installed artifacts."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from plenora_runtime import Client, AsyncClient, ChainError

request = json.load(sys.stdin)
surface = sys.argv[1]
config, plan, options = request["config"], json.dumps(request["plan"]), request["options"]
async def execute():
    if surface == "cli":
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "interop.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            result = subprocess.run([os.environ["PLENORA_CHAIN_BIN"], "interop-call", str(path)],
                input=json.dumps({"function":"run", "plan":plan, "options":options}),
                text=True, encoding="utf-8", capture_output=True, check=False, timeout=180)
            if result.stderr: raise AssertionError("CLI emitted unexpected diagnostics")
            response = json.loads(result.stdout)
            if result.returncode != (0 if response["error"] is None else 3): raise AssertionError("chain exit/result mismatch")
            return response
    try:
        if surface == "sync":
            value = Client(config, profile="interop").run(plan=plan, options=options)
        elif surface == "async":
            client = await AsyncClient.create(config, profile="interop")
            value = await client.run(plan=plan, options=options)
        else:
            raise AssertionError("unknown qualification surface")
        return {"error":None,"value":value}
    except ChainError as error:
        return {"error":error.code,"value":error.result}
json.dump(asyncio.run(execute()), sys.stdout)

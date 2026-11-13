"""Native scripted qualification clients; no ETL library is attested by this profile."""
import asyncio
import json
from ._native import FUNCTIONS, NativeClient, NativeRemoteClient


class ChainError(Exception):
    """Data-free failure with the complete domain result retained separately."""

    def __init__(self, code, result=None):
        super().__init__(code)
        self.code = code
        self.result = result


class Client:
    """Synchronous native client. The extension releases the GIL during Rust work."""

    def __init__(self, config):
        self._native = NativeClient(json.dumps(config))

    def migrate(self):
        """Apply the confirmed primary-store migration before accepting runs."""
        return self._native.migrate()

    @classmethod
    def remote(cls, server, subject):
        """Bind the explicit NATS qualification endpoint."""
        instance = cls.__new__(cls)
        instance._native = NativeRemoteClient(server, subject)
        return instance

    def call(self, function, **arguments):
        """Call the same closed Request dispatcher as Rust and CLI."""
        reply = json.loads(self._native.call(json.dumps({"function": function, **arguments})))
        if reply["error"] is not None:
            raise ChainError(reply["error"], reply["value"])
        return reply["value"]

    def __getattr__(self, name):
        if name not in FUNCTIONS:
            raise AttributeError(name)
        return lambda **arguments: self.call(name, **arguments)


class AsyncClient:
    """Async waits do not block the event loop. Cancelling a wait does not cancel a run."""

    def __init__(self, config):
        self._sync = Client(config)

    async def migrate(self):
        """Migrate without blocking the event loop."""
        return await asyncio.to_thread(self._sync.migrate)

    @classmethod
    async def remote(cls, server, subject):
        """Connect without blocking the event loop."""
        instance = cls.__new__(cls)
        instance._sync = await asyncio.to_thread(Client.remote, server, subject)
        return instance

    async def call(self, function, **arguments):
        """Execute native work on a worker thread; durable cancellation is an explicit call."""
        return await asyncio.to_thread(self._sync.call, function, **arguments)

    def __getattr__(self, name):
        if name not in FUNCTIONS:
            raise AttributeError(name)
        async def invoke(**arguments):
            return await self.call(name, **arguments)
        return invoke


__all__ = ["Client", "AsyncClient", "ChainError", "FUNCTIONS"]

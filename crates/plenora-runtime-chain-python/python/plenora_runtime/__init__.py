"""Native scripted qualification clients; no ETL library is attested by this profile."""
import asyncio
import json
from ._native import FUNCTIONS, NativeClient, NativeRemoteClient, NativeEventCursor, failure_json


class ChainError(Exception):
    """Data-free failure with the complete domain result retained separately."""

    def __init__(self, code, result=None, failure=None):
        super().__init__(code)
        self.code = code
        self.result = result
        self.failure = failure if failure is not None else json.loads(failure_json(code, json.dumps(result)))
        for field in ("kind", "category", "phase", "remote_effect", "effect_scope", "retry", "run_id", "domain_code"):
            setattr(self, field, self.failure[field])


def _native_call(function, *arguments):
    """Native construction, migration and transport errors use the same Rust projection."""
    try:
        return function(*arguments)
    except RuntimeError as error:
        raise ChainError(str(error)) from None
    except (TypeError, ValueError, OverflowError):
        raise ChainError("invalid") from None


def _encode(value):
    try:
        return json.dumps(value, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        raise ChainError("invalid") from None


class Client:
    """Synchronous native client. The extension releases the GIL during Rust work."""

    def __init__(self, config):
        self._native = _native_call(NativeClient, _encode(config))

    def migrate(self):
        """Apply the confirmed primary-store migration before accepting runs."""
        return _native_call(self._native.migrate)

    @classmethod
    def remote(cls, server, subject, *, observation_timeout_ms=10_000):
        """Bind the explicit NATS qualification endpoint."""
        instance = cls.__new__(cls)
        instance._native = _native_call(NativeRemoteClient, server, subject, _encode(observation_timeout_ms))
        return instance

    def call(self, function, **arguments):
        """Call the same closed Request dispatcher as Rust and CLI."""
        reply = json.loads(_native_call(self._native.call, _encode({"function": function, **arguments})))
        if reply["error"] is not None:
            raise ChainError(reply["error"], reply["value"], reply["failure"])
        return reply["value"]

    def iter_events(self, run_id, cursor=0, limit=256):
        """Yield currently persisted events; an empty page ends this enumeration."""
        pages = _EventPages(run_id, cursor, limit)
        while True:
            page = pages.accept(self.call(**pages.request()))
            if not page:
                return
            yield from page

    def __getattr__(self, name):
        if name not in FUNCTIONS:
            raise AttributeError(name)
        return lambda **arguments: self.call(name, **arguments)


class AsyncClient:
    """Async waits do not block the event loop. Cancelling a wait does not cancel a run."""

    def __init__(self, config):
        # Configuration is consumed on first await; construction performs no native work.
        self._config = config
        self._initialization = None

    async def _ready(self):
        if self._initialization is None:
            self._initialization = asyncio.create_task(asyncio.to_thread(Client, self._config))
        return await asyncio.shield(self._initialization)

    @classmethod
    async def create(cls, config):
        """Construct and validate off the event loop; returns only after initialization."""
        instance = cls(config)
        await instance._ready()
        return instance

    async def migrate(self):
        """Migrate without blocking the event loop."""
        return await asyncio.to_thread((await self._ready()).migrate)

    @classmethod
    async def remote(cls, server, subject, *, observation_timeout_ms=10_000):
        """Connect without blocking the event loop."""
        instance = cls.__new__(cls)
        instance._initialization = asyncio.create_task(asyncio.to_thread(Client.remote, server, subject, observation_timeout_ms=observation_timeout_ms))
        await asyncio.shield(instance._initialization)
        return instance

    async def call(self, function, **arguments):
        """Execute native work on a worker thread; durable cancellation is an explicit call."""
        return await asyncio.to_thread((await self._ready()).call, function, **arguments)

    async def iter_events(self, run_id, cursor=0, limit=256):
        """Yield persisted events without blocking the loop or cancelling the run."""
        pages = _EventPages(run_id, cursor, limit)
        while True:
            value = await self.call(**pages.request())
            page = await asyncio.to_thread(pages.accept, value)
            if not page:
                return
            for event in page:
                yield event

    def __getattr__(self, name):
        if name not in FUNCTIONS:
            raise AttributeError(name)
        async def invoke(**arguments):
            return await self.call(name, **arguments)
        return invoke


class _EventPages:
    """Cursor validation and sequence advancement live exclusively in Rust."""

    def __init__(self, run_id, cursor, limit):
        try:
            self._cursor = NativeEventCursor(json.dumps({"function": "events", "run_id": run_id, "cursor": cursor, "limit": limit}))
        except (RuntimeError, OverflowError, TypeError):
            raise ChainError("invalid") from None

    def request(self):
        return json.loads(self._cursor.request())

    def accept(self, value):
        try:
            return json.loads(self._cursor.accept(json.dumps(value)))
        except RuntimeError as error:
            raise ChainError(str(error)) from None


__all__ = ["Client", "AsyncClient", "ChainError", "FUNCTIONS"]

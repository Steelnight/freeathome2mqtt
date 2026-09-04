"""A fake MQTT broker: a real `amqtt` broker bound to a local port (docs/10 §3.4).

Used instead of mocking the MQTT client so tests exercise real sockets, real retained-message
semantics and real subscription state -- see docs/10 §2 and §3.4. Docker is not available in this
environment (no `docker.sock`), so this in-process broker is the "fallback so the suite runs
without Docker" docs/10 §3.4 calls for, standing in for a Mosquitto container.
"""

from __future__ import annotations

import contextlib
import socket
from collections.abc import AsyncIterator

from amqtt.broker import Broker


def free_port() -> int:
    """Ask the OS for a currently-unused localhost port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.asynccontextmanager
async def running_fake_broker(port: int | None = None) -> AsyncIterator[Broker]:
    """Start a fresh in-process broker; yields it with `.port` set to where it's listening.

    Pass an explicit `port` to simulate a broker restart (same address, fresh process, any
    retained state and subscriptions gone) -- exactly the P-42 / P-27 scenario.
    """
    resolved_port = port if port is not None else free_port()
    broker = Broker(
        {
            "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{resolved_port}"}},
            "plugins": {
                "amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True},
            },
        }
    )
    broker.port = resolved_port  # type: ignore[attr-defined]
    await broker.start()
    try:
        yield broker
    finally:
        await broker.shutdown()

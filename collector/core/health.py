from __future__ import annotations

import logging
from typing import Awaitable, Callable

from aiohttp import web

log = logging.getLogger(__name__)


async def serve_health(
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
    check_ready: Callable[[], Awaitable[tuple[bool, str]]],
) -> None:
    async def health(_: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def ready(_: web.Request) -> web.Response:
        ok, message = await check_ready()
        if not ok:
            return web.Response(status=503, text=message)
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/ready", ready)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("Health probes listening on %s:%s (/health /ready)", host, port)

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from aiohttp import web

log = logging.getLogger(__name__)


class _QuietKubeProbeFilter(logging.Filter):
    _MARKERS = ('"GET /health ', '"GET /ready ', "GET /health ", "GET /ready ")

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != "aiohttp.access":
            return True
        line = str(getattr(record, "first_request_line", "") or record.getMessage())
        return not any(marker in line for marker in self._MARKERS)


def _silence_probe_access_logs() -> None:
    access_logger = logging.getLogger("aiohttp.access")
    if any(isinstance(f, _QuietKubeProbeFilter) for f in access_logger.filters):
        return
    access_logger.addFilter(_QuietKubeProbeFilter())


async def serve_health(
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
    check_ready: Callable[[], Awaitable[tuple[bool, str]]],
) -> None:
    _silence_probe_access_logs()

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
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("Health probes listening on %s:%s (/health /ready)", host, port)

"""Discord webhook alerting.

Single channel. If DISCORD_WEBHOOK_URL is unset, messages are logged to
stderr only. No multi-channel framework — one webhook, one destination.
"""

from __future__ import annotations

import sys

import aiohttp

from kalshi_collector.logging import get_logger

log = get_logger(__name__)

_MAX_EMBED_LEN = 4096


class DiscordAlerter:
    def __init__(self, webhook_url: str | None) -> None:
        self._url = webhook_url
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "DiscordAlerter":
        if self._url:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._session:
            await self._session.close()

    async def send(self, title: str, description: str, level: str = "warning") -> None:
        """Send an alert. Falls back to stderr if webhook is not configured."""
        colour = {"info": 0x5865F2, "warning": 0xFEE75C, "error": 0xED4245}.get(level, 0xAAAAAA)

        if self._url is None or self._session is None:
            print(f"[ALERT:{level.upper()}] {title}: {description}", file=sys.stderr)
            return

        payload = {
            "embeds": [
                {
                    "title": title[:256],
                    "description": description[:_MAX_EMBED_LEN],
                    "color": colour,
                }
            ]
        }

        try:
            async with self._session.post(self._url, json=payload) as resp:
                if resp.status not in (200, 204):
                    body = await resp.text()
                    log.warning("discord_send_failed", status=resp.status, body=body[:200])
        except aiohttp.ClientError:
            log.exception("discord_http_error", title=title)

"""Agentic Agency Hermes companion. Registration is the only host-facing entry point."""

import json
import logging
import os
from pathlib import Path
import stat
from urllib.parse import urlsplit

from .channel import Channel
from .hooks import Hooks
from .version import __version__

logger = logging.getLogger(__name__)


def _home(ctx):
    # Neither supported PluginContext version exposes a home accessor today.
    # Prefer one if a future host provides it, then use Hermes' context-local resolver.
    accessor = getattr(ctx, "get_hermes_home", None)
    if callable(accessor):
        return Path(accessor()).expanduser()
    from hermes_constants import get_hermes_home
    return get_hermes_home()


def load_config(home):
    """Read a small regular file with owner-only permissions; errors omit all values."""
    path = Path(home) / "agentic_agency" / "plugin.json"
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(fd, "r", encoding="utf-8") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) not in (0o600, 0o400):
            raise ValueError("config must be a regular file with mode 0600 or 0400")
        if info.st_size > 16 * 1024:
            raise ValueError("config too large")
        config = json.loads(source.read(16 * 1024 + 1))
    if not isinstance(config, dict):
        raise ValueError("invalid config")
    url, secret = config.get("bff_url"), config.get("secret")
    if not isinstance(url, str) or not isinstance(secret, str) or not secret.strip():
        raise ValueError("missing config fields")
    parsed = urlsplit(url)
    parsed.port  # Validate malformed/out-of-range ports at configuration time.
    if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment):
        raise ValueError("invalid BFF URL")
    if parsed.scheme == "http" and parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
        raise ValueError("non-loopback BFF requires HTTPS")
    if any(ord(char) < 32 or ord(char) > 126 for char in secret) or any(ord(c) < 33 for c in url):
        raise ValueError("invalid config characters")
    return {"bff_url": url.rstrip("/"), "secret": secret}


def _reasoning_setting():
    try:
        from hermes_cli import config
        load = getattr(config, "load_config_readonly", None) or getattr(config, "load_config", None)
        value = load().get("plugins", {}).get("stream_reasoning_deltas", False)
        return value if isinstance(value, bool) else None
    except Exception:
        return None


def _hermes_version():
    try:
        from hermes_cli.version_info import get_version_info
        version = get_version_info().base_version
        if version and version not in ("unknown", "0.0.0"):
            return str(version)
    except Exception:
        pass
    try:
        from hermes_cli import __version__ as version
        return str(version) if version and version not in ("unknown", "0.0.0") else None
    except Exception:
        return None


def register(ctx, **kwargs):
    """Register telemetry independently from optional scheduled delivery support."""
    channel = None
    try:
        home = _home(ctx)
        config = load_config(home)
        from hermes_cli.plugins import VALID_HOOKS
        available = [name for name in ("post_api_request", "on_stream_delta") if name in VALID_HOOKS]
        if not available or not callable(getattr(ctx, "on_unload", None)):
            raise ValueError("host lacks observer lifecycle support")
        channel = Channel(config, profile=getattr(ctx, "profile_name", "default"),
                          hermes_version=_hermes_version(),
                          stream_reasoning_deltas=_reasoning_setting())
        observers = Hooks(channel)
        # Install cleanup before acquiring hooks or starting a worker.
        ctx.on_unload(channel.stop)
        for name in available:
            ctx.register_hook(name, getattr(observers, name))
            channel.hooks.append(name)
        from .platform import register_delivery
        register_delivery(ctx, config, home, channel)
        channel.start()
    except Exception:
        if channel is not None:
            channel.stop()
        # One warning per registration, with no exception/config content or secret.
        logger.warning("Agentic Agency plugin inert: check private plugin.json and host hook support")

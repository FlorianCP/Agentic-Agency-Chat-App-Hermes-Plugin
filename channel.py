"""Bounded, best-effort telemetry. Network and JSON work stays off hook threads."""

from collections import deque
import json
import threading
import time
import urllib.request

from .version import __version__

MAX_EVENTS = 256
MAX_BYTES = 256 * 1024
QUEUE_SIZE = 4096
# Bounds coalescing/splitting work and leaves room for the envelope and identities.
CHUNK_CHARS = 16 * 1024


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Channel:
    def __init__(self, config, *, profile="default", hermes_version=None,
                 stream_reasoning_deltas=None, hooks=(), queue_size=QUEUE_SIZE):
        self.url = config["bff_url"].rstrip("/") + "/v1/plugin/events"
        self._secret = config["secret"]
        self.metadata = {"plugin_version": __version__, "hermes_version": hermes_version,
                         "profile": profile}
        self.hooks = list(hooks)
        self.stream_reasoning_deltas = stream_reasoning_deltas
        self._queue = deque()
        self._capacity = queue_size
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._reasoning_pending = 0
        self.dropped = 0
        self._seq = 0
        self._backoff = 0.5
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        self.thread = None
        self.delivery_platform = False
        self.inbox_session_id = None

    def enqueue(self, event):
        # Fixed work per hook: no encoding, coalescing, file access, or network calls.
        with self._lock:
            if self._stop.is_set():
                return
            if len(self._queue) >= self._capacity:
                old = self._queue.popleft()
                self._reasoning_pending -= old["type"] == "reasoning"
                self.dropped += 1
            self._queue.append(event)
            self._reasoning_pending += event["type"] == "reasoning"
        self._wake.set()

    def set_delivery(self, enabled, inbox_session_id):
        with self._lock:
            self.delivery_platform = enabled
            self.inbox_session_id = inbox_session_id

    def hello(self):
        with self._lock:
            dropped = self.dropped
            delivery_platform = self.delivery_platform
            inbox_session_id = self.inbox_session_id
        return {"type": "hello", "ts": time.time(), "hooks": self.hooks,
                **self.metadata, "stream_reasoning_deltas": self.stream_reasoning_deltas,
                "dropped_events": dropped, "delivery_platform": delivery_platform,
                "inbox_session_id": inbox_session_id}

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True, name="agentic-agency-bff")
        self.thread.start()

    def stop(self, **kwargs):
        self._stop.set()
        self._wake.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=1.5)

    def _encode(self, events):
        return json.dumps({**self.metadata, "events": events}, ensure_ascii=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")

    def _batch(self, *, initial_hello=False):
        # Reserve first-connect hello outside the drop-oldest queue, so startup
        # traffic cannot evict discovery before the BFF has seen this plugin.
        events = [self.hello()] if initial_hello else []
        # Work is bounded by a batch, regardless of producer rate.
        for _ in range(MAX_EVENTS):
            with self._lock:
                if not self._queue:
                    break
                raw = self._queue.popleft()
                self._reasoning_pending -= raw["type"] == "reasoning"
            event = dict(raw)
            remainder = None
            if event["type"] == "reasoning":
                text = event["delta"]
                event["delta"] = text[:CHUNK_CHARS]
                if len(text) > CHUNK_CHARS:
                    remainder = {**raw, "delta": text[CHUNK_CHARS:]}
                self._seq += 1
                event["seq"] = self._seq
            merge = (events and event["type"] == "reasoning"
                     and events[-1]["type"] == "reasoning"
                     and all(events[-1][key] == event[key] for key in ("session_id", "turn_id"))
                     and len(events[-1]["delta"]) + len(event["delta"]) <= CHUNK_CHARS)
            if merge:
                combined = {**events[-1], "delta": events[-1]["delta"] + event["delta"]}
                candidate = events[:-1] + [combined]
            else:
                candidate = events + [event]
            try:
                fits = len(candidate) <= MAX_EVENTS and len(self._encode(candidate)) <= MAX_BYTES
            except (ValueError, TypeError):
                fits = False
            if not fits:
                if not events:
                    with self._lock:
                        self.dropped += 1
                    continue  # Oversize identities/runtime are dropped, never wedge delivery.
                self._prepend(raw)
                break
            events = candidate
            if remainder:
                self._prepend(remainder)
        return self._encode(events) if events else None

    def _prepend(self, event):
        with self._lock:
            # Producers may have filled the slot while the worker encoded the batch.
            if len(self._queue) >= self._capacity:
                self.dropped += 1
                return  # This worker-held event is older than everything queued.
            self._queue.appendleft(event)
            self._reasoning_pending += event["type"] == "reasoning"

    def _post(self, body):
        request = urllib.request.Request(self.url, data=body, method="POST", headers={
            "Authorization": "Bearer " + self._secret, "Content-Type": "application/json",
        })
        with self._opener.open(request, timeout=1.0) as response:
            if response.status != 204:
                raise OSError("BFF did not acknowledge telemetry")

    def _deliver(self, body):
        try:
            self._post(body)
        except Exception:
            delay = self._backoff
            self._backoff = min(self._backoff * 2, 30.0)
            return delay
        self._backoff = 0.5
        return None

    def _run(self):
        heartbeat = time.monotonic() + 60
        deadline = time.monotonic() + 1
        pending = None
        retry_at = 0
        initial_hello = True
        while not self._stop.is_set():
            now = time.monotonic()
            if now >= heartbeat:
                self.enqueue(self.hello())
                heartbeat = now + 60
            if pending is not None and now < retry_at:
                # Wake for heartbeats without shortening the network backoff.
                self._stop.wait(min(retry_at, heartbeat) - now)
                continue
            if pending is None:
                with self._lock:
                    reasoning = bool(self._reasoning_pending)
                if reasoning:
                    deadline = min(deadline, now + 0.15)
                if now < deadline:
                    self._wake.wait(min(deadline, heartbeat) - now)
                    self._wake.clear()
                    continue
                pending = self._batch(initial_hello=initial_hello)
            if pending is not None:
                delay = self._deliver(pending)
                if delay is not None:
                    retry_at = time.monotonic() + delay
                    continue
                pending = None
                initial_hello = False
            with self._lock:
                queued = bool(self._queue)
            deadline = time.monotonic() + (0 if queued else 1)

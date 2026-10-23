"""Observer mapping only. Future pre_llm_call notes belong beside these observers."""

import logging
import threading
import time

logger = logging.getLogger(__name__)


class Hooks:
    def __init__(self, channel):
        self.channel = channel
        self._last_debug = float("-inf")
        self._log_lock = threading.Lock()

    def _failed(self):
        # Never log kwargs or exception text: either could contain private content.
        try:
            with self._log_lock:
                now = time.monotonic()
                if now - self._last_debug >= 60:
                    self._last_debug = now
                    logger.debug("Agentic Agency observer discarded an invalid event")
        except Exception:
            pass

    def post_api_request(self, **kwargs):
        try:
            # Hermes uses post_auxiliary_call, with aux_task, for non-main-loop calls.
            if "aux_task" in kwargs:
                return
            session, turn = kwargs.get("session_id"), kwargs.get("turn_id")
            call = kwargs.get("api_call_count")
            if not isinstance(session, str) or not session or not isinstance(turn, str) or not turn:
                return
            if type(call) is not int or call < 1:
                return
            provider, model = kwargs.get("provider"), kwargs.get("model")
            response_model = kwargs.get("response_model")
            if (not isinstance(provider, str) or not provider.strip()
                    or not isinstance(model, str) or not model.strip()):
                return
            self.channel.enqueue({
                "type": "runtime", "ts": time.time(), "session_id": session,
                "turn_id": turn, "api_call": call, "provider": provider, "model": model,
                "response_model": response_model if isinstance(response_model, str) else None,
            })
        except Exception:
            self._failed()

    def on_stream_delta(self, **kwargs):
        try:
            if kwargs.get("kind") != "reasoning" or "aux_task" in kwargs:
                return
            session, turn, delta = (kwargs.get(key) for key in ("session_id", "turn_id", "delta"))
            if not all(isinstance(value, str) and value for value in (session, turn, delta)):
                return
            self.channel.enqueue({
                "type": "reasoning", "ts": time.time(), "session_id": session,
                "turn_id": turn, "delta": delta,
            })
        except Exception:
            self._failed()

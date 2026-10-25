"""Host messaging-platform registration; all blocking work uses a worker thread."""

import asyncio
from collections.abc import Mapping
import json
import logging
import time

from .delivery import Deliveries, WRAPPER, diagnostic, middleware, parse_target, unwrap

logger = logging.getLogger(__name__)
CRON_MEDIA_WINDOW = 60


def approval_prompt(session_info):
    # Hermes exposes the API platform, but no app-client marker, at prompt render time.
    if not isinstance(session_info, Mapping) or session_info.get("platform") != "api_server":
        return ""
    return (
        "When the user may be asked to approve a step (deleting or overwriting data, "
        "changing access or settings, stopping services, running downloaded or hidden instructions, "
        "sharing secrets, removing software, or rewriting saved history), first write in the chat, "
        "before calling the tool. Use one or two short sentences in the language the user is writing in, "
        "with no command text or jargon: say what you will do, why it helps, and any relevant loss or "
        "interruption. Never claim it is safe or already approved."
    )


def cron_job_id(content, metadata):
    job_id = metadata.get("job_id") if isinstance(metadata, dict) else None
    if job_id:
        return job_id
    if isinstance(content, str) and unwrap(content)[1]:
        return WRAPPER.match(content)[2] or None
    return None


DESCRIPTION = (
    "Proactively reach the user, for example from scheduled jobs or other channels. "
    "Never use this tool to answer in the current app chat. A cron job's final answer "
    "is already delivered; do not also notify with the same content. "
    "Target inbox (default) or an existing app session id."
)


def notify(service, args, **kwargs):
    try:
        if not isinstance(args, dict):
            raise ValueError("invalid arguments")
        delivery_id = service.enqueue(args.get("target", "inbox"), args.get("message"), source="agent")
        return json.dumps({"success": True, "delivery_id": delivery_id, "status": "queued"})
    except Exception as error:
        diagnostic("delivery.notify_rejected", profile=getattr(service, "profile", ""), reason="enqueue_failed", error_type=type(error).__name__)
        return json.dumps({"success": False, "error": "Delivery could not be queued; check target and plugin configuration"})


def register_delivery(ctx, config, home, channel):
    """Return None on unsupported hosts, without disturbing telemetry registrations."""
    service = None
    handles = []
    def track(handle):
        if handle is None:
            raise ValueError("host registration not acquired")
        handles.append(handle)
        return handle
    try:
        import fcntl  # noqa: F401 - fail closed without cross-process spool locking
        from hermes_state import SessionDB
        from hermes_state_registry import acquire, release
        from gateway.config import Platform
        from gateway.platforms.base import BasePlatformAdapter, SendResult
        from gateway.platform_registry import PlatformEntry
        required = ("register_platform", "register_middleware", "register_tool", "on_unload")
        if (not all(callable(getattr(ctx, name, None)) for name in required)
                or not callable(getattr(SessionDB, "append_completion_note", None))
                or not callable(getattr(SessionDB, "resolve_resume_session_id", None))
                or "cron_deliver_env_var" not in PlatformEntry.__dataclass_fields__):
            raise ValueError("delivery APIs unavailable")
        service = Deliveries(config, home, profile=getattr(ctx, "profile_name", "default"),
                             on_inbox=lambda value: channel.set_delivery(True, value))
        track(ctx.on_unload(service.stop))

        class Adapter(BasePlatformAdapter):
            splits_long_messages = True
            MAX_MESSAGE_LENGTH = 128 * 1024

            def __init__(self, platform_config):
                super().__init__(config=platform_config, platform=Platform("agentic_agency"))
                self._cron_media = {}

            async def connect(self, *, is_reconnect=False):
                try:
                    self._mark_connected()
                    return True
                except Exception:
                    return False

            async def disconnect(self):
                mark = getattr(self, "_mark_disconnected", None)
                if callable(mark):
                    mark()
                else:
                    self._running = False

            async def send(self, chat_id, content, reply_to=None, metadata=None):
                job_id = cron_job_id(content, metadata)
                if not job_id:
                    return self._ignore_host_send()
                metadata = dict(metadata if isinstance(metadata, dict) else {}, job_id=job_id)
                try:
                    delivery_id = await asyncio.to_thread(service.enqueue, chat_id, content, metadata, source="cron")
                    self._prune_cron_media()
                    if len(self._cron_media) >= 500 and chat_id not in self._cron_media:
                        self._cron_media.pop(next(iter(self._cron_media)))
                    self._cron_media[chat_id] = (time.monotonic(), job_id)
                    return SendResult(success=True, message_id=delivery_id)
                except Exception as error:
                    diagnostic("delivery.send_rejected", profile=getattr(service, "profile", ""), job_id=str(job_id)[:128],
                               session_id=(parse_target(chat_id) or (None,))[0], reason="enqueue_failed", error_type=type(error).__name__)
                    return SendResult(success=False, error="Agentic Agency delivery could not be queued")

            async def get_chat_info(self, chat_id):
                return {"name": "Assistant", "type": "dm", "id": chat_id}

            def _ignore_host_send(self):
                logger.info("Agentic Agency ignored non-cron host send")
                return SendResult(success=True)

            def _prune_cron_media(self):
                now = time.monotonic()
                self._cron_media = {chat: item for chat, item in self._cron_media.items()
                                    if 0 <= now - item[0] <= CRON_MEDIA_WINDOW}

            async def _media(self, chat_id, path, metadata):
                self._prune_cron_media()
                job_id = cron_job_id(None, metadata)
                # v0.22.5 cron sends files after text but omits job_id on files.
                # Admit that fallback only for this chat within 60s of a durable
                # cron text send; host notices never establish this window.
                if not job_id and chat_id in self._cron_media:
                    job_id = self._cron_media[chat_id][1]
                if not job_id:
                    diagnostic("delivery.media_rejected", profile=getattr(service, "profile", ""),
                               session_id=(parse_target(chat_id) or (None,))[0], reason="no_cron_identity_within_60s", window_seconds=CRON_MEDIA_WINDOW)
                    return self._ignore_host_send()
                metadata = dict(metadata if isinstance(metadata, dict) else {}, job_id=job_id)
                try:
                    delivery_id = await asyncio.to_thread(service.enqueue_media, chat_id, path, metadata)
                    return SendResult(success=True, message_id=delivery_id)
                except Exception:
                    return SendResult(success=False, error="Agentic Agency attachment could not be queued")

            async def send_image_file(self, chat_id, image_path, caption=None, reply_to=None, metadata=None, **kwargs):
                return await self._media(chat_id, image_path, metadata)

            async def send_document(self, chat_id, file_path, caption=None, file_name=None, reply_to=None, metadata=None, **kwargs):
                return await self._media(chat_id, file_path, metadata)

            async def send_video(self, chat_id, video_path, caption=None, reply_to=None, metadata=None, **kwargs):
                return await self._media(chat_id, video_path, metadata)

            async def send_voice(self, chat_id, audio_path, caption=None, reply_to=None, metadata=None, **kwargs):
                return await self._media(chat_id, audio_path, metadata)

        track(ctx.register_platform("agentic_agency", "Assistant", Adapter, lambda: True,
            validate_config=lambda value: True, is_connected=lambda value: True,
            cron_deliver_env_var="AGENTIC_AGENCY_HOME_CHANNEL", parse_target_ref_fn=parse_target,
            validate_target_ref_fn=service.validate, pii_safe=True, max_message_length=128 * 1024,
            env_enablement_fn=lambda: {"home_channel": {"chat_id": "inbox", "name": "Assistant"}}))
        def delivery_middleware(*args, **kwargs):
            return middleware(*args, **dict(kwargs, profile_name=getattr(ctx, "profile_name", "default")))
        track(ctx.register_middleware("tool_request", delivery_middleware))
        track(ctx.register_tool("agentic_agency_notify", "agentic_agency", {
            "name": "agentic_agency_notify", "description": DESCRIPTION,
            "parameters": {"type": "object", "properties": {
                "message": {"type": "string", "description": "Message to deliver"},
                "target": {"type": "string", "description": "inbox or an app session id"}},
                "required": ["message"], "additionalProperties": False}},
            lambda args, **kwargs: notify(service, args, **kwargs), description=DESCRIPTION))
        section = getattr(ctx, "register_system_prompt_section", None)
        if callable(section):
            track(section("agentic_agency.delivery", "Scheduled jobs created from an app chat deliver into that chat automatically. "
                "Use deliver=\"agentic_agency\" for the user's Assistant inbox. "
                "A cron job's final answer is already delivered; never also notify with the same content. "
                "Files for the app must be shared with the app's media:// outbox convention inside the answer text "
                "(as the agent rules describe), not as MEDIA: attachments."))
        channel.set_delivery(True, service.inbox_session_id)
        service.start()
        return service
    except Exception:
        if service:
            service.stop()
        channel.set_delivery(False, None)
        for handle in reversed(handles):
            try:
                handle.dispose()
            except Exception:
                pass
        logger.warning("Agentic Agency delivery inert: host APIs or private delivery storage unavailable")
        return None

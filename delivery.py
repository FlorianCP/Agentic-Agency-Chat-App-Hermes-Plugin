"""Durable scheduled/proactive delivery, independent from lossy telemetry."""

from contextlib import contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import stat
import threading
import time
import urllib.parse
import urllib.request
import uuid

from .channel import _NoRedirect

logger = logging.getLogger(__name__)
MAX_AGE = 24 * 60 * 60
MAX_ATTEMPTS = 50
MAX_MEDIA_BYTES = 20 * 1024 * 1024
MAX_RECORDS = 500
MAX_RECORD_BYTES = 256 * 1024
MAX_SPOOL_BYTES = 64 * 1024 * 1024
DEDUP_SECONDS = 600
TARGET = re.compile(r"[A-Za-z0-9_-]{1,256}\Z")
WRAPPER = re.compile(r'\ACronjob Response: ([^\r\n]+)\n\(job_id: ([^\r\n]*)\)\n-------------\n\n')


def parse_target(ref):
    if not isinstance(ref, str):
        return None
    value = ref.strip()
    if value == "agentic_agency":
        value = "inbox"
    elif value.startswith("agentic_agency:"):
        value = value[len("agentic_agency:"):]
    return (value, None) if TARGET.fullmatch(value) else None


def unwrap(content, job_id=None):
    """Strip only the exact host wrapper, including its matching management footer."""
    match = WRAPPER.match(content)
    if not match or (job_id and match[2] != job_id):
        return content, None
    footer = ('\n\nTo stop or manage this job, send me a new message '
              '(e.g. "stop reminder ' + match[1] + '").')
    if not content.endswith(footer):
        return content, None
    return content[match.end():-len(footer)], match[1]


def transcript(text, source, job_name=None, attachment=False):
    if attachment:
        header = "[Delivered by the assistant: attachment]"
    elif source == "cron":
        # A job name must never terminate the one-line machine-readable header.
        name = " ".join(str(job_name or "Scheduled job").split()).replace('"', "'")[:256]
        header = '[Delivered by the assistant: scheduled job "' + name + '"]'
    else:
        header = "[Delivered by the assistant]"
    return header + "\n" + text


def preview(text):
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"media://\S+", "[attachment]", text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[`*_#>~]", "", text)
    return " ".join(text.split())[:512]


def atomic_json(path, value):
    body = json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()
    if len(body) > MAX_RECORD_BYTES:
        raise ValueError("delivery record too large")
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def private_dir(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("delivery directory must be private")


@contextmanager
def file_lock(path, *, blocking=True):
    # Hermes gateway hosts are Unix. A host without flock stays telemetry-only.
    import fcntl
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield
    finally:
        os.close(fd)


def read_json(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "r", encoding="utf-8") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > MAX_RECORD_BYTES:
            raise ValueError("invalid delivery file")
        return json.load(source)


def media_identity(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


@contextmanager
def media_file(path, *, max_bytes=MAX_MEDIA_BYTES):
    if hasattr(path, "read"):
        path.seek(0)
        yield path
        return
    # Refuse symlinks in every path component, including directory aliases.
    candidate = Path(path).absolute()
    if any(part.is_symlink() for part in (candidate, *candidate.parents)):
        raise ValueError("invalid media path")
    fd = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > max_bytes:
            raise ValueError("invalid media file")
        yield source


class Host:
    """Explicit-home access only; background workers never read an ambient profile."""
    def __init__(self, home, profile):
        self.home = Path(home)
        self.profile = profile

    @contextmanager
    def database(self):
        from hermes_state_registry import acquire, release
        db = acquire(self.home / "state.db")
        try:
            yield db
        finally:
            release(db)

    def resolve(self, session_id):
        with self.database() as db:
            resolved = db.resolve_resume_session_id(session_id)
            session = db.get_session(resolved)
            if (not session or session.get("source") != "api_server"
                    or session.get("profile_name") not in (None, self.profile)):
                raise ValueError("target must be an api_server session of this profile")
            return resolved

    def create_inbox(self, session_id):
        if not isinstance(session_id, str) or not TARGET.fullmatch(session_id) or session_id == "inbox":
            raise ValueError("invalid inbox identity")
        with self.database() as db:
            existing = db.get_session(session_id)
            if existing and (existing.get("source") != "api_server"
                             or existing.get("profile_name") not in (None, self.profile)):
                raise ValueError("inbox identity belongs to another channel")
            db.create_session(session_id, "api_server", profile_name=self.profile)
            db.set_session_title(session_id, "Assistant")

    def append(self, record):
        with self.database() as db:
            session_id = self.resolve(record["session_id"])
            db.append_completion_note(session_id, record["delivery_id"],
                transcript(record["text"], record["source"], record.get("job_name"), record.get("attachment", False)),
                "agentic_agency_delivery", {"delivery_id": record["delivery_id"],
                    "source": record["source"], "job_id": record.get("job_id"),
                    "job_name": record.get("job_name")})
            return session_id

    def job_name(self, job_id):
        from cron.jobs import get_job, use_cron_store
        with use_cron_store(self.home):
            job = get_job(job_id)
            return job.get("name") if job else None


class Deliveries:
    def __init__(self, config, home, profile="default", host=None, on_inbox=None):
        self.home = Path(home)
        self.root = self.home / "agentic_agency"
        private_dir(self.root)
        self.spool = self.root / "spool"
        private_dir(self.spool)
        self.host = host or Host(home, profile)
        self.profile = profile
        self.url = config["bff_url"]
        self._secret = config["secret"]
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        self.on_inbox = on_inbox or (lambda value: None)
        self.inbox_session_id = None
        try:
            state = read_json(self.root / "state.json")
            self.inbox_session_id = state["inbox_session_id"]
        except FileNotFoundError:
            pass
        self._stop = threading.Event()
        self._wake = threading.Event()
        self.thread = None

    def inbox(self):
        with file_lock(self.root / "state.lock"):
            path = self.root / "state.json"
            try:
                state = read_json(path)
                session_id = self.host.resolve(state["inbox_session_id"])
            except FileNotFoundError:
                session_id = "api_" + str(int(time.time())) + "_" + uuid.uuid4().hex[:8]
                # Persist the chosen identity first so a crash cannot create orphan inboxes.
                atomic_json(path, {"inbox_session_id": session_id})
                self.host.create_inbox(session_id)
            except ValueError:
                # Complete an interrupted first-use creation with the same identity.
                # Deleted/tombstoned identities may reject recreation; only then replace.
                session_id = state["inbox_session_id"]
                try:
                    self.host.create_inbox(session_id)
                except Exception:
                    session_id = "api_" + str(int(time.time())) + "_" + uuid.uuid4().hex[:8]
                    atomic_json(path, {"inbox_session_id": session_id})
                    self.host.create_inbox(session_id)
            self.inbox_session_id = session_id
            self.on_inbox(session_id)
            return session_id

    def target(self, target):
        parsed = parse_target(target)
        if not parsed:
            raise ValueError("invalid target")
        inbox = parsed[0] == "inbox"
        return (self.inbox() if inbox else self.host.resolve(parsed[0])), inbox

    def validate(self, target):
        try:
            parsed = parse_target(target)
            if not parsed:
                return False
            if parsed[0] != "inbox":
                self.host.resolve(parsed[0])
            return True
        except Exception:
            return False

    def enqueue_media(self, target, path, metadata=None):
        # Hermes already applied its media-path policy. Retain the exact file
        # identity, then revalidate on drain rather than duplicate up to 20 MiB.
        path = str(Path(path).absolute())
        with media_file(path) as source:
            identity = media_identity(os.fstat(source.fileno()))
        return self.enqueue(target, "[attachment]", metadata, media={"path": path, "identity": identity})

    def enqueue(self, target, content, metadata=None, *, source=None, media=None):
        if self._stop.is_set():
            raise ValueError("delivery service stopped")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("empty delivery")
        if len(content.encode("utf-8")) > MAX_RECORD_BYTES // 2:
            raise ValueError("delivery too large")
        session_id, inbox = self.target(target)
        metadata = metadata or {}
        job_id = metadata.get("job_id")
        # Hermes' standalone retry can re-enter the live adapter without the
        # original job metadata. Its exact wrapper still carries that identity.
        if source is None and not job_id:
            wrapped_text, wrapped_name = unwrap(content)
            if wrapped_name:
                job_id = WRAPPER.match(content)[2]
        source = source or ("cron" if job_id else "agent")
        text, job_name = unwrap(content, job_id) if source == "cron" else (content, None)
        if job_id and not job_name:
            try:
                job_name = self.host.job_name(job_id)
            except Exception:
                pass
        if job_id and not job_name:
            job_name = job_id
        now = int(time.time())
        fingerprint = hashlib.sha256(json.dumps([job_id, session_id, content, media], ensure_ascii=True).encode()).hexdigest()
        with file_lock(self.root / "spool.lock"):
            # A killed writer may leave its pre-rename temporary behind. All
            # record writers hold this lock, so these files are now abandoned.
            for temporary in self.spool.glob(".*.json.*"):
                temporary.unlink()
            paths = list(self.spool.glob("*.json"))
            size = 0
            count = 0
            for path in paths:
                record = read_json(path)
                if record.get("done") and now - record["created_at"] >= DEDUP_SECONDS:
                    path.unlink()
                    continue
                count += 1
                size += path.stat().st_size
                if record.get("fingerprint") == fingerprint and now - record["created_at"] < DEDUP_SECONDS:
                    return record["delivery_id"]
            if count >= MAX_RECORDS or size + MAX_RECORD_BYTES > MAX_SPOOL_BYTES:
                raise ValueError("delivery spool full")
            delivery_id = uuid.uuid4().hex
            record = {"delivery_id": delivery_id, "profile": self.profile, "session_id": session_id,
                "inbox": inbox, "source": source, "text": text, "preview": preview(text),
                "created_at": now, "fingerprint": fingerprint, "attempts": 0, "retry_at": 0}
            if job_id:
                record["job_id"] = str(job_id)[:128]
                record["job_name"] = str(job_name)[:256]
            if media:
                record.update(attachment=True, media=media, notify=False)
            # Serialize producers across processes and tolerate equal/backward clocks.
            last = max((int(p.name.split("-", 1)[0]) for p in self.spool.glob("*.json")
                        if p.name.split("-", 1)[0].isdigit()), default=0)
            order = max(time.time_ns(), last + 1)
            atomic_json(self.spool / (f"{order:020d}-" + delivery_id + ".json"), record)
        self._wake.set()
        return delivery_id

    def post(self, path, body, content_type="application/json"):
        request = urllib.request.Request(self.url + path, data=body, method="POST", headers={
            "Authorization": "Bearer " + self._secret, "Content-Type": content_type})
        with self.opener.open(request, timeout=2) as response:
            if response.status != 200:
                raise OSError("BFF delivery not acknowledged")
            value = response.read(16 * 1024 + 1)
            if len(value) > 16 * 1024:
                raise ValueError("BFF response too large")
            return json.loads(value)

    def upload_media(self, path, *, max_bytes=MAX_MEDIA_BYTES, name=None):
        """Upload a host-policy-approved regular file with a bounded read."""
        with media_file(path, max_bytes=max_bytes) as source:
            identity = media_identity(os.fstat(source.fileno()))
            body = source.read(max_bytes + 1)
            if identity != media_identity(os.fstat(source.fileno())):
                raise ValueError("media changed during read")
        if len(body) > max_bytes:
            raise ValueError("media too large")
        result = self.post("/v1/plugin/media?" + urllib.parse.urlencode({"name": name or Path(path).name}),
                           body, "application/octet-stream")
        ref = result.get("ref")
        if not isinstance(ref, str) or not ref.startswith("media://deliveries/") or re.search(r"\s", ref):
            raise ValueError("invalid media reference")
        return ref

    def drop(self, path, reason):
        with file_lock(self.root / "spool.lock"):
            path.unlink(missing_ok=True)
        # Fixed reasons only: no content, paths, session IDs, or exception strings.
        logger.warning("Agentic Agency delivery dropped: %s", reason)

    def drain(self):
        # Strict enqueue order per resolved session, including deferred retries.
        with file_lock(self.root / "worker.lock", blocking=False):
            blocked = set()
            for path in sorted(self.spool.glob("*.json")):
                if self._stop.is_set():
                    return
                record = read_json(path)
                if record.get("done"):
                    continue
                if time.time() - record["created_at"] >= MAX_AGE or record["attempts"] >= MAX_ATTEMPTS:
                    self.drop(path, "retry budget exhausted")
                    continue
                session = record["session_id"]
                try:
                    try:
                        record["session_id"] = self.host.resolve(session)
                    except ValueError:
                        self.drop(path, "target unavailable")
                        continue
                    session = record["session_id"]
                    if session in blocked:
                        continue
                    if record.get("retry_at", 0) > time.time():
                        blocked.add(session)
                        continue
                    if record.get("attachment") and not record.get("media_ref"):
                        media = record["media"]
                        try:
                            with media_file(media["path"]) as source:
                                if media_identity(os.fstat(source.fileno())) != media["identity"]:
                                    raise ValueError("media changed")
                                # Upload the validated descriptor, avoiding a path-swap race.
                                ref = self.upload_media(source, name=Path(media["path"]).name)
                        except (ValueError, FileNotFoundError):
                            self.drop(path, "attachment unavailable")
                            continue
                        record.update(media_ref=ref, text=ref, preview="[attachment]")
                        with file_lock(self.root / "spool.lock"):
                            atomic_json(path, record)
                    record["session_id"] = self.host.append(record)
                    fields = ("delivery_id", "profile", "session_id", "inbox", "source", "job_id",
                              "job_name", "preview", "created_at", "notify")
                    body = json.dumps({k: record[k] for k in fields if k in record}, ensure_ascii=True).encode()
                    if len(body) > 16 * 1024:
                        raise ValueError("receipt too large")
                    result = self.post("/v1/plugin/deliveries", body)
                    if result.get("delivery_id") != record["delivery_id"] or "cursor" not in result:
                        raise ValueError("invalid delivery acknowledgement")
                    record["done"] = True
                    for key in ("text", "preview", "media", "media_ref"):
                        record.pop(key, None)
                except Exception:
                    record["attempts"] += 1
                    if record["attempts"] >= MAX_ATTEMPTS or time.time() - record["created_at"] >= MAX_AGE:
                        self.drop(path, "retry budget exhausted")
                        continue
                    record["retry_at"] = time.time() + min(0.5 * 2 ** min(record["attempts"], 6), 30)
                    blocked.add(session)
                with file_lock(self.root / "spool.lock"):
                    atomic_json(path, record)

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True, name="agentic-agency-delivery")
        self.thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.drain()
            except Exception:
                logger.debug("Agentic Agency delivery deferred")
            self._wake.wait(0.5)
            self._wake.clear()

    def stop(self, **kwargs):
        self._stop.set()
        self._wake.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=1.5)


def middleware(tool_name="", args=None, session_id="", **kwargs):
    try:
        from gateway.session_context import get_session_env
        if (tool_name == "cronjob_manage" and isinstance(args, dict)
                and args.get("action") in ("create", "update")
                and ((args.get("action") == "create" and args.get("deliver") in (None, "origin"))
                     or (args.get("action") == "update" and args.get("deliver") == "origin"))
                and get_session_env("HERMES_SESSION_PLATFORM") == "api_server"
                and TARGET.fullmatch(session_id)):
            return {"args": {**args, "deliver": "agentic_agency:" + session_id}}
    except Exception:
        pass
    return None

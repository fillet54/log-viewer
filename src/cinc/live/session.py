from __future__ import annotations

import queue
import threading
import uuid
from datetime import datetime, timezone


class SessionManager:
    def __init__(self, store, log_type, source):
        self._store, self._log_type, self._source = store, log_type, source
        self._thread = None
        self._stop_event = threading.Event()
        self._active_session_id = None
        self._subscribers = []
        self._lock = threading.Lock()

    @property
    def log_type_id(self):
        return self._log_type.id

    @property
    def active_session_id(self):
        return self._active_session_id

    def start(self):
        if self._thread and self._thread.is_alive():
            raise RuntimeError("A live capture is already active")
        session_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc)
        header = self._source.start(session_id)
        record = self._store.create_log(
            self._log_type.id,
            "",
            f"Live capture {started:%Y-%m-%d %H:%M}",
            {"payload_header": header},
            source="live",
            status="active",
            started_at=started,
            log_id=session_id,
        )
        self._active_session_id = record.id
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._poll_loop, args=(record.id, header), daemon=True
        )
        self._thread.start()
        return record.id

    def stop(self):
        thread = self._thread
        if thread is None:
            return
        self._stop_event.set()
        thread.join(timeout=10)
        self._source.stop()
        self._thread = None

    def _poll_loop(self, log_id, header):
        try:
            while not self._stop_event.wait(self._source.poll_interval):
                rows = self._source.poll()
                if rows:
                    events = self._log_type.normalize_events({"events": rows, **header})
                    self._store.append_events(
                        log_id, events, source="live", tags=("live",)
                    )
                    with self._lock:
                        for subscriber in self._subscribers:
                            subscriber.put_nowait({"type": "events", "events": events})
                if self._source.finished():
                    break
        finally:
            self._store.complete_log(log_id)
            with self._lock:
                for subscriber in self._subscribers:
                    subscriber.put_nowait(None)
                self._subscribers.clear()
            self._active_session_id = None

    def subscribe(self):
        subscriber = queue.Queue(maxsize=500)
        with self._lock:
            self._subscribers.append(subscriber)
        return subscriber

    def unsubscribe(self, subscriber):
        with self._lock:
            if subscriber in self._subscribers:
                self._subscribers.remove(subscriber)


class LiveSessionRegistry:
    def __init__(self, registry, store):
        self._managers = {}
        for log_type in registry.all():
            source = log_type.create_live_source()
            if source is not None:
                self._managers[log_type.id] = SessionManager(store, log_type, source)

    def managers(self):
        return dict(self._managers)

    def get(self, log_type_id):
        return self._managers.get(log_type_id)

    def active(self):
        return [
            (key, manager.active_session_id)
            for key, manager in self._managers.items()
            if manager.active_session_id
        ]

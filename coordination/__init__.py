"""直播多方消息协调服务包。

用法::

    from coordination import Coordinator

    svc = Coordinator()
    svc.ingest({"at": 0, "kind": "reference", "record": {...}})
    svc.inspect(session="s1", at_media=101)
"""

from coordinator import (  # noqa: F401
    CAPTURE_EXPIRED,
    CAPTURE_INVALIDATED,
    CAPTURE_SUCCEEDED,
    CAPTURE_WAITING,
    OFFICIAL,
    PREVIEW,
    REPLACED,
    STALE,
    WITHDRAWN,
    Coordinator,
    CoordinationError,
    CoordinationService,
    LiveCoordinationService,
    MessageCoordinator,
    Service,
    create_service,
    handle_message,
    process_message,
    process_messages,
    run_file,
    run_jsonl_text,
    span_contains,
)

__all__ = [
    "Coordinator",
    "CoordinationService",
    "MessageCoordinator",
    "LiveCoordinationService",
    "Service",
    "CoordinationError",
    "create_service",
    "handle_message",
    "process_message",
    "process_messages",
    "run_file",
    "run_jsonl_text",
    "span_contains",
    "PREVIEW",
    "OFFICIAL",
    "WITHDRAWN",
    "REPLACED",
    "STALE",
    "CAPTURE_WAITING",
    "CAPTURE_SUCCEEDED",
    "CAPTURE_INVALIDATED",
    "CAPTURE_EXPIRED",
]

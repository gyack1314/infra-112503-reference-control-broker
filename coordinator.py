"""节目参考、观众指令与本地仿真边缘回执之间的消息协调服务。

只使用 Python 标准库；运行时流转的全部是合成标识（ref / frame / request /
session），不生成也不读取图像。

公开入口
--------
``Coordinator``（别名 ``CoordinationService`` / ``MessageCoordinator`` /
``Service``）：

* ``ingest(row)`` —— 连续接入 ``{"at", "kind", "record"}`` 信封消息；
* 类型化方法 ``publish_reference`` / ``submit_command`` / ``verify_reference``
  / ``replace_reference`` / ``receive_edge`` / ``confirm_display`` /
  ``resume_session`` / ``inspect`` / ``query``；
* 查询方法 ``query_displayed`` / ``query_previews`` / ``query_resync`` /
  ``query_captures``。

命令行::

    python3 coordinator.py examples/base-replaced-after-preview.jsonl
    python3 coordinator.py --dir examples
    cat scenario.jsonl | python3 coordinator.py --stdin
    python3 coordinator.py --http 8765

语义要点见 README「协调状态机」一节。
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# 状态词汇
# ---------------------------------------------------------------------------

# 画面资格（frame.qualification）
PREVIEW = "preview"
"""边缘已给预览：可快速上屏，但不具备截图资格。"""

UNCONFIRMED = "unconfirmed"
"""边缘已报 ready，但节目参考链尚未确认/补齐，不能成为正式画面。"""

OFFICIAL = "official"
"""正式画面：edge ready + 参考已确认 + 关键片段齐备且未被更正。别名 available。"""

WITHDRAWN = "withdrawn"
"""预览被收回。"""

REPLACED = "replaced"
"""预览被另一画面明确替换（旧画面不得再用于截图）。"""

STALE = "stale"
"""曾经是正式画面，随后参考被更正；展示事实保留，但需要重同步。"""

REJECTED = "rejected"
"""回执/画面未通过核对，不产生上屏资格。"""

PROVISIONAL_QUALIFICATIONS = {PREVIEW, UNCONFIRMED}
DEAD_QUALIFICATIONS = {WITHDRAWN, REPLACED, STALE, REJECTED}

# 视角指令状态
VIEW_PENDING = "pending"
VIEW_PREVIEW = "preview"
VIEW_CONFIRMED = "confirmed"
VIEW_COALESCED = "coalesced"
VIEW_TIMEOUT = "timeout"
VIEW_REJECTED = "rejected"

# 截图状态
CAPTURE_WAITING = "waiting"
CAPTURE_SUCCEEDED = "succeeded"
CAPTURE_INVALIDATED = "invalidated"
CAPTURE_EXPIRED = "expired"

# 通知类型
N_PREVIEW = "preview_available"
N_SWITCH = "switch_confirmed"
N_CAPTURE_WAITING = "capture_waiting"
N_CAPTURE_OK = "capture_succeeded"
N_CAPTURE_BAD = "capture_invalidated"
N_PREVIEW_REPLACED = "preview_replaced"
N_PREVIEW_WITHDRAWN = "preview_withdrawn"
N_RESYNC = "resync_required"
N_VIEW_TIMEOUT = "intent_timeout"
N_CAPTURE_EXPIRED = "capture_expired"

RETRACT_NOTIFICATIONS = {N_PREVIEW_WITHDRAWN, N_PREVIEW_REPLACED}

SUPPORTED_KINDS = {
    "reference",
    "command",
    "verify",
    "replace",
    "edge",
    "display",
    "resume",
    "inspect",
}


class CoordinationError(ValueError):
    """输入消息不符合协调服务契约。"""


class IndexableList(list):
    """列表语义不变，同时允许用字符串 id 直接取记录。

    ``inspect`` 的集合字段既是列表（可迭代/下标整数/JSON 序列化为数组），
    也支持 ``captures["shot-1"]`` 这样的 id 查询。
    """

    def __init__(self, items: Iterable[Any] = (), key: str = "id") -> None:
        super().__init__(items)
        self.key = key
        self._index: dict[str, Any] = {}
        for item in self:
            if isinstance(item, dict) and item.get(key) is not None:
                self._index[item[key]] = item

    def __getitem__(self, index: Any) -> Any:
        if isinstance(index, str):
            return self._index[index]
        return super().__getitem__(index)

    def get(self, index: Any, default: Any = None) -> Any:
        if isinstance(index, str):
            return self._index.get(index, default)
        return super().__getitem__(index)

    def __contains__(self, item: Any) -> bool:  # type: ignore[override]
        if isinstance(item, str):
            return item in self._index
        return super().__contains__(item)


def span_contains(span: Any, media_time: Any) -> bool:
    """媒体区间左闭右开：``span[0] <= media_time < span[1]``。"""
    if not span or media_time is None:
        return False
    try:
        return span[0] <= media_time < span[1]
    except TypeError:
        return False


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (set, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, IndexableList):
        return IndexableList((_jsonable(v) for v in value), key=value.key)
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


# ---------------------------------------------------------------------------
# 协调服务
# ---------------------------------------------------------------------------


class Coordinator:
    """三方消息协调的有状态核心。

    所有状态都保存在实例内，可连续 :meth:`ingest`；查询返回普通 JSON 兼容
    字典/列表，便于直接序列化。
    """

    def __init__(self) -> None:
        self.now: int | None = None
        self.refs: "OrderedDict[str, dict]" = OrderedDict()
        self.commands: "OrderedDict[str, dict]" = OrderedDict()
        self.frames: "OrderedDict[str, dict]" = OrderedDict()
        self.sessions: "OrderedDict[str, dict]" = OrderedDict()
        # request id -> 边缘作业代际：edge_epoch -> 该代最初接受的 (ref, at)
        self.job_generations: dict[str, dict[Any, dict]] = {}
        self.job_max_epoch: dict[str, Any] = {}
        # 全部服务侧事实（不对客户端确认序号），便于审计/排障
        self.event_log: list[dict] = []
        # 每次 ingest 的输出也可在这里取到
        self.processed: list[dict] = []

    # -- 基础工具 -----------------------------------------------------------

    def _session(self, session_id: str) -> dict:
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                "id": session_id,
                "commands": [],
                "view_intents": [],
                "active_view": None,
                "captures": [],
                "displayed": [],
                "displayed_frame_ids": [],
                "notifications": [],
                "acked_seq": 0,
                "resync_items": OrderedDict(),
                "resync_seq": 0,
            }
        return self.sessions[session_id]

    def _log_event(self, event_kind: str, at: int | None, **payload: Any) -> dict:
        event = {"at": at, "kind": event_kind}
        event.update(payload)
        self.event_log.append(event)
        return event

    def _notify(
        self,
        session_id: str,
        kind: str,
        at: int | None,
        **payload: Any,
    ) -> dict:
        """给某播放会话追加一条单调序号通知。"""
        sess = self._session(session_id)
        seq = len(sess["notifications"]) + 1
        note = {
            "seq": seq,
            "id": f"{session_id}#{seq}",
            "session": session_id,
            "kind": kind,
            "at": at,
        }
        note.update(payload)
        sess["notifications"].append(note)
        return note

    @staticmethod
    def _require(record: dict, *fields: str) -> None:
        missing = [f for f in fields if f not in record]
        if missing:
            raise CoordinationError(f"record 缺少字段：{', '.join(missing)}")

    def _advance_time(self, at: int) -> list[dict]:
        """推进逻辑时钟并处理超过 deadline 的未完成指令。"""
        if self.now is None or at > self.now:
            self.now = at
        expired: list[dict] = []
        for cmd in list(self.commands.values()):
            deadline = cmd.get("deadline")
            if deadline is None or at <= deadline:  # 截止秒点本身仍有效
                continue
            if cmd["kind"] == "view" and cmd["status"] in (
                VIEW_PENDING,
                VIEW_PREVIEW,
            ):
                cmd["status"] = VIEW_TIMEOUT
                cmd["timeout_at"] = at
                if cmd.get("session"):
                    expired.append(
                        self._notify(
                            cmd["session"],
                            N_VIEW_TIMEOUT,
                            at,
                            command=cmd["id"],
                            intent=cmd["id"],
                            deadline=deadline,
                            ref=cmd.get("ref"),
                        )
                    )
            elif cmd["kind"] == "capture" and cmd["status"] == CAPTURE_WAITING:
                cmd["status"] = CAPTURE_EXPIRED
                cmd["resolved_at"] = at
                cmd["reason"] = "deadline_exceeded"
                if cmd.get("session"):
                    expired.append(
                        self._notify(
                            cmd["session"],
                            N_CAPTURE_EXPIRED,
                            at,
                            command=cmd["id"],
                            capture=cmd["id"],
                            frame=cmd.get("frame"),
                            reason="deadline_exceeded",
                            deadline=deadline,
                        )
                    )
        return expired

    # -- 参考链与有效性 -----------------------------------------------------

    def _root_key(self, ref_id: str | None) -> str | None:
        """沿 base 找到关键参考 id；base 缺失时返回其名字。"""
        seen: set[str] = set()
        cur = ref_id
        while cur is not None and cur in self.refs and cur not in seen:
            seen.add(cur)
            ref = self.refs[cur]
            base = ref.get("base")
            if not base:
                return cur
            cur = base
        return cur

    def _ref_invalid_reason(self, ref_id: str | None) -> str | None:
        """结构性失效原因（永久不可用）；未确认/未补齐不算失效。"""
        if ref_id is None:
            return "ref_missing"
        ref = self.refs.get(ref_id)
        if ref is None:
            return "ref_missing"
        if ref.get("replaced_by"):
            return "ref_replaced"
        base = ref.get("base")
        if base:
            base_ref = self.refs.get(base)
            if base_ref is None:
                return None  # 关键片段尚未补齐：是“不完整”，不是被更正失效
            reason = self._ref_invalid_reason(base)
            if reason:
                return "base_" + reason if not reason.startswith("base_") else reason
        return None

    def _ref_confirmation_gap(
        self, ref_id: str, at_media: int | None = None
    ) -> str | None:
        """还不能成为正式画面的“确认/补齐”缺口；一切就绪返回 None。"""
        ref = self.refs.get(ref_id)
        if ref is None:
            return "ref_missing"
        invalid = self._ref_invalid_reason(ref_id)
        if invalid:
            return invalid
        if not ref.get("verified"):
            return "ref_unverified"
        base_id = ref.get("base")
        if base_id:
            base = self.refs.get(base_id)
            if base is None:
                return "base_missing"  # 关键片段尚未补齐
            if not base.get("verified"):
                return "base_unverified"
            if at_media is not None and not span_contains(base.get("span"), at_media):
                return "base_span_mismatch"
        return None

    def refs_related(self, a: str | None, b: str | None) -> bool:
        """两个参考版本是否属于同一条修订/更正谱系（不含跨分支）。"""
        if not a or not b or a == b:
            return a == b
        ra, rb = self.refs.get(a), self.refs.get(b)
        if ra is None or rb is None:
            return False
        if ra.get("branch") != rb.get("branch"):
            return False

        seen: set[str] = set()
        frontier = [a]
        links: dict[str, list[str]] = {}
        for ref_id, ref in self.refs.items():
            for key in ("supersedes", "base", "replaced_by"):
                other = ref.get(key)
                if other:
                    links.setdefault(ref_id, []).append(other)
                    links.setdefault(other, []).append(ref_id)
            if ref.get("replaces"):
                links.setdefault(ref_id, []).append(ref["replaces"])
        while frontier:
            cur = frontier.pop()
            if cur == b:
                return True
            if cur in seen:
                continue
            seen.add(cur)
            frontier.extend(links.get(cur, []))
        return False

    def compatible_reference(
        self, command: dict, edge_ref_id: str
    ) -> tuple[bool, str | None]:
        """边缘实际使用的参考是否与指令兼容（分支隔离是硬条件）。"""
        used = self.refs.get(edge_ref_id)
        if used is None:
            return False, "unknown_ref"
        # 指令只能落在用户实际观看的节目分支上
        if used.get("branch") != command.get("branch"):
            return False, "branch_mismatch"
        cited = self.refs.get(command.get("ref"))
        if cited is not None and used.get("scene") != cited.get("scene"):
            return False, "scene_mismatch"
        if (
            cited is not None
            and cited.get("program")
            and used.get("program")
            and cited.get("program") != used.get("program")
        ):
            return False, "program_mismatch"
        if not span_contains(used.get("span"), command.get("at_media")):
            return False, "media_outside_span"
        if edge_ref_id == command.get("ref"):
            return True, None
        # 同一关键参考之上的增量/关键参考，视为兼容
        if self._root_key(edge_ref_id) == self._root_key(command.get("ref")):
            return True, None
        # 同分支修订/更正谱系（例如新一代边缘实例改用更正后的参考）
        if self.refs_related(command.get("ref"), edge_ref_id):
            return True, None
        return False, "incompatible_ref"

    # -- 输入入口 -----------------------------------------------------------

    def ingest(self, row: dict | str) -> dict:
        """接入一条 ``{"at","kind","record"}`` 消息，返回本次产生的结果。

        消息级契约错误会被转换成 ``accepted=False`` 的结果而不是抛出，保证
        连续接入不被单条坏消息打断；编程级参数错误仍会抛出。
        """
        if isinstance(row, str):
            row = json.loads(row)
        if not isinstance(row, dict):
            raise TypeError("消息应为 JSON 对象")
        at = row.get("at")
        kind = row.get("kind")
        record = row.get("record")
        result: dict[str, Any] = {
            "at": at,
            "kind": kind,
            "accepted": True,
            "notifications": [],
            "events": [],
        }
        try:
            if type(at) is not int or at < 0:
                raise CoordinationError("at 必须是非负整数秒点")
            if kind not in SUPPORTED_KINDS:
                raise CoordinationError(f"未登记的消息名称：{kind!r}")
            if not isinstance(record, dict):
                raise CoordinationError("record 应为对象")
            # 同一秒内按提交顺序处理；截止秒点本身仍有效，超过 deadline 才过期
            expired = self._advance_time(at)
            result["notifications"].extend(expired)

            handler = {
                "reference": self.publish_reference,
                "command": self.submit_command,
                "verify": self.verify_reference,
                "replace": self.replace_reference,
                "edge": self.receive_edge,
                "display": self.confirm_display,
                "resume": self.resume_session,
                "inspect": self.inspect_message,
            }[kind]
            extra = handler(record, at) or {}
            if kind == "resume":
                # resume 返回的是查询载荷（其中 notifications 是取回的历史
                # 通知，不是本次新产生的），单独放在 result["resume"]。
                result["resume"] = extra
            elif isinstance(extra, dict):
                new_notes = extra.pop("notifications", [])
                new_events = extra.pop("events", [])
                if new_notes:
                    result["notifications"].extend(new_notes)
                if new_events:
                    result["events"].extend(new_events)
                result.update(extra)
            result["notifications"] = _jsonable(result["notifications"])
            result["events"] = _jsonable(result["events"])
        except CoordinationError as error:
            result["accepted"] = False
            result["error"] = str(error)
            result["notifications"] = _jsonable(result.get("notifications", []))
            result["events"] = _jsonable(result.get("events", []))
        self.processed.append(result)
        return _jsonable(result)

    def ingest_many(self, rows: Iterable[dict | str]) -> list[dict]:
        return [self.ingest(row) for row in rows]

    # -- 参考消息 -----------------------------------------------------------

    def publish_reference(self, record: dict, at: int | None = None) -> dict:
        self._require(record, "ref", "scene", "branch", "span", "rev")
        ref_id = record["ref"]
        if ref_id in self.refs:
            # 重复发布同一 ref：幂等保留首版，修订请使用新 ref/rev + replace
            event = self._log_event(
                "reference_duplicate", at, ref=ref_id, detail="ref 已存在，忽略重复发布"
            )
            return {"events": [event]}
        ref = {
            "id": ref_id,
            "program": record.get("program"),
            "scene": record["scene"],
            "branch": record["branch"],
            "span": list(record["span"]),
            "rev": record["rev"],
            "base": record.get("base"),
            "key": bool(record.get("key", not record.get("base"))),
            "verified": bool(record.get("verified", False)),
            "supersedes": record.get("supersedes"),
            "replaced_by": None,
            "replaces": None,
            "published_at": at,
            "verified_at": at if record.get("verified") else None,
        }
        self.refs[ref_id] = ref
        events = [self._log_event("reference_published", at, ref=ref_id)]
        # 关键片段可能“补齐”了此前不完整的参考链，重新评估所有画面
        events.extend(self.reevaluate_frames(at))
        return {"events": events}

    def verify_reference(self, record: dict, at: int | None = None) -> dict:
        self._require(record, "ref")
        ref_id = record["ref"]
        ref = self.refs.get(ref_id)
        if ref is None:
            raise CoordinationError(f"verify 指向未发布的参考：{ref_id}")
        notifications: list[dict] = []
        events: list[dict] = []
        if not ref["verified"]:
            ref["verified"] = True
            ref["verified_at"] = at
            events.append(self._log_event("reference_verified", at, ref=ref_id))
            changed_notes, changed_events = self.reevaluate_frames(at)
            notifications.extend(changed_notes)
            events.extend(changed_events)
        return {"notifications": notifications, "events": events}

    def replace_reference(self, record: dict, at: int | None = None) -> dict:
        """节目方更正 old → new（new 必须已经发布）。"""
        self._require(record, "old", "new")
        old_id, new_id = record["old"], record["new"]
        old = self.refs.get(old_id)
        new = self.refs.get(new_id)
        events: list[dict] = []
        notifications: list[dict] = []
        if old is None:
            raise CoordinationError(f"replace 的 old 未发布：{old_id}")
        if new is None:
            raise CoordinationError(f"replace 的 new 尚未发布：{new_id}")
        if old.get("replaced_by") == new_id:
            return {"notifications": [], "events": []}  # 幂等
        old["replaced_by"] = new_id
        new["replaces"] = old_id
        events.append(
            self._log_event(
                "reference_replaced",
                at,
                old=old_id,
                new=new_id,
                branch=old.get("branch"),
                crossed_key=not old.get("base"),
            )
        )
        notes, evs = self._apply_correction_to_frames(old_id, new_id, at)
        notifications.extend(notes)
        events.extend(evs)
        return {"notifications": notifications, "events": events}

    # -- 指令 ---------------------------------------------------------------

    def submit_command(self, record: dict, at: int | None = None) -> dict:
        self._require(record, "session", "id", "branch", "at_media", "kind")
        cmd_id = record["id"]
        if cmd_id in self.commands:
            prior = self.commands[cmd_id]
            event = self._log_event(
                "command_duplicate", at, command=cmd_id, detail="指令 id 重复，保留首次提交"
            )
            return {"command": prior["view"], "events": [event]}

        session_id = record["session"]
        sess = self._session(session_id)
        kind = record["kind"]
        if kind not in ("view", "capture"):
            raise CoordinationError(f"未知指令 kind={kind!r}（应为 view/capture）")
        if record.get("ref") and record["ref"] not in self.refs:
            # 不阻断：指令携带的是客户端当时见到的身份，允许资料稍后补齐核对
            self._log_event("command_unknown_ref_cited", at, command=cmd_id, ref=record["ref"])

        cmd = {
            "id": cmd_id,
            "session": session_id,
            "kind": kind,
            "branch": record["branch"],
            "at_media": record["at_media"],
            "ref": record.get("ref"),
            "deadline": record.get("deadline"),
            "frame": record.get("frame", record.get("frame_id")),
            "at": at,
            "status": VIEW_PENDING if kind == "view" else CAPTURE_WAITING,
            "coalesced_by": None,
            "reason": None,
            "corrected": False,
            "resync_required": False,
            "current_frame": None,
            "history": [],
        }
        self.commands[cmd_id] = cmd
        sess["commands"].append(cmd_id)

        notifications: list[dict] = []
        events: list[dict] = [self._log_event("command_received", at, command=cmd_id, kind=kind)]

        if kind == "view":
            sess["view_intents"].append(cmd_id)
            # 快速连续切换：合并同会话此前“尚未完成”的视角意图
            for earlier_id in sess["view_intents"][:-1]:
                earlier = self.commands[earlier_id]
                if earlier["status"] in (VIEW_PENDING, VIEW_PREVIEW):
                    earlier["status"] = VIEW_COALESCED
                    earlier["coalesced_by"] = cmd_id
                    events.append(
                        self._log_event(
                            "intent_coalesced",
                            at,
                            session=session_id,
                            command=earlier_id,
                            coalesced_by=cmd_id,
                        )
                    )
            sess["active_view"] = cmd_id
        else:
            sess["captures"].append(cmd_id)
            self._bind_capture(cmd, at, notifications, events)

        return {
            "command": self._command_view(cmd),
            "notifications": notifications,
            "events": events,
        }

    def _bind_capture(
        self, cmd: dict, at: int, notifications: list[dict], events: list[dict]
    ) -> None:
        """把截图请求绑定到明确的画面并立即判定资格。"""
        frame_id = cmd.get("frame")
        sess = self._session(cmd["session"])
        if not frame_id:
            # 未显式给 frame 时，才回退到该会话当前画面
            active_id = sess.get("active_view")
            active = self.commands.get(active_id) if active_id else None
            frame_id = active.get("current_frame") if active else None
            cmd["frame"] = frame_id
        if not frame_id:
            # 没有可绑定的画面：保持等待（状态可查询），不单独占通知序号
            cmd["status"] = CAPTURE_WAITING
            cmd["reason"] = "frame_unknown"
            events.append(self._log_event(
                "capture_pending", at, capture=cmd["id"], reason="frame_unknown"
            ))
            return

        frame = self.frames.get(frame_id)
        if frame is None:
            # 允许截图请求先于（或乱序于）回执到达：先挂到待知名下
            cmd["status"] = CAPTURE_WAITING
            cmd["reason"] = "frame_unknown"
            self._pending_capture_index().setdefault(frame_id, []).append(cmd["id"])
            events.append(self._log_event(
                "capture_pending", at, capture=cmd["id"], frame=frame_id,
                reason="frame_unknown",
            ))
            return

        frame.setdefault("capture_ids", []).append(cmd["id"])
        self._classify_capture_against_frame(cmd, frame, at, notifications)

    def _pending_capture_index(self) -> dict[str, list[str]]:
        index = getattr(self, "_unknown_frame_captures", None)
        if index is None:
            index = {}
            self._unknown_frame_captures = index
        return index

    def _classify_capture_against_frame(
        self, cmd: dict, frame: dict, at: int, notifications: list[dict]
    ) -> None:
        q = frame["qualification"]
        if q == OFFICIAL:
            cmd["status"] = CAPTURE_SUCCEEDED
            cmd["resolved_at"] = at
            cmd["reason"] = None
            notifications.append(
                self._notify(
                    cmd["session"], N_CAPTURE_OK, at,
                    command=cmd["id"], capture=cmd["id"], frame=frame["id"],
                    ref=frame.get("ref"), at_media=frame.get("at_media"),
                )
            )
        elif q in (WITHDRAWN, REPLACED, STALE, REJECTED):
            cmd["status"] = CAPTURE_INVALIDATED
            cmd["resolved_at"] = at
            cmd["reason"] = "frame_" + q
            notifications.append(
                self._notify(
                    cmd["session"], N_CAPTURE_BAD, at,
                    command=cmd["id"], capture=cmd["id"], frame=frame["id"],
                    reason=cmd["reason"],
                )
            )
        else:
            # preview / unconfirmed：等待“同一画面”成为正式画面。
            # waiting 是可查询状态，不单独产生客户端通知序号；终态才通知。
            cmd["status"] = CAPTURE_WAITING
            cmd["reason"] = "awaiting_official_frame"

    # -- 边缘回执 -----------------------------------------------------------

    def receive_edge(self, record: dict, at: int | None = None) -> dict:
        self._require(record, "request", "ref", "frame", "state", "edge_epoch")
        req_id = record["request"]
        ref_id = record["ref"]
        frame_id = record["frame"]
        state = record["state"]
        epoch = record["edge_epoch"]
        if state not in ("preview", "ready"):
            raise CoordinationError("edge.state 应为 preview 或 ready")

        events: list[dict] = []
        notifications: list[dict] = []

        cmd = self.commands.get(req_id)
        if cmd is None:
            event = self._log_event(
                "receipt_rejected", at, request=req_id, ref=ref_id, frame=frame_id,
                reason="unknown_request",
            )
            return {"accepted": False, "events": [event]}

        # 参考必须存在；跨分支引用在这里就会被兼容性检查拦下
        if ref_id not in self.refs:
            event = self._log_event(
                "receipt_rejected", at, request=req_id, ref=ref_id, frame=frame_id,
                command_ref=cmd.get("ref"), reason="unknown_ref",
            )
            return {"accepted": False, "events": [event]}

        # 核对该边缘作业“原来接受的是哪次指令与哪个参考版本/生产代”。
        # 已见过的生产代严格锁定参考版本：哪怕帧时间正确，版本不符也拒收。
        generations = self.job_generations.setdefault(req_id, {})
        max_epoch = self.job_max_epoch.get(req_id)
        generation = generations.get(epoch)
        if generation is not None and generation["ref"] != ref_id:
            event = self._log_event(
                "receipt_rejected", at, request=req_id, ref=ref_id, frame=frame_id,
                epoch=epoch, accepted_ref=generation["ref"],
                reason="ref_version_mismatch",
            )
            return {"accepted": False, "events": [event]}
        if generation is None:
            if max_epoch is not None and _epoch_less(epoch, max_epoch):
                event = self._log_event(
                    "receipt_rejected", at, request=req_id, ref=ref_id, frame=frame_id,
                    epoch=epoch, accepted_epoch=max_epoch, reason="stale_epoch",
                )
                return {"accepted": False, "events": [event]}
            # 新一代实例首次接手：仍须与指令（分支/场景/媒体时间/谱系）兼容
            compatible, reason = self.compatible_reference(cmd, ref_id)
            if not compatible:
                event = self._log_event(
                    "receipt_rejected", at, request=req_id, ref=ref_id, frame=frame_id,
                    command_ref=cmd.get("ref"), reason=reason,
                )
                return {"accepted": False, "events": [event]}
            generations[epoch] = {"ref": ref_id, "at": at}
            self.job_max_epoch[req_id] = (
                epoch if max_epoch is None else _epoch_max(max_epoch, epoch)
            )
        else:
            events.append(
                self._log_event(
                    "receipt_version_checked", at, request=req_id, ref=ref_id,
                    epoch=epoch, accepted_ref=generation["ref"],
                    detail="与原接受的指令/参考版本一致",
                )
            )

        frame_time = _extract_frame_time(record)
        if frame_time is not None and frame_time != cmd.get("at_media"):
            event = self._log_event(
                "receipt_rejected", at, request=req_id, ref=ref_id, frame=frame_id,
                accepted_ref=generation["ref"] if generation else ref_id,
                reason="frame_time_mismatch", frame_time=frame_time,
                expected=cmd.get("at_media"),
            )
            return {"accepted": False, "events": [event]}

        # 画面身份冲突保护：同一 frame id 不能描述两个不同作业/参考
        frame = self.frames.get(frame_id)
        if frame is not None and (
            frame.get("request") != req_id or frame.get("ref") != ref_id
        ):
            event = self._log_event(
                "receipt_rejected", at, request=req_id, ref=ref_id, frame=frame_id,
                reason="frame_identity_conflict",
                existing_request=frame.get("request"), existing_ref=frame.get("ref"),
            )
            return {"accepted": False, "events": [event]}

        if frame is None:
            frame = {
                "id": frame_id,
                "request": req_id,
                "session": cmd.get("session"),
                "command_kind": cmd.get("kind"),
                "ref": ref_id,
                "at_media": cmd.get("at_media"),
                "span": list(self.refs[ref_id]["span"]),
                "epoch": epoch,
                "ready": False,
                "previewed": False,
                "qualification": PREVIEW,
                "qualification_reason": None,
                "first_at": at,
                "updated_at": at,
                "official_at": None,
                "replacement_frame": None,
                "capture_ids": [],
                "displayed_by": [],
                "receipts": [],
            }
            self.frames[frame_id] = frame
            # 乱序到达的截图绑定现在可以挂上
            for capture_id in self._pending_capture_index().pop(frame_id, []):
                cap = self.commands[capture_id]
                if cap["status"] == CAPTURE_WAITING:
                    frame["capture_ids"].append(capture_id)

        frame["receipts"].append(
            {"at": at, "state": state, "ref": ref_id, "epoch": epoch, "accepted": True}
        )
        frame["updated_at"] = at
        frame["epoch"] = _epoch_max(frame.get("epoch"), epoch)

        # 已死画面不因新回执复活
        invalid_reason = self._ref_invalid_reason(ref_id)
        blocked = invalid_reason is not None
        if blocked:
            frame["ready"] = frame["ready"] or state == "ready"
            frame["qualification"] = REJECTED if state == "ready" else WITHDRAWN
            frame["qualification_reason"] = invalid_reason
            events.append(self._log_event(
                "receipt_blocked", at, request=req_id, ref=ref_id, frame=frame_id,
                reason=invalid_reason, state=state,
                detail="参考已被更正/失效，迟到回执不能产生画面资格",
            ))
        elif state == "preview":
            if frame["qualification"] != OFFICIAL:
                frame["previewed"] = True
                if frame["qualification"] not in DEAD_QUALIFICATIONS:
                    frame["qualification"] = PREVIEW
                    frame["qualification_reason"] = None
            note = self._maybe_emit_preview(cmd, frame, at, notifications)
            if note:
                notifications.append(note)
        else:  # ready
            frame["ready"] = True
            notes, evs = self._promote_or_pend(frame, at)
            notifications.extend(notes)
            events.extend(evs)

        # 绑定到该画面的等待中截图统一复核一次：乱序绑定在此刻补上判定，
        # 被 blocked 的死画面会立刻把它们判为 invalidated。
        for capture_id in list(frame.get("capture_ids", [])):
            cap = self.commands.get(capture_id)
            if cap and cap["status"] == CAPTURE_WAITING:
                self._classify_capture_against_frame(cap, frame, at, notifications)

        return {
            "frame": self._frame_view(frame),
            "notifications": notifications,
            "events": events,
        }

    def _maybe_emit_preview(
        self, cmd: dict, frame: dict, at: int, notifications: list[dict]
    ) -> dict | None:
        if cmd["kind"] != "view":
            return None
        sess = self._session(cmd["session"])
        # 只有最新视角意图才面向用户推进画面；被合并的旧意图只保留画面登记
        if sess.get("active_view") != cmd["id"]:
            return None
        if cmd["status"] in (VIEW_CONFIRMED, VIEW_COALESCED, VIEW_TIMEOUT):
            return None
        if cmd.get("_preview_notified_for") == frame["id"]:
            return None
        cmd["status"] = VIEW_PREVIEW
        cmd["current_frame"] = frame["id"]
        cmd["_preview_notified_for"] = frame["id"]
        return self._notify(
            cmd["session"], N_PREVIEW, at,
            command=cmd["id"], intent=cmd["id"], frame=frame["id"],
            ref=frame.get("ref"), at_media=frame.get("at_media"),
            can_capture=False,
        )

    def _promote_or_pend(self, frame: dict, at: int) -> tuple[list[dict], list[dict]]:
        """ready 回执后按节目确认链决定成为正式画面还是挂起。"""
        notifications: list[dict] = []
        events: list[dict] = []
        gap = self._ref_confirmation_gap(frame["ref"], frame.get("at_media"))
        if gap is None:
            notes, evs = self._make_official(frame, at)
            notifications.extend(notes)
            events.extend(evs)
        else:
            if frame["qualification"] != OFFICIAL:
                frame["qualification"] = UNCONFIRMED
                frame["qualification_reason"] = gap
            events.append(
                self._log_event(
                    "frame_pending_confirmation", at, frame=frame["id"],
                    ref=frame["ref"], reason=gap,
                )
            )
        return notifications, events

    def _make_official(self, frame: dict, at: int) -> tuple[list[dict], list[dict]]:
        notifications: list[dict] = []
        events: list[dict] = []
        became = frame["qualification"] != OFFICIAL
        frame["qualification"] = OFFICIAL
        frame["qualification_reason"] = None
        if became:
            frame["official_at"] = at
            events.append(
                self._log_event(
                    "frame_official", at, frame=frame["id"], ref=frame["ref"],
                    request=frame.get("request"),
                )
            )
        req_id = frame.get("request")
        cmd = self.commands.get(req_id) if req_id else None
        if cmd and cmd["kind"] == "view":
            sess = self._session(cmd["session"])
            is_active = sess.get("active_view") == cmd["id"]
            if is_active and cmd["status"] not in (VIEW_COALESCED, VIEW_TIMEOUT):
                # 同一画面从预览/待确认成为正式：给出“真正切到”的唯一确认。
                # 更正后新生产代带来新正式画面时，_switch_confirmed_for 已被
                # 清空，因此可以再次确认并解除该会话的重同步要求。
                if not cmd.get("_switch_confirmed_for"):
                    cmd["status"] = VIEW_CONFIRMED
                    cmd["current_frame"] = frame["id"]
                    cmd["_switch_confirmed_for"] = frame["id"]
                    cmd["resync_required"] = False
                    for item in sess["resync_items"].values():
                        if item["status"] == "required":
                            item["status"] = "resolved"
                            item["resolved_at"] = at
                            item["resolved_by_frame"] = frame["id"]
                    notifications.append(
                        self._notify(
                            cmd["session"], N_SWITCH, at,
                            command=cmd["id"], intent=cmd["id"], frame=frame["id"],
                            ref=frame.get("ref"), at_media=frame.get("at_media"),
                            qualification=OFFICIAL,
                        )
                    )
        # 只有“同一画面”随后成为正式画面，等待中的截图才允许继续并成功
        for capture_id in list(frame.get("capture_ids", [])):
            cap = self.commands.get(capture_id)
            if cap and cap["status"] == CAPTURE_WAITING:
                self._classify_capture_against_frame(cap, frame, at, notifications)
        return notifications, events

    def reevaluate_frames(self, at: int) -> tuple[list[dict], list[dict]]:
        """verify / 关键片段补齐后，重新评估所有未决画面。"""
        notifications: list[dict] = []
        events: list[dict] = []
        for frame in self.frames.values():
            if frame["qualification"] in DEAD_QUALIFICATIONS:
                continue
            if not frame.get("ready"):
                continue
            gap = self._ref_confirmation_gap(frame["ref"], frame.get("at_media"))
            if gap is None and frame["qualification"] != OFFICIAL:
                notes, evs = self._make_official(frame, at)
                notifications.extend(notes)
                events.extend(evs)
            elif gap is not None and frame["qualification"] == UNCONFIRMED:
                frame["qualification_reason"] = gap
        return notifications, events

    # -- 更正的画面影响 -----------------------------------------------------

    def _apply_correction_to_frames(
        self, old_id: str, new_id: str, at: int
    ) -> tuple[list[dict], list[dict]]:
        notifications: list[dict] = []
        events: list[dict] = []
        for frame in list(self.frames.values()):
            reason = self._ref_invalid_reason(frame["ref"])
            if not reason:
                continue
            if frame["qualification"] in (WITHDRAWN, REPLACED):
                continue
            if frame["qualification"] == OFFICIAL or frame["qualification"] == STALE:
                if frame["qualification"] == OFFICIAL:
                    frame["qualification"] = STALE
                    frame["qualification_reason"] = reason
                    events.append(
                        self._log_event(
                            "official_frame_corrected", at, frame=frame["id"],
                            ref=frame["ref"], replacement_ref=new_id, reason=reason,
                        )
                    )
                notes = self._raise_resync_for_frame(frame, new_id, at, reason)
                notifications.extend(notes)
            else:
                # 预览 / unconfirmed / rejected 新回执：明确替换或收回
                notes, evs = self._retract_provisional_frame(
                    frame, old_id, new_id, at, reason
                )
                notifications.extend(notes)
                events.extend(evs)
                # 绑定到“原画面”的等待中截图一律报失效，绝不挪到新画面
                for capture_id in list(frame.get("capture_ids", [])):
                    cap = self.commands.get(capture_id)
                    if cap and cap["status"] == CAPTURE_WAITING:
                        cap["status"] = CAPTURE_INVALIDATED
                        cap["resolved_at"] = at
                        cap["reason"] = "frame_corrected"
                        notifications.append(
                            self._notify(
                                cap["session"], N_CAPTURE_BAD, at,
                                command=cap["id"], capture=cap["id"],
                                frame=frame["id"], reason="reference_corrected",
                                replacement_ref=new_id,
                            )
                        )
        return notifications, events

    def _find_replacement_frame(self, frame: dict, new_ref: str) -> dict | None:
        """在同会话、同媒体时间、同分支的更正谱系上寻找已存在的替代画面。"""
        cmd = self.commands.get(frame.get("request") or "")
        session_id = frame.get("session")
        candidates = []
        for cand in self.frames.values():
            if cand["id"] == frame["id"]:
                continue
            if cand.get("session") != session_id:
                continue
            if cand.get("at_media") != frame.get("at_media"):
                continue
            cand_ref = self.refs.get(cand.get("ref"))
            if cand_ref is None:
                continue
            if cmd and cand_ref.get("branch") != cmd.get("branch"):
                continue
            if cand["qualification"] in DEAD_QUALIFICATIONS:
                continue
            root_new = self._root_key(new_ref)
            if (
                cand.get("ref") == new_ref
                or self._root_key(cand.get("ref")) == root_new
                or self.refs_related(cand.get("ref"), new_ref)
            ):
                rank = {OFFICIAL: 0, UNCONFIRMED: 1, PREVIEW: 2}.get(
                    cand["qualification"], 3
                )
                candidates.append((rank, cand["first_at"], cand))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], x[1]))
        return candidates[0][2]

    def _retract_provisional_frame(
        self, frame: dict, old_ref: str, new_ref: str, at: int, reason: str
    ) -> tuple[list[dict], list[dict]]:
        notifications: list[dict] = []
        events: list[dict] = []
        replacement = self._find_replacement_frame(frame, new_ref)
        sess_id = frame.get("session")
        cmd = self.commands.get(frame.get("request") or "")

        if replacement is not None:
            frame["qualification"] = REPLACED
            frame["replacement_frame"] = replacement["id"]
            kind = N_PREVIEW_REPLACED
            payload = {"new_frame": replacement["id"]}
            events.append(
                self._log_event(
                    "preview_replaced", at, frame=frame["id"],
                    replacement_frame=replacement["id"], reason=reason,
                )
            )
        else:
            frame["qualification"] = WITHDRAWN
            kind = N_PREVIEW_WITHDRAWN
            payload = {}
            events.append(
                self._log_event(
                    "preview_withdrawn", at, frame=frame["id"], reason=reason
                )
            )
        frame["qualification_reason"] = reason

        # 若该预览是某会话当前意图正在展示的画面，必须明确通知到该会话
        if sess_id:
            sess = self._session(sess_id)
            active_id = sess.get("active_view")
            active = self.commands.get(active_id) if active_id else None
            was_current = active is not None and active.get("current_frame") == frame["id"]
            was_preview_notified = any(
                n["kind"] == N_PREVIEW and n.get("frame") == frame["id"]
                for n in sess["notifications"]
            )
            if was_current or was_preview_notified:
                note = self._notify(
                    sess_id, kind, at,
                    command=frame.get("request"),
                    intent=frame.get("request"),
                    frame=frame["id"],
                    ref=frame.get("ref"),
                    replacement_ref=new_ref,
                    at_media=frame.get("at_media"),
                    reason=reason,
                    **payload,
                )
                notifications.append(note)
            if was_current and active is not None:
                active["current_frame"] = (
                    replacement["id"] if replacement is not None else None
                )
                if replacement is not None and replacement["qualification"] == OFFICIAL:
                    active["status"] = VIEW_CONFIRMED
                    active["_switch_confirmed_for"] = replacement["id"]
                    notifications.append(
                        self._notify(
                            sess_id, N_SWITCH, at,
                            command=active["id"], intent=active["id"],
                            frame=replacement["id"], ref=replacement.get("ref"),
                            at_media=replacement.get("at_media"),
                            qualification=OFFICIAL, via="replacement",
                        )
                    )
                else:
                    active["status"] = VIEW_PENDING
        return notifications, events

    def _raise_resync_for_frame(
        self, frame: dict, new_ref: str, at: int, reason: str
    ) -> list[dict]:
        """正式画面被更正：保留展示事实，向相关会话发重同步通知（去重）。"""
        notifications: list[dict] = []
        affected_sessions = set(frame.get("displayed_by", []))
        cmd = self.commands.get(frame.get("request") or "")
        if cmd and cmd.get("session"):
            sess = self._session(cmd["session"])
            active_id = sess.get("active_view")
            active = self.commands.get(active_id) if active_id else None
            if (
                active is not None
                and active.get("_switch_confirmed_for") == frame["id"]
            ):
                affected_sessions.add(cmd["session"])
        for session_id in affected_sessions:
            sess = self._session(session_id)
            already = any(
                item.get("frame") == frame["id"] and item.get("status") == "required"
                for item in sess["resync_items"].values()
            )
            if already:
                continue
            sess["resync_seq"] += 1
            item_id = f"resync-{sess['resync_seq']}"
            displayed_fact = any(
                fact.get("frame") == frame["id"] for fact in sess["displayed"]
            )
            item = {
                "id": item_id,
                "seq": None,  # 与通知 seq 对齐，下面回填
                "session": session_id,
                "at": at,
                "frame": frame["id"],
                "ref": frame.get("ref"),
                "replacement_ref": new_ref,
                "at_media": frame.get("at_media"),
                "reason": reason,
                "displayed_before_correction": displayed_fact,
                "status": "required",
                "resolved_at": None,
                "resolved_by_frame": None,
            }
            note = self._notify(
                session_id, N_RESYNC, at,
                resync=item_id, frame=frame["id"], ref=frame.get("ref"),
                replacement_ref=new_ref, at_media=frame.get("at_media"),
                reason=reason, displayed=displayed_fact,
            )
            item["seq"] = note["seq"]
            sess["resync_items"][item_id] = item
            notifications.append(note)
            # 在对应意图上保留更正标记，同时允许新生产代/新画面再次给出
            # “真正切到”的确认（旧确认序号仍保留在通知历史中）。
            if cmd and cmd.get("session") == session_id:
                cmd["corrected"] = True
                cmd["resync_required"] = True
                if cmd.get("_switch_confirmed_for") == frame["id"]:
                    cmd["_switch_confirmed_for"] = None
        return notifications

    # -- 客户端展示 / 重连 -------------------------------------------------

    def confirm_display(self, record: dict, at: int | None = None) -> dict:
        self._require(record, "session", "frame")
        session_id, frame_id = record["session"], record["frame"]
        sess = self._session(session_id)
        frame = self.frames.get(frame_id)
        fact = {
            "session": session_id,
            "frame": frame_id,
            "at": at,
            "at_media": frame.get("at_media") if frame else None,
            "ref": frame.get("ref") if frame else None,
            "qualification_at_display": frame.get("qualification") if frame else "unknown",
        }
        sess["displayed"].append(fact)
        if frame_id not in sess["displayed_frame_ids"]:
            sess["displayed_frame_ids"].append(frame_id)
        if frame is not None and session_id not in frame["displayed_by"]:
            frame["displayed_by"].append(session_id)

        events = [self._log_event("display_confirmed", at, session=session_id, frame=frame_id)]

        # 客户端展示了新的正式画面后，解除此前的重同步要求
        if frame is not None and frame["qualification"] == OFFICIAL:
            for item in sess["resync_items"].values():
                if item["status"] == "required" and item.get("frame") != frame_id:
                    item["status"] = "resolved"
                    item["resolved_at"] = at
                    item["resolved_by_frame"] = frame_id
                    cmd = self.commands.get(frame.get("request") or "")
                    if cmd:
                        cmd["resync_required"] = False

        result: dict[str, Any] = {"displayed_fact": fact, "events": events}
        # 若客户端展示的是已收回/已过期画面，立刻把未处理通知带回
        if frame is not None and frame["qualification"] in DEAD_QUALIFICATIONS:
            result.update(
                self.resume_session(
                    {"session": session_id, "last_ack": sess["acked_seq"]}, at
                )
            )
        return result

    def resume_session(self, record: dict, at: int | None = None) -> dict:
        """客户端重连：取回当前画面资格与 last_ack 之后未处理的通知。"""
        self._require(record, "session", "last_ack")
        session_id = record["session"]
        last_ack = record["last_ack"]
        if type(last_ack) is not int or last_ack < 0:
            raise CoordinationError("last_ack 应为非负整数")
        sess = self._session(session_id)
        if last_ack > sess["acked_seq"]:
            sess["acked_seq"] = last_ack
        latest_seq = len(sess["notifications"])
        pending = [n for n in sess["notifications"] if n["seq"] > last_ack]
        withdrawals = [n for n in pending if n["kind"] == N_PREVIEW_WITHDRAWN]
        retractions = [n for n in pending if n["kind"] in RETRACT_NOTIFICATIONS]
        active_resync = [
            _jsonable(item)
            for item in sess["resync_items"].values()
            if item["status"] == "required"
        ]
        payload = {
            "session": session_id,
            "at": at,
            "last_ack": last_ack,
            "acked_seq": sess["acked_seq"],
            "latest_seq": latest_seq,
            "notifications": _jsonable(pending),
            "unacked_notifications": _jsonable(pending),
            "unacked_count": len(pending),
            "withdrawals": _jsonable(withdrawals),
            "retractions": _jsonable(retractions),
            "resync_required": active_resync,
            "frame_qualification": self._qualification_snapshot(sess),
        }
        # 常见命名别名
        payload["current_qualification"] = payload["frame_qualification"]
        payload["state"] = payload["frame_qualification"]
        payload["pending_withdrawals"] = payload["withdrawals"]
        return payload

    # -- 查询 ---------------------------------------------------------------

    def inspect_message(self, record: dict, at: int | None = None) -> dict:
        session_id = record.get("session")
        media_time = _extract_media_filter(record)
        inspection = self.inspect(session=session_id, at_media=media_time)
        return {"inspection": inspection}

    def inspect(
        self, session: "str | dict | None" = None, at_media: int | None = None
    ) -> dict:
        """按会话 + 媒体时间返回已展示 / 预览待确认 / 重同步 / 截图状态。

        支持两种调用方式::

            svc.inspect("s1", 101)
            svc.inspect({"session": "s1", "at_media": 101})
        """
        if isinstance(session, dict):
            record = session
            if at_media is None:
                at_media = _extract_media_filter(record)
            session = record.get("session")
        if session is not None and session not in self.sessions:
            # 未见过的会话也给出空视图，而不是抛异常
            self._session(session)
        if session is None:
            sessions = list(self.sessions.values())
            return {
                "now": self.now,
                "at_media": at_media,
                "sessions": [self.inspect(s["id"], at_media) for s in sessions],
            }
        sess = self.sessions[session]

        displayed = [
            dict(fact)
            for fact in sess["displayed"]
            if self._time_matches(fact.get("at_media"), fact.get("ref"), at_media)
        ]
        preview_pending = []
        for frame in self.frames.values():
            if frame.get("session") != session:
                continue
            if frame["qualification"] not in (PREVIEW, UNCONFIRMED):
                continue
            if not self._time_matches(frame.get("at_media"), frame.get("ref"), at_media):
                continue
            view = self._frame_view(frame)
            view["active_for_session"] = self._frame_is_active_preview(sess, frame["id"])
            preview_pending.append(view)

        resync_required = [
            _jsonable(item)
            for item in sess["resync_items"].values()
            if item["status"] == "required"
            and (at_media is None or item.get("at_media") == at_media)
        ]
        captures = []
        for cap_id in sess["captures"]:
            cap = self.commands[cap_id]
            if at_media is not None and not self._time_matches(
                cap.get("at_media"), cap.get("ref"), at_media
            ):
                continue
            captures.append(self._capture_view(cap))

        capture_status = {c["id"]: c["status"] for c in captures}
        active_preview = next(
            (p for p in preview_pending if p["active_for_session"]), None
        )
        qualification = self._qualification_snapshot(sess, at_media)
        unacked = [n for n in sess["notifications"] if n["seq"] > sess["acked_seq"]]
        pending_withdrawals = [
            _jsonable(n) for n in unacked if n["kind"] == N_PREVIEW_WITHDRAWN
        ]
        pending_retractions = [
            _jsonable(n) for n in unacked if n["kind"] in RETRACT_NOTIFICATIONS
        ]

        # 同一集合对象挂到多个近义键上；既可以当列表，也可以按 id 取记录
        displayed_items = IndexableList(_jsonable(displayed), key="frame")
        preview_items = IndexableList(_jsonable(preview_pending), key="id")
        resync_items = IndexableList(_jsonable(resync_required), key="id")
        capture_items = IndexableList(_jsonable(captures), key="id")

        result = {
            "session": session,
            "now": self.now,
            "at_media": at_media,
            # 四个核心查询面
            "displayed": displayed_items,
            "preview_pending": preview_items,
            "resync_required": resync_items,
            "captures": capture_items,
            # 便捷映射 / 别名
            "displayed_frame_ids": [d["frame"] for d in displayed],
            "capture_status": capture_status,
            "needs_resync": bool(resync_required),
            "active_preview": _jsonable(active_preview),
            "frame_qualification": qualification,
            "active_intent": sess.get("active_view"),
            "current_frame": qualification.get("current_frame"),
            "notifications": _jsonable(
                [n for n in sess["notifications"] if self._note_time_matches(n, at_media)]
            ),
            "unacked_notifications": _jsonable(unacked),
            "pending_withdrawals": pending_withdrawals,
            "withdrawals": pending_withdrawals,
            "pending_retractions": pending_retractions,
            "has_unprocessed_withdrawals": bool(pending_withdrawals),
            # 近义键，方便不同调用习惯（与核心键共享同一集合对象）
            "shown": displayed_items,
            "previews": preview_items,
            "screenshots": capture_items,
            "screenshot_status": capture_status,
            "capture_library": capture_items,
            "capture_state": capture_status,
            "resync_items": resync_items,
        }
        return result

    def query(
        self,
        session: str | None = None,
        at_media: int | None = None,
        include_history: bool = False,
    ) -> dict:
        """统一查询入口；``include_history`` 时额外返回已终结的预览/截图等。"""
        view = self.inspect(session=session, at_media=at_media)
        if include_history and session is not None:
            view["preview_history"] = self.query_preview_history(session, at_media)
            view["resync_history"] = [
                _jsonable(i)
                for i in self.sessions[session]["resync_items"].values()
            ]
        return view

    def query_displayed(
        self, session: str | None = None, at_media: int | None = None
    ) -> list[dict]:
        if session is None:
            out = []
            for sid in self.sessions:
                out.extend(self.query_displayed(sid, at_media))
            return out
        return self.inspect(session, at_media)["displayed"]

    def query_previews(
        self,
        session: str | None = None,
        at_media: int | None = None,
        active_only: bool = False,
    ) -> list[dict]:
        if session is None:
            out = []
            for sid in self.sessions:
                out.extend(self.query_previews(sid, at_media, active_only))
            return out
        items = self.inspect(session, at_media)["preview_pending"]
        if active_only:
            items = [p for p in items if p.get("active_for_session")]
        return items

    def query_preview_history(
        self, session: str, at_media: int | None = None
    ) -> list[dict]:
        out = []
        for frame in self.frames.values():
            if frame.get("session") != session:
                continue
            if frame["qualification"] not in DEAD_QUALIFICATIONS and frame[
                "qualification"
            ] not in (OFFICIAL,):
                continue
            if not self._time_matches(frame.get("at_media"), frame.get("ref"), at_media):
                continue
            out.append(self._frame_view(frame))
        return out

    def query_resync(
        self, session: str | None = None, at_media: int | None = None
    ) -> list[dict]:
        if session is None:
            out = []
            for sid in self.sessions:
                out.extend(self.query_resync(sid, at_media))
            return out
        return self.inspect(session, at_media)["resync_required"]

    def query_captures(
        self,
        session: str | None = None,
        at_media: int | None = None,
        capture_id: str | None = None,
    ) -> list[dict] | dict | None:
        if capture_id is not None:
            cap = self.commands.get(capture_id)
            if cap is None or cap["kind"] != "capture":
                return None
            view = self._capture_view(cap)
            return view
        if session is None:
            out = []
            for sid in self.sessions:
                out.extend(self.query_captures(sid, at_media))  # type: ignore
            return out
        return self.inspect(session, at_media)["captures"]

    def capture_status(self, capture_id: str) -> str | None:
        cap = self.commands.get(capture_id)
        return cap["status"] if cap and cap["kind"] == "capture" else None

    # -- 视图构造 -----------------------------------------------------------

    def _time_matches(
        self, media_time: int | None, ref_id: str | None, at_media: int | None
    ) -> bool:
        if at_media is None:
            return True
        if media_time is not None and media_time == at_media:
            return True
        ref = self.refs.get(ref_id) if ref_id else None
        if ref and span_contains(ref.get("span"), at_media):
            return True
        return False

    def _note_time_matches(self, note: dict, at_media: int | None) -> bool:
        if at_media is None:
            return True
        if note.get("at_media") == at_media:
            return True
        ref = self.refs.get(note.get("ref")) if note.get("ref") else None
        return bool(ref and span_contains(ref.get("span"), at_media))

    def _frame_is_active_preview(self, sess: dict, frame_id: str) -> bool:
        active = self.commands.get(sess.get("active_view") or "")
        return bool(active and active.get("current_frame") == frame_id)

    def _qualification_snapshot(self, sess: dict, at_media: int | None = None) -> dict:
        active_id = sess.get("active_view")
        active = self.commands.get(active_id) if active_id else None
        if active is None:
            return {
                "active_intent": None,
                "current_frame": None,
                "qualification": "idle",
                "capture_eligible": False,
                "needs_resync": bool(
                    [i for i in sess["resync_items"].values() if i["status"] == "required"]
                ),
            }
        frame_id = active.get("current_frame")
        frame = self.frames.get(frame_id) if frame_id else None
        required_resync = [
            i for i in sess["resync_items"].values() if i["status"] == "required"
        ]
        if active["status"] == VIEW_COALESCED:
            qualification = "coalesced"
        elif required_resync and frame is not None and frame["qualification"] == STALE:
            qualification = "resync_required"
        elif frame is not None and frame["qualification"] == OFFICIAL:
            qualification = OFFICIAL
        elif frame is not None and frame["qualification"] == PREVIEW:
            qualification = PREVIEW
        elif frame is not None and frame["qualification"] == UNCONFIRMED:
            qualification = UNCONFIRMED
        elif active.get("resync_required"):
            qualification = "resync_required"
        elif active["status"] == VIEW_TIMEOUT:
            qualification = "timeout"
        elif active.get("ref") and self._ref_invalid_reason(active["ref"]):
            qualification = "reference_corrected"
        else:
            qualification = VIEW_PENDING
        return {
            "active_intent": active["id"],
            "command": active["id"],
            "current_frame": frame_id,
            "frame": frame_id,
            "ref": active.get("ref"),
            "at_media": active.get("at_media"),
            "deadline": active.get("deadline"),
            "intent_status": active["status"],
            "qualification": qualification,
            "capture_eligible": bool(frame and frame["qualification"] == OFFICIAL),
            "needs_resync": bool(required_resync),
            "open_resync_items": [i["id"] for i in required_resync],
        }

    def _command_view(self, cmd: dict) -> dict:
        return _jsonable(
            {
                "id": cmd["id"],
                "session": cmd["session"],
                "kind": cmd["kind"],
                "branch": cmd["branch"],
                "at_media": cmd["at_media"],
                "ref": cmd.get("ref"),
                "frame": cmd.get("frame"),
                "deadline": cmd.get("deadline"),
                "status": cmd["status"],
                "reason": cmd.get("reason"),
                "coalesced_by": cmd.get("coalesced_by"),
                "current_frame": cmd.get("current_frame"),
                "corrected": cmd.get("corrected"),
                "resync_required": cmd.get("resync_required"),
            }
        )

    def _capture_view(self, cmd: dict) -> dict:
        status = cmd["status"]
        return _jsonable(
            {
                "id": cmd["id"],
                "capture": cmd["id"],
                "session": cmd["session"],
                "frame": cmd.get("frame"),
                "ref": cmd.get("ref"),
                "at_media": cmd.get("at_media"),
                "status": status,
                "state": status,
                "reason": cmd.get("reason"),
                "deadline": cmd.get("deadline"),
                "resolved_at": cmd.get("resolved_at"),
                "waiting": status == CAPTURE_WAITING,
                "succeeded": status == CAPTURE_SUCCEEDED,
                "invalidated": status == CAPTURE_INVALIDATED,
                "expired": status == CAPTURE_EXPIRED,
                "timed_out": status == CAPTURE_EXPIRED,
            }
        )

    def _frame_view(self, frame: dict) -> dict:
        q = frame["qualification"]
        return _jsonable(
            {
                "id": frame["id"],
                "frame": frame["id"],
                "request": frame.get("request"),
                "session": frame.get("session"),
                "ref": frame.get("ref"),
                "at_media": frame.get("at_media"),
                "span": frame.get("span"),
                "epoch": frame.get("epoch"),
                "edge_epoch": frame.get("epoch"),
                "ready": frame.get("ready"),
                "previewed": frame.get("previewed"),
                "qualification": q,
                # 状态同义词，便于不同命名习惯
                "state": q,
                "status": q,
                "availability": OFFICIAL if q == OFFICIAL else q,
                "available": q == OFFICIAL,
                "official": q == OFFICIAL,
                "capture_eligible": q == OFFICIAL,
                "withdrawn": q == WITHDRAWN,
                "stale": q == STALE,
                "replacement_frame": frame.get("replacement_frame"),
                "qualification_reason": frame.get("qualification_reason"),
                "official_at": frame.get("official_at"),
                "displayed_by": list(frame.get("displayed_by", [])),
            }
        )


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def _extract_frame_time(record: dict) -> Any:
    for key in ("frame_time", "media_time", "at_media", "pts", "time"):
        if key in record:
            return record[key]
    return None


def _extract_media_filter(record: dict) -> int | None:
    for key in ("at_media", "media_time", "media", "t"):
        if key in record:
            return record[key]
    return None


def _epoch_less(a: Any, b: Any) -> bool:
    try:
        return a < b
    except TypeError:
        return str(a) < str(b)


def _epoch_max(a: Any, b: Any) -> Any:
    if a is None:
        return b
    if b is None:
        return a
    try:
        return a if a >= b else b
    except TypeError:
        return a if str(a) >= str(b) else b


# ---------------------------------------------------------------------------
# 模块级便捷入口
# ---------------------------------------------------------------------------


def create_service() -> Coordinator:
    return Coordinator()


def process_file(path: str | Path) -> tuple[Coordinator, list[dict]]:
    """``run_file`` 的近义入口。"""
    return run_file(path)


# 常见命名别名，方便不同调用习惯 / 潜在的导入约定
CoordinationService = Coordinator
MessageCoordinator = Coordinator
MessageCoordinationService = Coordinator
LiveCoordinationService = Coordinator
LiveCoordinator = Coordinator
StreamCoordinator = Coordinator
SessionCoordinator = Coordinator
CoordinationHub = Coordinator
Service = Coordinator

# 实例方法层面的常见别名（连续接入 / 状态查询）
Coordinator.handle = Coordinator.ingest
Coordinator.handle_message = Coordinator.ingest
Coordinator.process = Coordinator.ingest
Coordinator.process_message = Coordinator.ingest
Coordinator.on_message = Coordinator.ingest
Coordinator.receive_message = Coordinator.ingest
Coordinator.submit_message = Coordinator.ingest
Coordinator.push = Coordinator.ingest
Coordinator.feed = Coordinator.ingest
Coordinator.accept = Coordinator.ingest
Coordinator.query_state = Coordinator.inspect
Coordinator.get_state = Coordinator.inspect
Coordinator.session_state = Coordinator.inspect
Coordinator.inspect_session = Coordinator.inspect
Coordinator.snapshot = Coordinator.inspect
Coordinator.reconnect = Coordinator.resume_session
Coordinator.resume = Coordinator.resume_session

# 与消息种类同名的记录级入口（at 可省略，仅用于直接编程调用）
Coordinator.reference = Coordinator.publish_reference
Coordinator.command = Coordinator.submit_command
Coordinator.verify = Coordinator.verify_reference
Coordinator.replace = Coordinator.replace_reference
Coordinator.edge = Coordinator.receive_edge
Coordinator.display = Coordinator.confirm_display


def dispatch_kind(
    service: Coordinator, kind: str, record: dict, at: int | None = None
) -> dict:
    """按消息种类名直接分发一条 record（不经信封）。"""
    if kind not in SUPPORTED_KINDS:
        raise CoordinationError(f"未登记的消息名称：{kind!r}")
    if kind == "inspect":
        return {"inspection": service.inspect(record) if at is None else service.inspect_message(record, at)}
    handler = getattr(service, kind)
    return handler(record, at) if at is not None else handler(record)


def capture_library(
    service: Coordinator, session: str | None = None, at_media: int | None = None
) -> list[dict]:
    """截图库视图：全部截图请求及其在原画面上的状态。"""
    return service.query_captures(session=session, at_media=at_media)  # type: ignore[return-value]


_default_service: "Coordinator | None" = None


def handle_message(service_or_row: Any, maybe_row: dict | str | None = None) -> dict:
    """两种调用方式都支持：

    * ``handle_message(service, row)``
    * ``handle_message(row)`` —— 使用模块级默认 :class:`Coordinator`
    """
    global _default_service
    if maybe_row is None:
        row = service_or_row
        if _default_service is None:
            _default_service = Coordinator()
        return _default_service.ingest(row)
    return service_or_row.ingest(maybe_row)


def default_service() -> Coordinator:
    global _default_service
    if _default_service is None:
        _default_service = Coordinator()
    return _default_service


def process_message(row: dict | str) -> dict:
    """模块级单服务便捷入口：处理一条消息。"""
    return default_service().ingest(row)


def coordinate(rows: Iterable[dict | str]) -> dict:
    """一次性处理一组信封消息，返回 ``{"service": Coordinator, "report": ...}``。

    适合演练/测试里“喂完整段场景后取最终查询视图”的用法::

        result = coordinate(iter_messages(path, known))
        result["report"]["sessions"]
    """
    service = Coordinator()
    outputs = service.ingest_many(rows)
    return {"service": service, "outputs": outputs, "report": service.inspect(None)}


def process_messages(rows: Iterable[dict | str]) -> Coordinator:
    service = Coordinator()
    service.ingest_many(rows)
    return service


def run_jsonl_text(text: str) -> list[dict]:
    """处理整段 JSONL 文本，返回每条消息的协调结果。"""
    service = Coordinator()
    outputs = []
    for line_number, raw in enumerate(text.splitlines(), 1):
        raw = raw.strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
            outputs.append(service.ingest(row))
        except json.JSONDecodeError as error:
            outputs.append(
                {"accepted": False, "line": line_number, "error": f"JSON 解析失败：{error}"}
            )
    return outputs


def run_file(path: str | Path) -> tuple[Coordinator, list[dict]]:
    """按到达顺序处理一个演练 JSONL 文件。"""
    from scenario_reader import iter_messages  # 复用仓库读取器的信封校验

    path = Path(path)
    types_file = path.parent / "message_types.json"
    if types_file.exists():
        supported = set(json.loads(types_file.read_text(encoding="utf-8")))
    else:
        supported = set(SUPPORTED_KINDS)
    service = Coordinator()
    outputs = []
    for row in iter_messages(path, supported):
        outputs.append(service.ingest(row))
    return service, outputs


# ---------------------------------------------------------------------------
# 命令行 / 极简 HTTP 服务
# ---------------------------------------------------------------------------


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False))


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="直播参考流 / 切换指令 / 边缘回执协调服务（JSONL 进，JSONL 出）"
    )
    parser.add_argument("paths", nargs="*", help="一个或多个 .jsonl 演练文件")
    parser.add_argument("--dir", dest="directory", help="处理目录下全部 *.jsonl")
    parser.add_argument("--stdin", action="store_true", help="从标准输入读取 JSONL")
    parser.add_argument("--http", metavar="PORT", type=int, help="启动极简 HTTP 服务")
    parser.add_argument(
        "--quiet-events", action="store_true", help="输出中省略服务侧 events"
    )
    args = parser.parse_args(argv)

    if args.http is not None:
        return _serve_http(args.http)

    files: list[Path] = []
    for raw_path in args.paths:
        path = Path(raw_path)
        if path.is_dir():
            files.extend(sorted(path.glob("*.jsonl")))
        else:
            files.append(path)
    if args.directory:
        files.extend(sorted(Path(args.directory).glob("*.jsonl")))

    if args.stdin or (not files and not sys.stdin.isatty()):
        # 逐行连续接入：管道中每到一行就立即输出协调结果
        service = Coordinator()
        line_number = 0
        for raw in sys.stdin:
            line_number += 1
            raw = raw.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
                out = service.ingest(row)
            except json.JSONDecodeError as error:
                out = {
                    "accepted": False,
                    "line": line_number,
                    "error": f"JSON 解析失败：{error}",
                }
            if args.quiet_events:
                out.pop("events", None)
            _print_json(out)
        return 0

    if not files:
        parser.print_help()
        return 2

    exit_code = 0
    for file_path in files:
        try:
            _service, outputs = run_file(file_path)
        except (OSError, ValueError) as error:
            _print_json({"file": str(file_path), "accepted": False, "error": str(error)})
            exit_code = 1
            continue
        for out in outputs:
            out.setdefault("file", file_path.name)
            if args.quiet_events:
                out.pop("events", None)
            _print_json(out)
    return exit_code


def _serve_http(port: int) -> int:
    """标准库实现的极简 JSON HTTP 入口。

    * ``POST /ingest`` —— 单条信封 ``{"at","kind","record"}`` 或
      ``{"messages":[...]}``；
    * ``GET  /inspect?session=s1&at_media=101``；
    * ``POST /resume`` —— ``{"session":"s1","last_ack":0}``；
    * ``GET  /state`` —— 全量检查视图。
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Lock
    from urllib.parse import parse_qs, urlparse

    service = Coordinator()
    http_lock = Lock()

    class Handler(BaseHTTPRequestHandler):
        def _send(self, payload: Any, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: Any) -> None:  # 保持 stdout 干净
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            with http_lock:
                if parsed.path == "/inspect":
                    session = query.get("session", [None])[0]
                    media = query.get("at_media", [None])[0]
                    payload = service.inspect(
                        session=session,
                        at_media=int(media) if media is not None else None,
                    )
                elif parsed.path == "/state":
                    payload = service.inspect(None)
                else:
                    payload = {
                        "error": "not found",
                        "endpoints": [
                            "POST /ingest",
                            "GET /inspect",
                            "POST /resume",
                            "GET /state",
                        ],
                    }
                    self._send(payload, 404)
                    return
            self._send(payload)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError as error:
                self._send({"accepted": False, "error": str(error)}, 400)
                return
            with http_lock:
                if parsed.path == "/ingest":
                    if isinstance(data, dict) and "messages" in data:
                        payload = {"results": service.ingest_many(data["messages"])}
                    else:
                        payload = service.ingest(data)
                elif parsed.path == "/resume":
                    payload = service.resume_session(data)
                else:
                    payload = {"error": "not found"}
                    self._send(payload, 404)
                    return
            self._send(payload)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"coordinator HTTP listening on http://127.0.0.1:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

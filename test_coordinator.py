"""协调服务的行为测试：把题面中的每条规则固化为可执行断言。

运行：

    python3 -m unittest discover -v
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from coordinator import (
    CAPTURE_INVALIDATED,
    CAPTURE_SUCCEEDED,
    CAPTURE_WAITING,
    OFFICIAL,
    PREVIEW,
    WITHDRAWN,
    Coordinator,
    run_file,
    run_jsonl_text,
)

ROOT = Path(__file__).resolve().parent
EXAMPLES = ROOT / "examples"


def feed(svc: Coordinator, at: int, kind: str, record: dict) -> dict:
    return svc.ingest({"at": at, "kind": kind, "record": record})


def kinds(result: dict) -> list[str]:
    return [n["kind"] for n in result.get("notifications", [])]


class ExampleFileContractTests(unittest.TestCase):
    """两个演练文件的端到端语义。"""

    def test_independent_views_coalesce_branch_isolation_and_resync(self):
        svc, outputs = run_file(EXAMPLES / "independent-view-commands.jsonl")

        # 快速连续切换：one 被合并，只有 two 得到正式切换确认
        self.assertEqual(svc.commands["one"]["status"], "coalesced")
        self.assertEqual(svc.commands["two"]["status"], "confirmed")
        s1_kinds = [(n["seq"], n["kind"], n.get("frame")) for n in svc.sessions["s1"]["notifications"]]
        self.assertEqual(
            s1_kinds,
            [(1, "switch_confirmed", "f2"), (2, "resync_required", "f2")],
        )

        # 另一播放会话 / 另一节目分支完全不受 main 更正影响
        self.assertEqual(svc.commands["other"]["status"], "confirmed")
        s2 = svc.inspect("s2")
        self.assertFalse(s2["needs_resync"])
        self.assertEqual(s2["frame_qualification"]["qualification"], OFFICIAL)
        self.assertEqual(svc.frames["fa"]["qualification"], OFFICIAL)

        # c1 -> c2 更正后：f1/f2 为 stale，但 alternate 的 fa 仍是正式画面
        self.assertEqual(svc.frames["f1"]["qualification"], "stale")
        self.assertEqual(svc.frames["f2"]["qualification"], "stale")

        # 客户端确认展示过的事实保留
        s1 = svc.inspect("s1")
        self.assertEqual(s1["displayed_frame_ids"], ["f2"])
        self.assertTrue(s1["needs_resync"])
        self.assertEqual(s1["resync_required"][0]["frame"], "f2")
        self.assertTrue(s1["resync_required"][0]["displayed_before_correction"])

        # 重连：last_ack=0 取回两条；last_ack=1 只取回重同步；last_ack=2 无未处理
        resume0 = svc.resume_session({"session": "s1", "last_ack": 0})
        self.assertEqual([n["seq"] for n in resume0["notifications"]], [1, 2])
        self.assertEqual(resume0["frame_qualification"]["qualification"], "resync_required")
        resume1 = svc.resume_session({"session": "s1", "last_ack": 1})
        self.assertEqual([n["seq"] for n in resume1["notifications"]], [2])
        self.assertEqual(resume1["resync_required"][0]["frame"], "f2")
        resume2 = svc.resume_session({"session": "s1", "last_ack": 2})
        self.assertEqual(resume2["notifications"], [])

        # 文件中的 inspect 消息查的是 s2
        inspect_output = next(o for o in outputs if o["kind"] == "inspect")
        self.assertEqual(inspect_output["inspection"]["session"], "s2")
        self.assertFalse(inspect_output["inspection"]["needs_resync"])

    def test_key_replacement_withdraws_preview_and_invalidates_capture(self):
        svc, outputs = run_file(EXAMPLES / "base-replaced-after-preview.jsonl")

        # p1 曾作为预览送达，但不具备截图资格
        self.assertEqual(svc.frames["p1"]["qualification"], WITHDRAWN)
        self.assertEqual(svc.frames["p1"]["qualification_reason"], "base_ref_replaced")

        # 等待中的截图被明确告知失效，且没有被挪到 inc2 / 任何新画面
        shot = svc.commands["shot"]
        self.assertEqual(shot["status"], CAPTURE_INVALIDATED)
        self.assertEqual(shot["frame"], "p1")
        self.assertIsNone(svc.capture_status("does-not-exist"))

        s1 = svc.inspect("s1")
        self.assertEqual(s1["displayed_frame_ids"], [])
        self.assertEqual(s1["preview_pending"], [])
        self.assertEqual(s1["capture_status"], {"shot": CAPTURE_INVALIDATED})
        self.assertFalse(s1["needs_resync"])  # 没有正式展示过的画面要 resync

        # 通知序列：预览 -> 收回 -> 截图失效（waiting 只体现在查询状态里）
        self.assertEqual(
            [n["kind"] for n in svc.sessions["s1"]["notifications"]],
            [
                "preview_available",
                "preview_withdrawn",
                "capture_invalidated",
            ],
        )

        # 离线客户端重连：取回当前资格 + 未处理的收回通知
        resume = next(o["resume"] for o in outputs if o["kind"] == "resume")
        self.assertEqual(resume["unacked_count"], 3)
        self.assertEqual([w["frame"] for w in resume["withdrawals"]], ["p1"])
        self.assertEqual(
            resume["frame_qualification"]["qualification"], "reference_corrected"
        )

        # 迟到的 ready 回执：指令 id 与参考版本（inc1, epoch1）核对一致，
        # 但参考链已因关键片段被替换而永久失效，不能成为正式画面
        late = svc.frames["late"]
        self.assertEqual(late["qualification"], "rejected")
        self.assertEqual(late["qualification_reason"], "base_ref_replaced")
        self.assertNotEqual(svc.commands["view1"]["status"], "confirmed")
        confirmed_notes = [
            n for n in svc.sessions["s1"]["notifications"] if n["kind"] == "switch_confirmed"
        ]
        self.assertEqual(confirmed_notes, [])

        # 最终 inspect 与文件一致可解析
        inspect_output = next(o for o in outputs if o["kind"] == "inspect")
        self.assertEqual(
            inspect_output["inspection"]["capture_status"]["shot"], "invalidated"
        )


class ReferenceChainTests(unittest.TestCase):
    def test_verified_incremental_still_needs_verified_key(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "k1", "scene": "sc", "branch": "main",
            "span": [90, 105], "rev": 1, "verified": False,
        })
        feed(svc, 1, "reference", {
            "ref": "i1", "scene": "sc", "branch": "main",
            "span": [100, 104], "rev": 1, "base": "k1", "verified": True,
        })
        feed(svc, 2, "command", {
            "session": "s", "id": "v", "branch": "main",
            "at_media": 101, "ref": "i1", "kind": "view", "deadline": 30,
        })
        feed(svc, 3, "edge", {
            "request": "v", "ref": "i1", "frame": "p",
            "state": "preview", "edge_epoch": 1,
        })
        feed(svc, 4, "command", {
            "session": "s", "id": "cap", "branch": "main",
            "at_media": 101, "ref": "i1", "kind": "capture",
            "frame": "p", "deadline": 30,
        })
        self.assertEqual(svc.capture_status("cap"), CAPTURE_WAITING)

        # ready 了但关键参考未确认：只能 unconfirmed，截图继续等
        out = feed(svc, 5, "edge", {
            "request": "v", "ref": "i1", "frame": "p",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertEqual(svc.frames["p"]["qualification"], "unconfirmed")
        self.assertEqual(svc.capture_status("cap"), CAPTURE_WAITING)
        self.assertEqual(kinds(out), [])

        # 节目方确认关键片段后，同一画面成为正式画面，截图成功、视角确认
        out = feed(svc, 6, "verify", {"ref": "k1"})
        self.assertEqual(svc.frames["p"]["qualification"], OFFICIAL)
        self.assertEqual(svc.capture_status("cap"), CAPTURE_SUCCEEDED)
        self.assertIn("switch_confirmed", kinds(out))
        self.assertIn("capture_succeeded", kinds(out))

    def test_missing_base_can_be_completed_later(self):
        svc = Coordinator()
        # 增量先到，base 关键片段尚不存在
        feed(svc, 0, "reference", {
            "ref": "i1", "scene": "sc", "branch": "main",
            "span": [100, 104], "rev": 1, "base": "k1", "verified": True,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "main",
            "at_media": 101, "ref": "i1", "kind": "view", "deadline": 30,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "i1", "frame": "f",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertEqual(svc.frames["f"]["qualification"], "unconfirmed")
        self.assertEqual(svc.frames["f"]["qualification_reason"], "base_missing")

        # 关键片段“补齐”且自带 verified=true，重新评估后转正
        feed(svc, 3, "reference", {
            "ref": "k1", "scene": "sc", "branch": "main",
            "span": [90, 105], "rev": 1, "verified": True,
        })
        self.assertEqual(svc.frames["f"]["qualification"], OFFICIAL)
        self.assertEqual(svc.commands["v"]["status"], "confirmed")

    def test_unverified_ref_verified_later_promotes_frame(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r1", "scene": "sc", "branch": "main",
            "span": [0, 10], "rev": 1, "verified": False,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "main",
            "at_media": 5, "ref": "r1", "kind": "view", "deadline": 30,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "r1", "frame": "f",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertEqual(svc.frames["f"]["qualification_reason"], "ref_unverified")
        feed(svc, 3, "verify", {"ref": "r1"})
        self.assertEqual(svc.frames["f"]["qualification"], OFFICIAL)


class CaptureTests(unittest.TestCase):
    def test_capture_on_official_frame_succeeds_immediately(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "view", "deadline": 9,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "r", "frame": "f",
            "state": "ready", "edge_epoch": 1,
        })
        out = feed(svc, 3, "command", {
            "session": "s", "id": "shot", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "capture",
            "frame": "f", "deadline": 9,
        })
        self.assertEqual(svc.capture_status("shot"), CAPTURE_SUCCEEDED)
        self.assertIn("capture_succeeded", kinds(out))

    def test_capture_is_never_retargeted_to_new_frame(self):
        svc = Coordinator()
        for rid in ("old", "new"):
            feed(svc, 0, "reference", {
                "ref": rid, "scene": "sc", "branch": "b",
                "span": [0, 10], "rev": 1, "verified": True,
            })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "old", "kind": "view", "deadline": 99,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "old", "frame": "p-old",
            "state": "preview", "edge_epoch": 1,
        })
        feed(svc, 3, "command", {
            "session": "s", "id": "shot", "branch": "b",
            "at_media": 5, "ref": "old", "kind": "capture",
            "frame": "p-old", "deadline": 99,
        })
        self.assertEqual(svc.capture_status("shot"), CAPTURE_WAITING)

        # 新画面 new-f 已正式可用，与旧截图请求无关
        feed(svc, 4, "command", {
            "session": "s", "id": "v2", "branch": "b",
            "at_media": 5, "ref": "new", "kind": "view", "deadline": 99,
        })
        feed(svc, 5, "edge", {
            "request": "v2", "ref": "new", "frame": "new-f",
            "state": "ready", "edge_epoch": 1,
        })
        feed(svc, 6, "replace", {"old": "old", "new": "new"})

        cap = svc.commands["shot"]
        self.assertEqual(cap["status"], CAPTURE_INVALIDATED)
        self.assertEqual(cap["frame"], "p-old")  # 仍绑定原画面
        self.assertNotEqual(cap["frame"], "new-f")

    def test_capture_arriving_before_frame_is_reconciled_later(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "shot", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "capture",
            "frame": "fx", "deadline": 99,
        })
        self.assertEqual(svc.capture_status("shot"), CAPTURE_WAITING)
        # 视角指令与预览随后到达
        feed(svc, 2, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "view", "deadline": 99,
        })
        feed(svc, 3, "edge", {
            "request": "v", "ref": "r", "frame": "fx",
            "state": "preview", "edge_epoch": 1,
        })
        self.assertEqual(svc.capture_status("shot"), CAPTURE_WAITING)
        feed(svc, 4, "edge", {
            "request": "v", "ref": "r", "frame": "fx",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertEqual(svc.capture_status("shot"), CAPTURE_SUCCEEDED)

    def test_early_capture_invalidated_when_first_receipt_is_dead(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "k1", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 1, "reference", {
            "ref": "k2", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 2, "verified": True,
        })
        feed(svc, 2, "replace", {"old": "k1", "new": "k2"})
        # 截图先按 frame id 挂起
        feed(svc, 3, "command", {
            "session": "s", "id": "shot", "branch": "b",
            "at_media": 5, "ref": "k1", "kind": "capture",
            "frame": "late-frame", "deadline": 99,
        })
        feed(svc, 4, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "k1", "kind": "view", "deadline": 99,
        })
        out = feed(svc, 5, "edge", {
            "request": "v", "ref": "k1", "frame": "late-frame",
            "state": "ready", "edge_epoch": 1,
        })
        # 回执通过指令/版本核对并登记，但因参考已失效而 blocked
        self.assertTrue(out["accepted"])
        self.assertTrue(
            any(e.get("kind") == "receipt_blocked" for e in out["events"])
        )
        self.assertEqual(svc.frames["late-frame"]["qualification"], "rejected")
        self.assertEqual(svc.capture_status("shot"), CAPTURE_INVALIDATED)

    def test_different_ready_frame_does_not_promote_bound_preview(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "view", "deadline": 99,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "r", "frame": "p1",
            "state": "preview", "edge_epoch": 1,
        })
        feed(svc, 3, "command", {
            "session": "s", "id": "shot", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "capture",
            "frame": "p1", "deadline": 99,
        })
        # ready 回执给的是另一个画面身份 late：p1 不因此转正
        feed(svc, 4, "edge", {
            "request": "v", "ref": "r", "frame": "late",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertEqual(svc.frames["p1"]["qualification"], PREVIEW)
        self.assertEqual(svc.frames["late"]["qualification"], OFFICIAL)
        self.assertEqual(svc.capture_status("shot"), CAPTURE_WAITING)


class CorrectionAndWithdrawalTests(unittest.TestCase):
    def test_preview_explicitly_replaced_when_replacement_exists(self):
        svc = Coordinator()
        for rid in ("old", "new"):
            feed(svc, 0, "reference", {
                "ref": rid, "scene": "sc", "branch": "b",
                "span": [0, 10], "rev": int(rid == "new") + 1, "verified": True,
            })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "old", "kind": "view", "deadline": 99,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "old", "frame": "pold",
            "state": "preview", "edge_epoch": 1,
        })
        feed(svc, 3, "command", {
            "session": "s", "id": "v2", "branch": "b",
            "at_media": 5, "ref": "new", "kind": "view", "deadline": 99,
        })
        feed(svc, 4, "edge", {
            "request": "v2", "ref": "new", "frame": "pnew",
            "state": "preview", "edge_epoch": 1,
        })
        out = feed(svc, 5, "replace", {"old": "old", "new": "new"})
        replaced = [n for n in out["notifications"] if n["kind"] == "preview_replaced"]
        self.assertEqual(len(replaced), 1)
        self.assertEqual(replaced[0]["frame"], "pold")
        self.assertEqual(replaced[0]["new_frame"], "pnew")
        self.assertEqual(svc.frames["pold"]["qualification"], "replaced")

    def test_switch_confirmed_but_undisplayed_still_triggers_resync(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r1", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 0, "reference", {
            "ref": "r2", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 2, "verified": True,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "r1", "kind": "view", "deadline": 9,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "r1", "frame": "f",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertEqual(svc.commands["v"]["status"], "confirmed")
        # 客户端离线，没来得及 display
        feed(svc, 3, "replace", {"old": "r1", "new": "r2"})
        insp = svc.inspect("s")
        self.assertTrue(insp["needs_resync"])
        self.assertEqual(insp["displayed_frame_ids"], [])
        resume = svc.resume_session({"session": "s", "last_ack": 0})
        kinds_ = [n["kind"] for n in resume["notifications"]]
        self.assertIn("switch_confirmed", kinds_)
        self.assertIn("resync_required", kinds_)

    def test_display_of_dead_frame_piggybacks_pending_notices(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "k1", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": False,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "k1", "kind": "view", "deadline": 99,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "k1", "frame": "p",
            "state": "preview", "edge_epoch": 1,
        })
        feed(svc, 3, "reference", {
            "ref": "k2", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 2, "verified": True,
        })
        feed(svc, 4, "replace", {"old": "k1", "new": "k2"})
        # 客户端仍回执展示已被收回的预览：展示事实保留，同时带回收回通知
        out = feed(svc, 5, "display", {"session": "s", "frame": "p"})
        self.assertEqual(svc.inspect("s")["displayed_frame_ids"], ["p"])
        self.assertTrue(any(n["kind"] == "preview_withdrawn" for n in out["notifications"]))


    def test_new_generation_reconfirms_and_clears_resync(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r1", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 0, "reference", {
            "ref": "r2", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 2, "verified": True, "supersedes": "r1",
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "r1", "kind": "view", "deadline": 99,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "r1", "frame": "f1",
            "state": "ready", "edge_epoch": 1,
        })
        feed(svc, 3, "display", {"session": "s", "frame": "f1"})
        feed(svc, 4, "replace", {"old": "r1", "new": "r2"})
        self.assertTrue(svc.inspect("s")["needs_resync"])

        # 新边缘生产代基于更正后的 r2 给出正式画面：再次确认并解除重同步
        out = feed(svc, 5, "edge", {
            "request": "v", "ref": "r2", "frame": "f2",
            "state": "ready", "edge_epoch": 2,
        })
        switch_notes = [n for n in out["notifications"] if n["kind"] == "switch_confirmed"]
        self.assertEqual(len(switch_notes), 1)
        self.assertEqual(switch_notes[0]["frame"], "f2")
        insp = svc.inspect("s")
        self.assertFalse(insp["needs_resync"])
        # 当时展示 f1 的事实仍然保留
        self.assertEqual(insp["displayed_frame_ids"], ["f1"])


class BranchIsolationTests(unittest.TestCase):
    def test_independent_segment_and_session_continue(self):
        svc = Coordinator()
        # 两个互不相交的关键片段
        feed(svc, 0, "reference", {
            "ref": "kA", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 0, "reference", {
            "ref": "kB", "scene": "sc", "branch": "b",
            "span": [100, 110], "rev": 1, "verified": True,
        })
        feed(svc, 0, "reference", {
            "ref": "kA2", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 2, "verified": True,
        })
        for sid, req, ref, t, frame in (
            ("s1", "jA", "kA", 5, "fA"),
            ("s2", "jB", "kB", 105, "fB"),
        ):
            feed(svc, 1, "command", {
                "session": sid, "id": req, "branch": "b",
                "at_media": t, "ref": ref, "kind": "view", "deadline": 99,
            })
            feed(svc, 2, "edge", {
                "request": req, "ref": ref, "frame": frame,
                "state": "ready", "edge_epoch": 1,
            })
        self.assertEqual(svc.frames["fA"]["qualification"], OFFICIAL)
        self.assertEqual(svc.frames["fB"]["qualification"], OFFICIAL)

        feed(svc, 3, "replace", {"old": "kA", "new": "kA2"})
        self.assertEqual(svc.frames["fA"]["qualification"], "stale")
        # 独立片段与另一个播放会话不受影响
        self.assertEqual(svc.frames["fB"]["qualification"], OFFICIAL)
        self.assertFalse(svc.inspect("s2")["needs_resync"])
        self.assertTrue(svc.inspect("s1")["needs_resync"])

    def test_overlapping_refs_of_other_branch_never_used(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "m", "scene": "sc", "branch": "main",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 0, "reference", {
            "ref": "a", "scene": "sc", "branch": "alt",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "main",
            "at_media": 5, "ref": "m", "kind": "view", "deadline": 9,
        })
        # 边缘若回了 alt 分支的画面，协调服务必须拒收
        out = feed(svc, 2, "edge", {
            "request": "v", "ref": "a", "frame": "fa",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertFalse(out["accepted"])
        self.assertNotIn("fa", svc.frames)

        # main 失效后也不会自动改用 alt
        feed(svc, 3, "reference", {
            "ref": "m2", "scene": "sc", "branch": "main",
            "span": [0, 10], "rev": 2, "verified": True,
        })
        feed(svc, 4, "replace", {"old": "m", "new": "m2"})
        qual = svc._qualification_snapshot(svc.sessions["s"])
        self.assertNotEqual(qual["qualification"], OFFICIAL)
        self.assertIsNone(qual["current_frame"])


class ReceiptReconciliationTests(unittest.TestCase):
    def _basic_job(self, svc: Coordinator, epoch=1):
        feed(svc, 0, "reference", {
            "ref": "r1", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "j", "branch": "b",
            "at_media": 5, "ref": "r1", "kind": "view", "deadline": 99,
        })
        feed(svc, 2, "edge", {
            "request": "j", "ref": "r1", "frame": "f",
            "state": "preview", "edge_epoch": epoch,
        })

    def test_unknown_request_rejected(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": True,
        })
        out = feed(svc, 1, "edge", {
            "request": "ghost", "ref": "r", "frame": "z",
            "state": "ready", "edge_epoch": 1,
        })
        self.assertFalse(out["accepted"])
        self.assertEqual(out["events"][0]["reason"], "unknown_request")

    def test_same_epoch_wrong_ref_version_rejected_even_with_right_frame_time(self):
        svc = Coordinator()
        self._basic_job(svc)
        feed(svc, 0, "reference", {
            "ref": "r2", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 2, "verified": True,
        })
        out = feed(svc, 3, "edge", {
            "request": "j", "ref": "r2", "frame": "g",
            "state": "ready", "edge_epoch": 1, "frame_time": 5,
        })
        self.assertFalse(out["accepted"])
        self.assertEqual(out["events"][0]["reason"], "ref_version_mismatch")

    def test_old_epoch_late_receipt_rejected(self):
        svc = Coordinator()
        self._basic_job(svc, epoch=2)
        # 先出现更新的生产代
        feed(svc, 3, "edge", {
            "request": "j", "ref": "r1", "frame": "f2",
            "state": "ready", "edge_epoch": 3,
        })
        # epoch=1 从未见过且低于已知最大代：旧实例迟到回执
        out = feed(svc, 4, "edge", {
            "request": "j", "ref": "r1", "frame": "old",
            "state": "ready", "edge_epoch": 1, "frame_time": 5,
        })
        self.assertFalse(out["accepted"])
        self.assertEqual(out["events"][0]["reason"], "stale_epoch")

    def test_wrong_frame_time_rejected(self):
        svc = Coordinator()
        self._basic_job(svc)
        out = feed(svc, 3, "edge", {
            "request": "j", "ref": "r1", "frame": "g",
            "state": "ready", "edge_epoch": 1, "frame_time": 7,
        })
        self.assertFalse(out["accepted"])
        self.assertEqual(out["events"][0]["reason"], "frame_time_mismatch")

    def test_new_epoch_may_take_corrected_lineage(self):
        svc = Coordinator()
        self._basic_job(svc)
        feed(svc, 3, "reference", {
            "ref": "r2", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 2, "verified": True, "supersedes": "r1",
        })
        # 新生产代 epoch=2 可以接手更正后的同谱系参考
        out = feed(svc, 4, "edge", {
            "request": "j", "ref": "r2", "frame": "fnew",
            "state": "ready", "edge_epoch": 2,
        })
        self.assertTrue(out["accepted"])
        self.assertEqual(svc.frames["fnew"]["qualification"], OFFICIAL)


class MediaTimeQueryTests(unittest.TestCase):
    def test_filter_by_media_time(self):
        svc = Coordinator()
        for rid, start in (("r1", 90), ("r2", 200)):
            feed(svc, 0, "reference", {
                "ref": rid, "scene": "sc", "branch": "b",
                "span": [start, start + 15], "rev": 1, "verified": True,
            })
        feed(svc, 1, "command", {
            "session": "s", "id": "v1", "branch": "b",
            "at_media": 101, "ref": "r1", "kind": "view", "deadline": 99,
        })
        feed(svc, 2, "edge", {
            "request": "v1", "ref": "r1", "frame": "f101",
            "state": "ready", "edge_epoch": 1,
        })
        feed(svc, 3, "display", {"session": "s", "frame": "f101"})
        feed(svc, 4, "command", {
            "session": "s", "id": "shot101", "branch": "b",
            "at_media": 101, "ref": "r1", "kind": "capture",
            "frame": "f101", "deadline": 99,
        })
        feed(svc, 5, "command", {
            "session": "s", "id": "v2", "branch": "b",
            "at_media": 205, "ref": "r2", "kind": "view", "deadline": 99,
        })
        feed(svc, 6, "edge", {
            "request": "v2", "ref": "r2", "frame": "f205",
            "state": "preview", "edge_epoch": 1,
        })

        at101 = svc.inspect("s", at_media=101)
        self.assertEqual(at101["displayed_frame_ids"], ["f101"])
        self.assertEqual(list(at101["capture_status"]), ["shot101"])
        self.assertEqual([p["id"] for p in at101["preview_pending"]], [])

        at205 = svc.inspect("s", at_media=205)
        self.assertEqual(at205["displayed_frame_ids"], [])
        self.assertEqual(list(at205["capture_status"]), [])
        self.assertEqual([p["id"] for p in at205["preview_pending"]], ["f205"])

        # 区间包含也能命中（查询点落在 ref span 内）
        self.assertEqual(
            [d["frame"] for d in svc.query_displayed("s", at_media=95)], ["f101"]
        )
        # 不带媒体时间返回全部
        self.assertEqual(len(svc.inspect("s")["captures"]), 1)


class DeadlineTests(unittest.TestCase):
    def test_strictly_past_deadline_expires_intent_and_capture(self):
        svc = Coordinator()
        feed(svc, 0, "reference", {
            "ref": "r", "scene": "sc", "branch": "b",
            "span": [0, 10], "rev": 1, "verified": False,
        })
        feed(svc, 1, "command", {
            "session": "s", "id": "v", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "view", "deadline": 3,
        })
        feed(svc, 2, "edge", {
            "request": "v", "ref": "r", "frame": "p",
            "state": "preview", "edge_epoch": 1,
        })
        feed(svc, 2, "command", {
            "session": "s", "id": "shot", "branch": "b",
            "at_media": 5, "ref": "r", "kind": "capture",
            "frame": "p", "deadline": 3,
        })
        # at == deadline 仍有效
        feed(svc, 3, "inspect", {"session": "s"})
        self.assertEqual(svc.capture_status("shot"), CAPTURE_WAITING)
        self.assertEqual(svc.commands["v"]["status"], "preview")
        # at > deadline 才过期
        feed(svc, 4, "inspect", {"session": "s"})
        self.assertEqual(svc.capture_status("shot"), "expired")
        self.assertEqual(svc.commands["v"]["status"], "timeout")


class ContinuousIngestAndCliTests(unittest.TestCase):
    def test_run_jsonl_text_and_bad_message_does_not_break_stream(self):
        text = (
            json.dumps({"at": 0, "kind": "reference", "record": {
                "ref": "r", "scene": "sc", "branch": "b",
                "span": [0, 10], "rev": 1, "verified": True}})
            + "\n"
            + json.dumps({"at": 1, "kind": "wat", "record": {}})
            + "\n"
            + json.dumps({"at": 2, "kind": "command", "record": {
                "session": "s", "id": "v", "branch": "b",
                "at_media": 5, "ref": "r", "kind": "view", "deadline": 9}})
            + "\n"
        )
        outputs = run_jsonl_text(text)
        self.assertTrue(outputs[0]["accepted"])
        self.assertFalse(outputs[1]["accepted"])
        self.assertIn("未登记", outputs[1]["error"])
        self.assertTrue(outputs[2]["accepted"])

    def test_cli_runs_example_file(self):
        proc = subprocess.run(
            [sys.executable, str(ROOT / "coordinator.py"),
             str(EXAMPLES / "independent-view-commands.jsonl"), "--quiet-events"],
            capture_output=True, text=True, cwd=ROOT,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        lines = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
        self.assertEqual(len(lines), 13)
        self.assertTrue(all("accepted" in line for line in lines))

    def test_cli_stdin(self):
        payload = json.dumps({"at": 0, "kind": "inspect", "record": {"session": "s"}})
        proc = subprocess.run(
            [sys.executable, str(ROOT / "coordinator.py"), "--stdin"],
            input=payload, capture_output=True, text=True, cwd=ROOT,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["inspection"]["session"], "s")

    def test_temp_scenario_file_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "case.jsonl"
            rows = [
                {"at": 0, "kind": "reference", "record": {
                    "ref": "r", "scene": "sc", "branch": "b",
                    "span": [0, 10], "rev": 1, "verified": True}},
                {"at": 1, "kind": "inspect", "record": {"session": "new"}},
            ]
            path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
            svc, outputs = run_file(path)
            self.assertEqual(len(outputs), 2)
            self.assertEqual(svc.inspect("new")["session"], "new")


class AliasSurfaceTests(unittest.TestCase):
    def test_common_aliases_share_behavior(self):
        from service import Service
        from coordination import Coordinator as PkgCoordinator

        for cls in (Coordinator, Service, PkgCoordinator):
            svc = cls()
            self.assertIsInstance(svc.handle({
                "at": 0, "kind": "inspect", "record": {"session": "s"},
            }), dict)
            self.assertEqual(svc.query_state("s")["session"], "s")
            self.assertEqual(svc.get_state(session="s", at_media=3)["at_media"], 3)


if __name__ == "__main__":
    unittest.main()

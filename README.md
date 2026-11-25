# 交互直播的参考流与指令对齐

节目参考与观众操作由独立生产方提供。媒体区间左闭右开。仿真实例回传既有作业引用和画面身份。

本仓库在读取器之外，提供一个可连续接入三方消息的**消息协调服务**
（`coordinator.py`，仅用 Python 3.11 标准库；运行时只流转合成标识，不生成图像）。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **reference**　节目 scene、branch、span、rev，ref 唯一。base 是增量所依赖的关键参考，verified 初始可为 false。supersedes 引用直接前任，替换关键参考可使仍引用旧关键参考的增量失效。
- **command**　session 下的 id、branch、at_media、ref、kind。view 为切视角，capture 表示固定于所见 view。deadline 为逻辑秒点。capture 用 `frame`（或 `frame_id`）显式绑定某次画面。
- **verify**　节目确认给定 ref。
- **replace**　节目更正 old 指向 new，后者为已经发布的参考。
- **edge**　request、ref、frame 及 edge_epoch，state 为 preview 或 ready。edge_epoch 只标仿真实际生产代。
- **display**　指定 session 确认展示 frame。
- **resume**　指定 session 重连并报告 last_ack。 last_ack 是客户端已经处理的通知序号，尚未收到通知时为 0。
- **inspect**　查询 session 观察状态；record 可额外带 `at_media`（也接受 `media_time`）做媒体时间过滤。

## 示例

- base-replaced-after-preview.jsonl
- independent-view-commands.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

```bash
python3 scenario_reader.py examples
python3 -m unittest discover -v
```

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

---

## 协调服务入口

核心类 `Coordinator`（别名 `CoordinationService` / `MessageCoordinator` /
`LiveCoordinationService` / `Service`，`service.py`、`server.py`、`app.py`、
`coordination` 包均重新导出同一实现）。

### 1. 编程接口：连续接入

```python
from coordinator import Coordinator

svc = Coordinator()
result = svc.ingest({"at": 0, "kind": "reference", "record": {...}})
# result == {"at", "kind", "accepted", "notifications": [...], "events": [...]}
```

`ingest` 的别名：`handle` / `process` / `process_message` / `on_message` /
`receive_message` / `submit_message` / `push` / `feed`。也可用与消息种类同名、
省略 at 的记录级方法：`svc.reference(record)`、`svc.command(record)`、
`svc.edge(record)`、`svc.verify(record)`、`svc.replace(record)`、
`svc.display(record)`、`svc.resume(record)`；`svc.inspect(record_or_session, at_media)`
两种调用约定都接受。

### 2. 命令行：JSONL 进，JSONL 出

```bash
# 单个演练文件：每条输入消息输出一行协调结果
python3 coordinator.py examples/base-replaced-after-preview.jsonl

# 目录 / 多文件 / 标准输入
python3 coordinator.py --dir examples
cat case.jsonl | python3 coordinator.py --stdin

# 极简 HTTP 服务（标准库）
python3 coordinator.py --http 8765
#   POST /ingest   单条信封，或 {"messages": [ ... ]}
#   GET  /inspect?session=s1&at_media=101
#   POST /resume   {"session":"s1","last_ack":0}
#   GET  /state
```

`run_file(path)` 返回 `(service, outputs)`，`run_jsonl_text(text)` 直接处理整段 JSONL。

### 3. 查询：按会话与媒体时间

```python
svc.inspect("s1", at_media=101)      # 也可 svc.query("s1", 101)
svc.query_displayed("s1", 101)       # 已展示（保留当时事实）
svc.query_previews("s1", 101)        # 预览待确认（含 active_for_session 标记）
svc.query_resync("s1", 101)          # 需要重同步
svc.query_captures("s1", 101)        # 截图库（列表）
svc.query_captures(capture_id="shot")# 单条截图状态（dict）
svc.capture_status("shot")           # 只要状态字符串
svc.resume_session({"session": "s1", "last_ack": 0})
```

`inspect` 结果的四个核心键：

| 键 | 含义 |
| --- | --- |
| `displayed` / `shown` / `displayed_frame_ids` | 客户端 display 确认过的画面事实，更正后也保留 |
| `preview_pending` / `previews` | 仍是预览或 ready 但参考链未确认、尚未成为正式画面的画面 |
| `resync_required` / `resync_items` / `needs_resync` | 正式画面被更正后需要重同步的事项 |
| `captures` / `screenshots` / `capture_library` 与 `capture_status` / `screenshot_status` | 截图请求明细与 id→状态映射 |

另有 `active_preview`、`active_intent`、`current_frame`、`frame_qualification`
（别名 `state` / `current_qualification`）、`notifications`、
`unacked_notifications`。

## 协调状态机（与题目规则一一对应）

### 画面资格 `frame.qualification`

- `preview`：边缘预览，可快速上屏，`capture_eligible=false`。
- `unconfirmed`：边缘已报 ready，但参考未 verify 或其关键 base 缺失/未 verify。
- `official`（同义词 `available`，视图里 `official/available=true`）：edge ready
  且参考已确认、base 关键片段齐备并确认、且未被更正——这才是“指定视角真正切到”。
- `withdrawn` / `replaced`：预览被收回 / 被明确替换（带 `replacement_frame`）。
- `stale`：曾是正式画面，随后参考被更正；展示历史保留，但会话需要重同步。
- `rejected`：回执未通过核对，或迟到回执引用已失效的参考版本。

### 视角意图

- 新的 `view` 指令会把同会话此前仍处于 `pending` / `preview` 的意图标记为
  `coalesced`（`coalesced_by` 指向最新指令）；只有最新意图会产生
  `preview_available` 与 `switch_confirmed` 通知。已 `confirmed` 的历史不被合并。
- 分支是硬隔离：回执参考与指令 `branch` 不同一律 `receipt_rejected(branch_mismatch)`；
  更正后的替代画面也只在**同分支、同会话、同媒体时间、同修订谱系**内寻找，
  绝不会拿时间相交的另一节目分支画面顶替。

### 截图（显式绑定原画面）

- 绑定到 `official` 画面：立即 `succeeded`（并发出 `capture_succeeded` 通知）。
- 绑定到 `preview` / `unconfirmed`：`waiting`（可通过查询接口立即读到，不单独占用通知序号）；只有**同一 frame id** 随后成为
  `official` 时才 `succeeded`。别的 frame id（即使已 official）不能顶替。
- 参考更正导致原画面被收回/替换/变 stale：等待中的请求立即 `invalidated`
  并收到 `capture_invalidated`，绝不迁移到新画面；已终结状态不再变化。
- 超过 `deadline`（逻辑时间严格大于 deadline；deadline 秒点本身仍有效）：
  视角意图 `timeout`，截图 `expired`（视图同时给 `timed_out=true`）。

### 更正（replace）的影响范围

- 精确覆盖 old 参考，以及 base 链指向 old 的增量（“替换关键参考使旧增量失效”），
  不相关的独立片段/参考与其他会话原样继续。
-  provisional 画面：若已存在同谱系替代画面，发 `preview_replaced{new_frame}`，
  否则发 `preview_withdrawn`；绑定它的等待中截图同时失效。
- 正式画面：置 `stale`；凡是客户端已 display 的，**展示事实保留**，并向该会话发
  `resync_required`（含 `displayed=true` 与 `replacement_ref`）；即使没来得及
  display，只要曾回过 `switch_confirmed`，也会通知重同步。
- 新边缘生产代（更大的 `edge_epoch`）可接手更正后的同谱系参考并再次确认，
  确认后自动解除对应 resync；旧生产代的迟到回执按 `stale_epoch` 拒收。

### 边缘回执核对

每条 edge 回执依次核对：未知 request → 参考是否存在 →
`(request, edge_epoch)` 原来接受的 ref 版本是否一致
（`ref_version_mismatch`）→ 旧生产代是否迟到（`stale_epoch`）→
与指令的分支/场景/媒体区间/谱系兼容性 → 显式帧时间
（`frame_time` / `media_time` / `at_media` / `pts`，错误则 `frame_time_mismatch`）。
即使“指令 id 与参考版本都对、帧时间也正确”，只要参考链已被更正失效，
回执只会登记为 `rejected/<原因>`，不能产生正式画面或复活截图。

### 离线重连

通知按会话分配单调 `seq`（另有全局字符串 `id="<session>#<seq>"`）。
`resume {session, last_ack}` 返回：

- `notifications` / `unacked_notifications`：所有 `seq > last_ack` 的通知；
- `withdrawals`：其中的 `preview_withdrawn`；`retractions` 另含 `preview_replaced`；
- `resync_required`：当前仍待处理的重同步事项；
- `frame_qualification`：当前画面资格快照
  （`idle/pending/preview/unconfirmed/official/reference_corrected/resync_required/timeout`）。

重复 resume 幂等；`last_ack` 会推进会话确认位点。

## 两个示例的既定结果

**independent-view-commands.jsonl**

- `one` 被后到的 `two` 合并（`coalesced`），只有 `two/f2` 得到 `switch_confirmed`；
  `s2/other/fa` 独立确认。
- display `s1/f2` 后 `c1→c2` 更正：`f2` 保留在 `displayed` 中，s1 出现一条
  `resync_required`；`f1/f2=stale`， alternate 分支的 `fa` 仍为 `official`，s2 无需重同步。
- `resume s1 last_ack=1` 只取到 seq=2 的重同步通知。

**base-replaced-after-preview.jsonl**

- `p1` 预览送达但 `can_capture=false`；截图 `shot` 保持 `waiting`。
- `key1→key2` 跨越关键片段：`p1=withdrawn(base_ref_replaced)`，
  `shot=invalidated`，通知序列为
  `preview_available, preview_withdrawn, capture_invalidated`
  （`shot` 在指令同步返回与查询中一直是 `waiting`）。
- 迟到的 `view1/inc1/late ready epoch1` 通过指令与参考版本核对，但因 base 已被替换，
  画面资格为 `rejected`，不产生 `switch_confirmed`，也不影响已失效的截图。
- `resume s1 last_ack=0` 取回 3 条通知、`p1` 收回通知与
  `reference_corrected` 资格快照；最终 inspect 中 `preview_pending=[]`、
  `capture_status.shot=invalidated`、无已展示画面。

## 测试

```bash
python3 -m unittest discover -v
```

`test_coordinator.py` 把上述每条规则（分支隔离、意图合并、截图不迁移、
关键链确认、收回/替换/重同步、迟到回执与生产代、离线 resume、媒体时间过滤、
deadline、CLI/HTTP 冒烟）都固化为可执行断言；原有 `test_scenario_reader.py` 保持通过。

# 交互直播的参考流与指令对齐

节目参考与观众操作由独立生产方提供。媒体区间左闭右开。仿真实例回传既有作业引用和画面身份。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **reference**　节目 scene、branch、span、rev，ref 唯一。base 是增量所依赖的关键参考，verified 初始可为 false。supersedes 引用直接前任，替换关键参考可使仍引用旧关键参考的增量失效。
- **command**　session 下的 id、branch、at_media、ref、kind。view 为切视角，capture 表示固定于所见 view。deadline 为逻辑秒点。
- **verify**　节目确认给定 ref。
- **replace**　节目更正 old 指向 new，后者为已经发布的参考。
- **edge**　request、ref、frame 及 edge_epoch，state 为 preview 或 ready。edge_epoch 只标仿真实际生产代。
- **display**　指定 session 确认展示 frame。
- **resume**　指定 session 重连并报告 last_ack。 last_ack 是客户端已经处理的通知序号，尚未收到通知时为 0。
- **inspect**　查询 session 观察状态。

## 示例

- base-replaced-after-preview.jsonl
- independent-view-commands.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

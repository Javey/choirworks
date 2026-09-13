# ChoirWorks

ChoirWorks —— 多 Agent 协作工作群：人类像 CEO 一样提出目标，协调者拆解任务并把 Agent 拉进群，
成员之间可以互相 @、引用回复、中途求助；平台不执行业务动作，负责编排、调度 A2A subagent，
并支持暂停协助、断点恢复与平台侧回退。

当前进度：**M1–M10 全部完成**（骨架、LLM 规划与 DAG 调度、SSE、HITL、断点恢复与对账、
回退/retry/cancel、动态 DAG 协作、群聊协作模式），并附带**群聊式前端**（React SPA）。

## 开发环境

- Python 3.12（由 uv 管理）
- [uv](https://docs.astral.sh/uv/)
- Node.js 20+（仅前端）

```bash
uv sync
uv run pytest
```

## 前端对话界面

群聊主线：人类/Agent/assistant 气泡、@ 高亮与自动补全、引用回复、排队角标、在途「打断」、
流式输出与成员状态；右侧「任务详情」面板保留规划/节点/干预/回退能力。断线自动重连，
历史消息按 seq 游标增量拉取。

```bash
# 开发（Vite :5173，/v1 代理到 :8080）
cd frontend && npm install
npm run dev

# 生产构建（FastAPI 自动托管 frontend/dist，访问 http://127.0.0.1:8080）
npm run build

# 前端测试
npm test
```

## 模拟运行（离线演示）

无需 API Key、无需真实 Agent，一条命令拉起「假 Agent + 确定性规划器 + Hub + 前端」：

```bash
uv run choirworks-sim --port 8080 --fresh
```

启动后自动注册 6 个脚本化 Agent（researcher / writer / critic / analyst / flaky / broken）并打印示例请求，打开 `http://127.0.0.1:8080` 直接对话：

| 请求示例 | 演示场景 |
|---|---|
| 请协调多个子代理协作完成这项分析 | 动态 DAG 协作：两个 worker 分别暂停，编排器扩展协助节点（带「协助」徽章）执行后回填、续跑（上下文保留） |
| 帮我调研 A2A 协议并写一份摘要 | 调研 → 写作依赖链 |
| 帮我评审这段文案 | critic 提问 → 人工介入 → 定稿 |
| 这个任务可能会偶发失败，请自动重试 | 节点自动重试（第 2 次成功） |
| 模拟失败并降级替换 | 两次失败 → 重规划为 plan v2 |

`--db` 指定模拟数据库（默认 `data/sim.db`），`--fresh` 启动前清空。模拟 Agent 的产出默认按 **打字机效果** 分块流式返回（`--chunk-size` 每块字符数，默认 2；`--chunk-delay` 块间隔秒数，默认 0.04，设为 0 可关闭延迟）。规划逻辑为确定性规则（`src/choirworks/sim/llm.py`），全程不访问外部服务；假 Agent 行为定义在 `src/choirworks/sim/fake_agent.py`。

## 工作群（群聊协作）

群聊是编排前门：所有消息经 Hub 记录（事件溯源），Agent 产出自动成为群消息，assistant 负责拆解/派发/入群/完成播报。核心接口：

```bash
# 人类消息（quote_id 可引用任意消息；interrupt 需同时给 quote_id）
curl -X POST localhost:8080/v1/conversations/<conversation_id>/messages \
  -H 'content-type: application/json' \
  -d '{"text":"请协调多个子代理协作完成这项分析","mentions":["researcher"]}'

# 房间时间线（seq 游标增量拉取，含成员与摘要）
curl 'localhost:8080/v1/conversations/<conversation_id>/messages?since_seq=0'

# 房间事件流（message/room/plan/node/intervention 事件，SSE）
curl -N 'localhost:8080/v1/conversations/<conversation_id>/stream?since_seq=0'
```

行为约定：

- `@单人` 直接建单节点任务给该 Agent；`@多人` 自动入群并交由 Planner 拆解；Agent 消息里的 `@` 由协调者仲裁（复用/扩展/并入，防循环）。
- `quote_id` 引用：干预消息 → 直接作答；在途节点 → 排队补投（`continue` 时合并）；终态消息 → follow-up 新任务；`interrupt=true` → 取消当前任务并转交新任务。
- assistant 播报：拆解、派发、入群、协助、排队、完成总结，均可作为普通消息被引用。
- 时间线顺序：CEO 发言 → 协调者拆解 → 成员入群播报 → 派发与产出；协助链路为「A 求助 → 协调者安排 → C 入群 → C 执行」。
- 上下文按「房间头 + 摘要 + 与我相关 + 最近窗口」分级投喂给每次被唤醒的 Agent。


## 运行

```bash
cp config.example.yaml config.yaml   # 可选
export OPENAI_API_KEY=...            # Planner 使用的模型 Key
uv run choirworks                     # 默认 http://127.0.0.1:8080
```

## 接口速览

```bash
# 注册一个 A2A agent（Agent Card 会被拉取并缓存）
curl -X POST localhost:8080/v1/agents \
  -H 'content-type: application/json' \
  -d '{"name":"demo","card_url":"http://127.0.0.1:9001"}'

# 自动规划：不传 target，由 LLM 拆解 DAG 并自动调度
curl -X POST localhost:8080/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"request":"调研 A2A 协议并写一份摘要"}'

# 手动单节点（调试用）：传 target 后调用 dispatch
curl -X POST localhost:8080/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"request":"hello","target":{"agent_name":"demo"}}'
curl -X POST localhost:8080/v1/tasks/<task_id>/nodes/<node_id>/dispatch

# 查询快照
curl localhost:8080/v1/tasks/<task_id>

# SSE 事件流；断线重连携带 Last-Event-ID 头或 ?after_seq= 自动回放缺失事件
curl -N localhost:8080/v1/tasks/<task_id>/events

# 人工介入：subagent input-required 时按策略（auto_llm/peer_agent/human）处置
curl "localhost:8080/v1/tasks/<task_id>/interventions?status=pending"
curl -X POST localhost:8080/v1/tasks/<task_id>/interventions/<intervention_id> \
  -H 'content-type: application/json' -d '{"text":"在这里回答"}'

# Checkpoint 与回退（dry_run 只报告影响面；restart 发取消信号并重置 checkpoint 之后的节点）
curl localhost:8080/v1/tasks/<task_id>/checkpoints
curl -X POST localhost:8080/v1/tasks/<task_id>/rollback \
  -H 'content-type: application/json' \
  -d '{"checkpoint_id":"<ck>","mode":"dry_run"}'

# 单节点重试 / 取消整个任务
curl -X POST localhost:8080/v1/tasks/<task_id>/nodes/<node_id>/retry
curl -X POST localhost:8080/v1/tasks/<task_id>/cancel
```

## 恢复与对账

- 进程重启时自动扫描非终态任务：在途节点通过 `SubscribeToTask` 重新挂接，`planning` 任务重新规划，停放任务恢复等待。
- 周期任务用远程 `GetTask` 对账本地状态漂移（`recovery.reconcile_interval_seconds`，默认 30s）。
- Checkpoint 在每个节点完成后自动生成，是回退锚点。

## 设计文档与计划

- 设计：`docs/superpowers/specs/2026-09-12-a2a-orchestration-platform-design.md`
- Plan 1（M1）：`docs/superpowers/plans/2026-09-12-a2a-platform-m1-skeleton.md`
- Plan 2（M2+M3）：`docs/superpowers/plans/2026-09-12-a2a-platform-m2-m3-planner-scheduler-sse.md`
- Plan 3（M4）：`docs/superpowers/plans/2026-09-12-a2a-platform-m4-hitl.md`
- Plan 4（M5–M7）：`docs/superpowers/plans/2026-09-12-a2a-platform-m5-m7-recovery-rollback.md`
- 前端设计：`docs/superpowers/specs/2026-09-13-frontend-chat-design.md`

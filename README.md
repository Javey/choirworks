# ChoirWorks

> [!WARNING]
> **本仓库目前仍处于概念验证阶段，不具备任何实际使用价值。**
> 所有设计、接口与行为均可能随时大幅变更乃至推翻重做，不承诺任何形式的稳定性、
> 兼容性与安全性，请勿用于生产环境或任何真实业务。本文档描述的是愿景与现状，
> 而非可用性承诺。

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
# 开发（Vite :5173，/v1 代理到 :8567）
cd frontend && npm install
npm run dev

# 生产构建（FastAPI 自动托管 frontend/dist，访问 http://127.0.0.1:8567）
npm run build

# 前端测试
npm test
```

## 模拟运行（离线演示）

无需 API Key、无需真实 Agent，一条命令拉起「假 Agent + 确定性规划器 + Hub + 前端」：

```bash
uv run choirworks-sim --port 8567 --fresh
```

启动后自动注册 6 个脚本化 Agent（researcher / writer / critic / analyst / flaky / broken）并打印示例请求，打开 `http://127.0.0.1:8567` 直接对话：

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
curl -X POST localhost:8567/v1/conversations/<conversation_id>/messages \
  -H 'content-type: application/json' \
  -d '{"text":"请协调多个子代理协作完成这项分析","mentions":["researcher"]}'

# 房间时间线（seq 游标增量拉取，含成员与摘要）
curl 'localhost:8567/v1/conversations/<conversation_id>/messages?since_seq=0'

# 房间事件流（message/room/plan/node/intervention 事件，SSE）
curl -N 'localhost:8567/v1/conversations/<conversation_id>/stream?since_seq=0'
```

行为约定：

- `@单人` 直接建单节点任务给该 Agent；`@多人` 自动入群并交由 Planner 拆解；Agent 消息里的 `@` 由协调者仲裁（复用/扩展/并入，防循环）。
- `quote_id` 引用：干预消息 → 直接作答；在途节点 → 排队补投（`continue` 时合并）；终态消息 → follow-up 新任务；`interrupt=true` → 取消当前任务并转交新任务。
- assistant 播报：拆解、派发、入群、协助、排队、完成总结，均可作为普通消息被引用。
- 时间线顺序：CEO 发言 → 协调者拆解 → 成员入群播报 → 派发与产出；协助链路为「A 求助 → 协调者安排 → C 入群 → C 执行」。
- 上下文按「房间头 + 摘要 + 与我相关 + 最近窗口」分级投喂给每次被唤醒的 Agent。


## A2A 北向接口（AgentCard + JSON-RPC）

ChoirWorks 自身是一个标准 A2A v1.0 Server，可被任意 A2A client（包括另一个 ChoirWorks 实例）当作 agent 编排，支持 task 级群嵌套。

- AgentCard：`GET /.well-known/agent-card.json`
- JSON-RPC：`POST /v1/a2a`，方法：`SendMessage` / `SendStreamingMessage` / `GetTask` / `CancelTask` / `SubscribeToTask`
- 公共地址由 `a2a.public_url`（env `CHOIRWORKS_A2A__PUBLIC_URL`）配置
- 映射：hub task = 1 个 A2A Task（`conversation_id` = `contextId`），节点状态/产物经 `metadata` 与 `artifact_update` 流式表达；`input-required` 即干预问题
- 嵌套：把内层实例的 AgentCard 地址作为 `card_url` 注册为 agent，即可派发任务并把产物回传
- Room Extension v1：`conversation_id` 即合成 Room Task 的 `id`/`contextId`，`GetTask`/`SubscribeToTask`/`SendMessage` 均可直接操作房间；规范见 `docs/extensions/room-v1.md`

```bash
# AgentCard
curl -s localhost:8567/.well-known/agent-card.json | python -m json.tool

# 发消息（非流式）；带 contextId 续接同一群，带 taskId 作答/排队/续接
curl -s -X POST localhost:8567/v1/a2a -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"SendMessage",
  "params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"@researcher 请分析 X"}]}}
}'

# 流式订阅任务（SSE，data 帧为 JSON-RPC response，result 为 StreamResponse）
curl -s -N -X POST localhost:8567/v1/a2a -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":2,"method":"SubscribeToTask","params":{"id":"<task_id>"}
}'
```

> 当前仅支持 v1.0（未开启 v0.3 兼容）；`ListTasks` 与 push notification 返回不支持；房间级 A2A 暴露（Room Extension）在 M12 提供。


## 运行

```bash
cp config.example.yaml config.yaml   # 可选
export OPENAI_API_KEY=...            # Planner 使用的模型 Key
uv run choirworks                     # 默认 http://127.0.0.1:8567
```

## 接口速览

```bash
# 注册一个 A2A agent（Agent Card 会被拉取并缓存）
curl -X POST localhost:8567/v1/agents \
  -H 'content-type: application/json' \
  -d '{"name":"demo","card_url":"http://127.0.0.1:9001"}'

# 自动规划：不传 target，由 LLM 拆解 DAG 并自动调度
curl -X POST localhost:8567/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"request":"调研 A2A 协议并写一份摘要"}'

# 手动单节点（调试用）：传 target 后调用 dispatch
curl -X POST localhost:8567/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"request":"hello","target":{"agent_name":"demo"}}'
curl -X POST localhost:8567/v1/tasks/<task_id>/nodes/<node_id>/dispatch

# 查询快照
curl localhost:8567/v1/tasks/<task_id>

# SSE 事件流；断线重连携带 Last-Event-ID 头或 ?after_seq= 自动回放缺失事件
curl -N localhost:8567/v1/tasks/<task_id>/events

# 人工介入：subagent input-required 时按策略（auto_llm/peer_agent/human）处置
curl "localhost:8567/v1/tasks/<task_id>/interventions?status=pending"
curl -X POST localhost:8567/v1/tasks/<task_id>/interventions/<intervention_id> \
  -H 'content-type: application/json' -d '{"text":"在这里回答"}'

# Checkpoint 与回退（dry_run 只报告影响面；restart 发取消信号并重置 checkpoint 之后的节点）
curl localhost:8567/v1/tasks/<task_id>/checkpoints
curl -X POST localhost:8567/v1/tasks/<task_id>/rollback \
  -H 'content-type: application/json' \
  -d '{"checkpoint_id":"<ck>","mode":"dry_run"}'

# 单节点重试 / 取消整个任务
curl -X POST localhost:8567/v1/tasks/<task_id>/nodes/<node_id>/retry
curl -X POST localhost:8567/v1/tasks/<task_id>/cancel
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

## License

[MIT](LICENSE)

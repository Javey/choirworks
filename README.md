# ChoirWorks

> [!WARNING]
> **本仓库目前仍处于概念验证阶段，不具备任何实际使用价值。**
> 所有设计、接口与行为均可能随时大幅变更乃至推翻重做，不承诺任何形式的稳定性、
> 兼容性与安全性，请勿用于生产环境或任何真实业务。本文档描述的是愿景与现状，
> 而非可用性承诺。

ChoirWorks —— 多 Agent 协作工作群：人类像 CEO 一样提出目标，编排器拆解任务并把 Agent 拉进群，
成员之间可以互相 @、引用回复、中途求助；平台不执行业务动作，负责编排、调度 A2A subagent，
并支持非阻塞并行、暂停求助与断点恢复。

## 架构（A2A SDK 唯一实现）

- **协议层（`a2a/`）**：基于 `a2a-sdk` 的 A2A v1.0 Server（AgentCard / JSON-RPC / REST / SSE）；
  只放协议出入口：`card`（AgentCard）、`client`（南向调用）、`room`（群聊扩展 wire 契约）、
  `wire`（protobuf 事件编码）、`executor`（`ChoirWorksAgentExecutor` 适配器）、`recovery`（启动恢复）。
- **编排层（`orchestration/`）**：计划 DAG、节点调度、结果交接、人工介入、计划修订等业务核心。
  `ChoirWorksAgentExecutor` 挂载在 SDK `DefaultRequestHandlerV2` 上，每个 A2A Task
  由 `ActiveTask` 管理；`execute()` 路由一条消息后**立即返回**，节点由后台 runner 非阻塞执行。
- **状态层**：Plan / 节点 / 群成员 / 干预 / 排队消息写入 `Task.metadata`，由 SDK `TaskManager`
  合并、`DatabaseTaskStore` 持久化；重启时 `recover_tasks` 重挂非终态任务并重新跟踪远端工作。
- **旧版事件溯源编排栈**（`core/orchestrator`、`core/coordinator`、`store/event_store`、
  房间 REST facade）已删除，不再作为实现依据。

## 开发环境

- Python 3.12（由 uv 管理）
- [uv](https://docs.astral.sh/uv/)
- Node.js 20+（仅前端）

```bash
uv sync
uv run pytest
```

## 前端对话界面

群聊主线：人类/Agent/assistant 气泡、计划卡片、流式输出与成员状态；右侧「任务详情」面板。
发送消息走 `SendStreamingMessage`，发送流结束后若任务未终态，自动 `SubscribeToTask`
续订实时事件（前端 `useConversation`）；打开历史会话时先 `GetTask` 快照再续订。

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
| 请协调多个子代理协作完成这项分析 | researcher/writer 并行暂停 → 编排器拉 analyst 协助 → 回填续跑 |
| 帮我调研 A2A 协议并写一份摘要 | 调研 → 写作依赖链 |
| 帮我评审这段文案 | critic 提问 → 人工介入 → 定稿 |
| 这个任务可能会偶发失败，请自动重试 | 节点自动重试（第 2 次成功） |
| 模拟失败并降级替换 | 两次失败 → 重规划为 plan v2 |

`--db` 指定模拟数据库（默认 `data/sim.db`），`--fresh` 启动前清空。模拟 Agent 的产出默认按 **打字机效果** 分块流式返回（`--chunk-size` 每块字符数，默认 2；`--chunk-delay` 块间隔秒数，默认 0.04，设为 0 可关闭延迟）。规划逻辑为确定性规则（`src/choirworks/sim/litellm_mock.py`），全程不访问外部服务；假 Agent 行为定义在 `src/choirworks/sim/fake_agent.py`。

## 群聊交互约定

- **会话 = A2A Context**：同一 `contextId` 下的 Task 属于同一个群；`Task.metadata` 保存计划、
  节点、成员、干预与排队消息，`GetTask` 即快照。
- **非阻塞**：Agent 在后台工作，流式产出以 artifact / status 事件推送；`@`、引用、求助可随时进入。
- **求助处理**：subagent `input-required` 后由大模型统一分析——转交另一个 agent（建协助节点，
  完成后回填续跑）或转交人类（提问等待答复后续跑）；LLM 不可用时降级转人工。
- **引用与打断**：消息 metadata 的 `quote_id` 引用在途节点则排队补投、引用已完成产出则建接续节点；
  `interrupt=true` 取消当前节点并转入新节点。
- **动态组队**：计划涉及的 Agent 自动入群；用户 `@mention` 入群；Agent 间 `@` 由编排器仲裁
  （创建协助节点，带防环上限）。
- **无完成汇总**：最后一条 Agent 产出即最终结果；编排器不检查、不总结。

## A2A 北向接口

ChoirWorks 自身是一个标准 A2A v1.0 Server，可被任意 A2A client（包括另一个 ChoirWorks 实例）当作 agent 编排，支持 task 级群嵌套。

- AgentCard：`GET /.well-known/agent-card.json`
- JSON-RPC：`POST /v1/a2a`，方法：`SendMessage` / `SendStreamingMessage` / `GetTask` / `CancelTask` / `SubscribeToTask`
- REST：`POST /v1/message:send`、`POST /v1/message:stream`、`GET /v1/tasks/{id}`、`GET /v1/tasks/{id}:subscribe` 等
- 公共地址由 `a2a.public_url`（env `CHOIRWORKS_A2A__PUBLIC_URL`）配置
- 群聊元数据：`https://github.com/Javey/choirworks/extensions/room/v1` 承载 `mentions` /
  `quote_id` / `interrupt`，`sender` / `node_id` 等展示字段
- 嵌套：把内层实例的 AgentCard 地址注册为 agent，即可派发任务并把产物回传；对端返回控制权后
  本端自动 `GetTask` + `SubscribeToTask` 跟随后续事件

```bash
# AgentCard
curl -s localhost:8567/.well-known/agent-card.json | python -m json.tool

# 发消息（JSON-RPC）
curl -s -X POST localhost:8567/v1/a2a -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"SendMessage",
  "params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"帮我调研 A2A 协议"}]}}
}'

# REST 非流式
curl -s -X POST localhost:8567/v1/message:send -H 'content-type: application/json' -H 'A2A-Version: 1.0' -d '{
  "message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"帮我调研 A2A 协议"}]}
}'

# 订阅任务事件（SSE）
curl -s -N -X POST localhost:8567/v1/a2a -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":2,"method":"SubscribeToTask","params":{"id":"<task_id>"}
}'
```

> 当前仅支持 A2A v1.0（未开启 v0.3 兼容）；请求需带 `A2A-Version: 1.0`。

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

# 列出/删除/刷新 agent
curl localhost:8567/v1/agents
curl -X DELETE localhost:8567/v1/agents/<agent_id>
curl -X POST localhost:8567/v1/agents/<agent_id>/refresh

# 对话列表（按 context 聚合的便捷视图）
curl localhost:8567/v1/conversations

# 发消息：见上方 A2A 北向接口
# 查询快照 / 取消 / 订阅：GetTask / CancelTask / SubscribeToTask
# 回复 input-required：向同一 taskId 再发一条消息即可
```

## 恢复

- 进程重启时自动扫描非终态 Task，对在途节点重新 `SubscribeToTask`，对未派发节点继续调度。
- 非终态任务在没有活跃订阅者时的事件由 `Task.metadata` 快照兜底，`GetTask` 始终可用。
- 已终态 Task 不支持在原 Task 上继续对话；在同一 `contextId` 下发送新消息即可发起接续计划。

## 设计文档与计划

- 产品说明：`docs/product.md`
- 以下 `docs/superpowers/**` 为旧版事件溯源栈的历史设计与实施计划，仅作归档参考，
  与当前 A2A SDK 实现不一致：
  - `docs/superpowers/specs/2026-09-12-a2a-orchestration-platform-design.md`
  - `docs/superpowers/specs/2026-09-13-a2a-facade-design.md`
  - `docs/superpowers/specs/2026-09-13-group-chat-collaboration-design.md`
  - `docs/superpowers/plans/**`

## License

[MIT](LICENSE)

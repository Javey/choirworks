# Agent Hub

A2A 多 Agent 编排平台（MVP）。平台不执行业务动作，负责理解需求、拆分任务 DAG、
调度 A2A subagent，并支持暂停协助、断点恢复与平台侧回退。

当前进度：**M1 骨架与单节点派发**（手动指定 agent 的单节点计划；LLM 规划、SSE、
HITL、恢复与回退见后续计划）。

## 开发环境

- Python 3.12（由 uv 管理）
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
uv run pytest
```

## 运行

```bash
cp config.example.yaml config.yaml   # 可选
uv run agent-hub                     # 默认 http://127.0.0.1:8080
```

## M1 接口速览

```bash
# 注册一个 A2A agent（Agent Card 会被拉取并缓存）
curl -X POST localhost:8080/v1/agents \
  -H 'content-type: application/json' \
  -d '{"name":"demo","card_url":"http://127.0.0.1:9001"}'

# 提交任务（M1 直接用 target 指定 agent，单节点计划）
curl -X POST localhost:8080/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"request":"hello","target":{"agent_name":"demo"}}'

# 手动派发节点（M1 暂用手动派发，Plan 2 起由调度器自动派发）
curl -X POST localhost:8080/v1/tasks/<task_id>/nodes/<node_id>/dispatch

# 查询快照
curl localhost:8080/v1/tasks/<task_id>
```

## 设计文档与计划

- 设计：`docs/superpowers/specs/2026-09-12-a2a-orchestration-platform-design.md`
- Plan 1：`docs/superpowers/plans/2026-09-12-a2a-platform-m1-skeleton.md`

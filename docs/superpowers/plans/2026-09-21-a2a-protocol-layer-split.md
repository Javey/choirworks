# 计划：拆分 a2a 协议层与编排业务层

## 目标

`src/choirworks/a2a/` 只保留 A2A 协议出入口；计划/节点/调度等编排业务移入新包 `src/choirworks/orchestration/`。纯机械移动 + import 路径调整，无行为变化。

## 目标结构

```
src/choirworks/
├── a2a/                     # 协议出入口
│   ├── card.py              # AgentCard 构建
│   ├── client.py            # 南向 RemoteAgentClient
│   ├── room.py              # 群聊扩展 wire 契约
│   ├── wire.py              # 原 helpers.py 的纯 pb2 编码/时间辅助
│   ├── executor.py          # ChoirWorksAgentExecutor（适配器）
│   └── recovery.py          # SDK 层启动恢复
├── orchestration/           # 编排业务核心
│   ├── state.py  patch.py  markers.py  deps.py  context.py  session.py
│   ├── planning.py  runner.py  routing.py  node_executor.py  remote_caller.py
│   ├── repair.py  intervention.py  assist.py  rewind.py  registry.py
│   ├── events.py            # 出站事件编码
│   └── flows.py             # join_members / execute_function
└── core/util.py             # now_iso / truncate / as_model
```

## 模块归属

| 原路径 | 新路径 |
|---|---|
| `a2a/state.py` `patch.py` `markers.py` `deps.py` `context.py` `session.py` `planning.py` `runner.py` `routing.py` `node_executor.py` `remote_caller.py` `repair.py` `intervention.py` `assist.py` `rewind.py` `registry.py` `events.py` | `orchestration/*` |
| `a2a/helpers.py` 的 `join_members` / `execute_function` | `orchestration/flows.py` |
| `a2a/helpers.py` 的 `now_iso` / `truncate` / `as_model` | `core/util.py` |
| `a2a/helpers.py` 的 pb2 辅助（`struct`/`strip_none`/`join_text`/`status_update`/`function_call_part`） | `a2a/wire.py` |
| `a2a/card.py` `client.py` `room.py` `executor.py` `recovery.py` | 原地不动 |

`a2a/executor.py`、`a2a/recovery.py` 作为适配器允许导入 `orchestration.*`；其余 a2a 模块保持协议依赖。

## 执行步骤

1. 基线：全量 pytest（201 passed）；`git status` 干净、与 origin 同步。
2. `git mv` 17 个模块 + 建 `orchestration/__init__.py`；`git mv a2a/helpers.py a2a/wire.py`。
3. 拆分 helpers：新建 `core/util.py`、`orchestration/flows.py`，wire 只留 pb2 辅助。
4. 全量替换 import：`choirworks.a2a.<业务模块>` → `choirworks.orchestration.<模块>`；helpers 各符号按上表分流。
5. 更新测试 import；README 架构节补分层说明。
6. 验证：`ruff check src tests`、全量 pytest（对齐基线 201）、basedpyright 对比、`import choirworks.main / choirworks.sim.runner` 冒烟。
7. 单个机械重构 commit。

## 验证记录

- 基线（`2194153`，`git stash -u` 实测）：全量 `201 passed`；basedpyright `53 errors / 497 warnings`。
- 重构后：全量 `201 passed`；`ruff check src tests` 全绿；basedpyright `46 errors / 497 warnings`
  （较基线 -7，无新增）；`import choirworks.main / choirworks.sim.runner / choirworks.api.app` 冒烟通过。

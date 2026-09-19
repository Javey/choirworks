# 计划执行引擎 — 派发恢复 / 交活解读 / 结果交接 / 计划修订

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把休眠的派发引擎接回 `execute()` 路由，恢复 DAG 派发与人工介入，并新增交活解读（快车道标记 + LLM 兜底）、结果交接（直接依赖产出原文拼接）、增量计划修订（补丁）与"取消进行中节点需人类确认"。

**Architecture:** 沿用 A2A SDK V2 的 `ActiveTask` 模型——`execute()` 路由/规划后立即返回，后台 runner 常驻 `_event_queue_agent` 继续发事件（终态事件后才关闭）；会话状态真源仍为 `contexts` 表，节点由远端 A2A task 状态驱动；交活解读插在节点 `completed` 分支之前，一次结构化调用同时产出意图、协助目标（peer/human）与计划补丁；所有状态变更在 `runtime.lock` 内应用并 `_persist`。

**Tech Stack:** Python 3.12 + a2a-sdk 1.1.2 (`DefaultRequestHandlerV2`)、pydantic、aiosqlite、pytest。

**Spec:** `docs/product.md`（产品语义：交活解读 / 结果交接 / 计划修订 / 人类介入）

**前置：** `feat: contexts 会话状态真源 + 隐藏式轮次回退`（56be735）已完成。

**SDK 语义约束（已核实，实现必须遵守）：**
- `DefaultRequestHandler` 在 1.1.2 指向 `DefaultRequestHandlerV2`；每个 task 一个 `ActiveTask`，`execute()` 请求由 `_request_lock` 串行。
- `execute()` 返回后事件队列不关闭；后台 runner 可继续 `enqueue_event`，直到终态事件被消费。**终态后禁止再发事件**。
- `message:stream` 的单请求流在 `execute()` 返回时结束；前端随后 `resubscribeTask` 继续跟随。
- `input-required` 非终态；前端视为 settled，下一条消息通常开新 task，回答必须从 contexts 状态（pending interventions）恢复。

**关键约定（已拍板）：**
- 回执标记（快车道，零 LLM）：产出中一行 `[cw:deliver]` / `[cw:need_info] <内容>` / `[cw:assist] <内容>` / `[cw:revise] <理由>`；`need_info`/`assist` 同归求助；未命中走 LLM 解读。
- 解读复用同一 LLM；默认判 `deliver`；`target_agent` 用 `Literal` 枚举约束（群内名册 + 全局注册表候选）。
- 派发文本 = 任务要求 + 群内成员名册 + 回执约定 + 直接依赖产出（`quote_untrusted` 包裹，单段 2000 / 总 8000 截断）。
- 计划补丁：`add` / `invalidate` 增量修改；新增节点 id `x{patch_count}`；被作废 pending 节点的下游级联作废；进行中节点不自动取消。
- 进行中节点作废请求 → `confirm_cancel` 介入；目标节点先结束则介入自动 `expired`（重启/加载时归一化）。
- 失败修复从整计划重规划改为补丁式（保留 `replan_on_failure` 开关）。
- 禁止 `Any`（AGENTS.md）。

---

## 文件结构（增量）

```
src/choirworks/
  a2a/markers.py        # 回执标记解析
  a2a/state.py          # +patch_count、级联作废辅助、intervention.kind
  a2a/executor.py       # 路由恢复、交活解读、交接组装、补丁应用、confirm_cancel
  core/context.py       # +派发文本/名册/约定、解读与补丁提示词
tests/
  unit/test_markers.py
  unit/test_handoff.py
  unit/test_patch.py
  unit/test_interventions.py
  unit/test_recovery.py（改）
  integration/test_a2a_hitl.py（解 skip 适配）
  integration/test_a2a_queue.py（解 skip 适配）
  integration/test_a2a_recovery.py（解 skip 适配）
  integration/test_a2a_revise.py（新）
  support/fakes.py      # FakeLLM 支持解读/补丁响应
  sim/fake_agent.py     # +文本求助/建议改计划行为
frontend/src/lib/conversationView.ts  # +新事件渲染
```

---

## M1 派发恢复 + 交活解读 + 结果交接

### Task 1: 回执标记解析

**Files:**
- Create: `src/choirworks/a2a/markers.py`
- Test: `tests/unit/test_markers.py`

- [x] **Step 1: 写失败测试**

```python
from choirworks.a2a.markers import Marker, parse_marker


def test_need_info_marker():
    assert parse_marker("分析完成\n[cw:need_info] 需要 2024 营收数据\n") == Marker(
        intent="need_info", text="需要 2024 营收数据"
    )


def test_deliver_marker():
    assert parse_marker("[cw:deliver]\n报告如下……").intent == "deliver"


def test_assist_maps_to_need_info():
    assert parse_marker("[cw:assist] 需要 designer 帮忙").intent == "need_info"


def test_revise_marker():
    assert parse_marker("…\n[cw:revise] 不再需要 writer").intent == "revise"


def test_no_marker():
    assert parse_marker("普通交付内容") is None


def test_first_marker_wins():
    text = "[cw:need_info] A\n[cw:revise] B"
    assert parse_marker(text).intent == "need_info"
```

- [x] **Step 2: 跑测试确认失败** `uv run pytest tests/unit/test_markers.py -q`
- [x] **Step 3: 实现**（`Marker` dataclass + 多行正则 `^\[cw:(deliver|need_info|assist|revise)\]\s*(.*)$`，首个命中；`assist`→`need_info`）
- [x] **Step 4: 跑测试通过 + `uv run ruff check src tests`**

### Task 2: 派发文本组装（名册 + 约定 + 依赖产出）

**Files:**
- Modify: `src/choirworks/core/context.py`
- Test: `tests/unit/test_handoff.py`

- [x] **Step 1: 写失败测试**（覆盖：直接依赖才拼接、间接依赖不拼、fencing 标记、单段截断、总长截断、名册只含群成员、约定段落存在、续跑文本含答复）
- [x] **Step 2: 实现** `build_dispatch_text(node, state, members) -> str`、`build_continuation_text(..., answer)`、`RECEIPT_CONVENTION`；复用 `quote_untrusted`
- [x] **Step 3: 测试通过 + ruff**

### Task 3: `execute()` 路由恢复

**Files:**
- Modify: `src/choirworks/a2a/executor.py`
- Test: `tests/integration/test_a2a_send.py`（补充路由用例）

- [x] **Step 1: 写失败测试**（新消息有 pending intervention → 答复；有在途工作 → 排队；空闲 → 新计划并启动 runner）
- [x] **Step 2: 实现** 路由顺序：`pending_interventions → resume → runner 活跃/has_pending_work → _route_message → start_new_plan + _plan_and_launch`；删除 execute 内重复建节点代码；恢复 mentions 提取；**移除 `updater.complete()`**（runner 发终态）
- [x] **Step 3: 集成测试通过 + ruff**

### Task 4: 交活解读接入

**Files:**
- Modify: `src/choirworks/a2a/executor.py`、`src/choirworks/core/context.py`
- Test: `tests/unit/test_interpret.py`（新增）、`tests/integration/test_a2a_hitl.py`（解 skip 适配）

- [x] **Step 1: 写失败测试**（快车道 need_info → 节点 input_required；未命中 → LLM 解读；默认 deliver；远端 input_required → 同一合并调用；target_agent 枚举约束）
- [x] **Step 2: 实现** `OutcomeDecision` schema + `build_outcome_prompt` + `_interpret_outcome`；`_execute_node` completed 分支改为：先解读 → deliver/completed（原逻辑）/ need_info（`node.status="input_required"` + `node.question`，交 `_settle_input`）/ revise（M2 接补丁，M1 先按 deliver 处理并记日志）；`_decide_assistance` 并入
- [x] **Step 3: 测试通过 + ruff**

### Task 5: fake agent 行为 + 解 skip

**Files:**
- Modify: `src/choirworks/sim/fake_agent.py`、`tests/support/fakes.py`、`tests/integration/test_a2a_hitl.py`、`tests/integration/test_a2a_queue.py`、`tests/integration/test_a2a_recovery.py`
- Test: 上述集成测试

- [x] **Step 1: 新增行为** `needs_info_text`（首次回复 `[cw:need_info] …`，被续跑后正常交付）
- [x] **Step 2: 逐个解 skip，按新语义（contexts 状态、SessionRuntime）适配断言**
- [x] **Step 3: 全量** `uv run pytest tests -q -p no:cacheprovider` + ruff

---

## M2 计划修订

### Task 6: 补丁模型与应用

**Files:**
- Modify: `src/choirworks/a2a/state.py`（`patch_count`、级联作废）、`src/choirworks/a2a/executor.py`（`_apply_patch`）
- Test: `tests/unit/test_patch.py`

- [x] **Step 1: 写失败测试**（新增节点接依赖、id 唯一、作废未开始节点、级联作废下游、进行中节点不自动取消、作废后 has_pending_work/all_completed 语义）
- [x] **Step 2: 实现** `PlanPatch`/`PatchNode` + `_apply_patch`（校验 agent 存在、deps 存在、发出 `plan.revised`/`node.invalidated` 事件、拉新成员入群、`_persist`）
- [x] **Step 3: 测试通过 + ruff**

### Task 7: 失败修复改补丁式

**Files:**
- Modify: `src/choirworks/a2a/executor.py`
- Test: `tests/unit/test_patch.py` / `tests/integration/test_a2a_send.py`

- [x] **Step 1: 写失败测试**（重试耗尽的失败节点 → 补丁修复；补丁失败 → `task.failed`；`replan_on_failure=False` 直接失败）
- [x] **Step 2: 实现** `_repair_plan` 取代 `_replan` 的整计划清空（保留 `_replan` 删除）
- [x] **Step 3: 测试通过 + ruff**

### Task 8: confirm_cancel 介入

**Files:**
- Modify: `src/choirworks/a2a/state.py`（`Intervention.kind/target_node_id/status expired`）、`src/choirworks/a2a/executor.py`
- Test: `tests/unit/test_interventions.py`

- [x] **Step 1: 写失败测试**（进行中作废 → 生成 confirm_cancel；人类确认 → cancel + 级联；节点先结束 → 自动过期；重启归一化；同节点去重；pending 过滤 expired）
- [x] **Step 2: 实现** `_request_cancel_confirmation`、`_expire_cancel_requests`（节点三分支终止处调用）、`_answer_intervention` 分流与 yes/no 归一化
- [x] **Step 3: 测试通过 + ruff**

### Task 9: 修订链路集成测试

**Files:**
- Test: `tests/integration/test_a2a_revise.py`（新建）

- [x] **Step 1: 场景** 依赖链交付后解读 `[cw:revise]` → 作废未开始节点/新增 agent 入群；进行中取消需确认；过期路径
- [x] **Step 2: 全量 pytest + ruff**

---

## M3 前端 + 联调

### Task 10: 事件渲染

**Files:**
- Modify: `frontend/src/lib/conversationView.ts`
- Test: `frontend/src/lib/__tests__/conversationView.test.ts`

- [x] **Step 1: 写失败测试**（`plan.revised`、`node.invalidated`、`intervention.requested`（后端已用名）、`task.requires_input`、`intervention.expired` 渲染；修正 `intervention.question` 不匹配）
- [x] **Step 2: 实现** + `npm test`（workdir `frontend`）

### Task 11: 验证收尾

- [x] `uv run pytest tests -q -p no:cacheprovider`
- [x] `uv run ruff check src tests`
- [x] `timeout 600 uvx basedpyright src`（不新增错误）
- [x] `cd frontend && npm test && npm run build`
- [x] `uv run choirworks-sim --fresh` 手测：依赖链交接 / 文本求助→协助→续跑 / 人类答复 / 修订 / 回退


---

## 执行记录（2026-09-19）

- M1–M3 全部完成：`uv run pytest tests -q -p no:cacheprovider` 142 passed；ruff 全绿；basedpyright 与改动前同为 13 个既有错误；前端 26 passed + build。
- 与原计划的差异：
  - 回执标记只认「首个非空行」，避免 agent 回显派发文本（含标记示例）造成误判。
  - 名册改为不带 `@` 的列表，避免 echo 类 agent 回显名册触发误仲裁。
  - `all_completed()` 把 `canceled` 视为已结算（人类确认取消后计划可正常完成）。
  - `confirm_cancel` 不把 task 置为 input-required（任务仍在进行），人类在任务未结算前用同一 task 回复「确认」即可打断。
  - 自造 status 事件补上 timestamp（task store 以 `status.timestamp` 排序，回退依赖该顺序）。
  - SQLite 连接补 `busy_timeout`（派发后编排器与 SDK 并发写库需要）。
  - sim 的 `REQUEST_PATTERN` 适配带 fencing preamble 的规划提示词。
- 未覆盖/后续：多条 pending intervention 的定向答复（PRD 要求「答复需指明针对哪个提问」，当前按第一条处理）；T2 依赖边界评估与增量修订已实现，但"编排器主动校验"仍以交活解读为入口。

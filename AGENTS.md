# AGENTS.md

## 核心原则

**如果用不到，就不要提前实现它。**

**禁止使用 Any 类型。**

**项目处于开发阶段，不考虑历史数据兼容与迁移，可以放心重命名和调整结构。**

**`__init__.py` 保持空代码，可以添加注释说明。**

## ADK 借鉴规则

**借鉴 ADK 编写 `core/` 时保持同构**：方法名、调用链、方法拆分与 ADK 一一对应。不合并、不内联、不简化结构。即使方法再简单也要独立存在，和 ADK 保持一致。除非用户主动提出变更。

## 版权要求

**引用 Google ADK 代码时必须标明来源和许可证。**

## 验证命令

- 后端测试：`uv run pytest tests -q`
- Lint：`uv run ruff check --fix src tests` && `uv run ruff format src tests`
- 类型检查：`uv run basedpyright`（要求 0 errors；规则配置见 `pyproject.toml` 的 `[tool.basedpyright]`）
- 前端（`frontend/`）：`npm test`、`npm run build`、`npm run lint`

**按改动范围执行**：只有 `src/` 或 `tests/` 变更时才跑上述后端三项 + 前端（涉及时）；
纯文档改动（`docs/`、`*.md` 等）直接提交，不触发全量测试。

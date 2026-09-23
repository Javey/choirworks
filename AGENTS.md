# AGENTS.md

## 核心原则

**如果用不到，就不要提前实现它。**

**禁止使用 Any 类型。**

**项目处于开发阶段，不考虑历史数据兼容与迁移，可以放心重命名和调整结构。**

## 版权要求

**引用 Google ADK 代码时必须标明来源和许可证。**

## 验证命令

- 后端测试：`uv run pytest tests -q`
- Lint：`uv run ruff check --fix src tests` && `uv run ruff format src tests`
- 类型检查：`uv run basedpyright`（要求 0 errors；规则配置见 `pyproject.toml` 的 `[tool.basedpyright]`）
- 前端（`frontend/`）：`npm test`、`npm run build`、`npm run lint`

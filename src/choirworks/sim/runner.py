from __future__ import annotations

import argparse
import asyncio
import contextlib
from pathlib import Path

import uvicorn

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.llm import LiteLLMClient
from choirworks.sim.fake_agent import FakeAgent, start_fake_agent
from choirworks.sim.litellm_mock import sim_acompletion

SIM_AGENTS: list[tuple[str, str]] = [
    ("product-manager", "collaborate"),
    ("developer", "inquire"),
    ("code-reviewer", "review"),
    ("qa-engineer", "assist"),
    ("finance-analyst", "echo"),
    ("approval-manager", "flaky_once"),
    ("auditor", "flaky_always"),
]

EXAMPLES = [
    "请协调团队完成这个功能的需求分析和开发",
    "帮我调研技术方案并写一份设计文档",
    "请审查这段代码的安全性和质量",
    "帮我分析上季度的财务数据并生成报告",
    "请审批这笔采购申请",
]


def build_settings(host: str, port: int, db_path: Path) -> Settings:
    return Settings(
        server={"host": host, "port": port},
        store={"db_path": db_path},
        a2a={"public_url": f"http://{host}:{port}"},
        scheduler={"retry_backoff_seconds": 0.2},
    )


async def start_sim_agents(
    *, chunk_size: int = 2, chunk_delay: float = 0.04
) -> list[tuple[str, FakeAgent]]:
    return [
        (
            name,
            await start_fake_agent(
                behavior, name=name, chunk_size=chunk_size, chunk_delay=chunk_delay
            ),
        )
        for name, behavior in SIM_AGENTS
    ]


def _reset_db(db_path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        Path(f"{db_path}{suffix}").unlink(missing_ok=True)


async def run(
    host: str,
    port: int,
    db_path: Path,
    fresh: bool,
    *,
    chunk_size: int = 2,
    chunk_delay: float = 0.04,
) -> None:
    if fresh:
        _reset_db(db_path)
    agents = await start_sim_agents(chunk_size=chunk_size, chunk_delay=chunk_delay)
    settings = build_settings(host, port, db_path)
    app = await create_app(
        settings,
        llm=LiteLLMClient(model="sim", completion_fn=sim_acompletion),
    )
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    try:
        while not server.started:  # noqa: ASYNC110 - 轮询 uvicorn 启动状态
            await asyncio.sleep(0.05)
        for name, agent in agents:
            await app.state.registry.register(name, agent.url)
        _print_banner(host, port, agents)
        await server_task
    finally:
        server_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server_task
        for _, agent in agents:
            await agent.stop()


def _print_banner(host: str, port: int, agents: list[tuple[str, FakeAgent]]) -> None:
    lines = [
        "",
        f"ChoirWorks 模拟环境已启动：http://{host}:{port}",
        "模拟 Agent：",
    ]
    lines += [f"  - {name:<11} {agent.url}  ({dict(SIM_AGENTS)[name]})" for name, agent in agents]
    lines += ["", "试试这些请求："]
    lines += [f"  - {example}" for example in EXAMPLES]
    lines += [
        "",
        "Ctrl-C 退出。",
        "",
    ]
    print("\n".join(lines), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="choirworks-sim",
        description="离线模拟运行 ChoirWorks（假 Agent + 确定性规划器，无需 API Key）",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8567)
    parser.add_argument("--db", default="data/sim.db", help="模拟数据库路径")
    parser.add_argument("--fresh", action="store_true", help="启动前清空模拟数据库")
    parser.add_argument(
        "--chunk-size", type=int, default=2, help="打字机每块字符数（0 关闭流式）"
    )
    parser.add_argument(
        "--chunk-delay", type=float, default=0.04, help="打字机块间隔秒数"
    )
    args = parser.parse_args()
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(
            run(
                args.host,
                args.port,
                Path(args.db),
                args.fresh,
                chunk_size=args.chunk_size,
                chunk_delay=args.chunk_delay,
            )
        )


if __name__ == "__main__":
    main()

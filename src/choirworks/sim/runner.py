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
    ("researcher", "collaborate"),
    ("writer", "inquire"),
    ("critic", "review"),
    ("analyst", "assist"),
    ("flaky", "flaky_once"),
    ("broken", "flaky_always"),
]

EXAMPLES = [
    "请协调多个子代理协作完成这项分析",
    "帮我调研 A2A 协议并写一份摘要",
    "帮我评审这段文案",
    "这个任务可能会偶发失败，请自动重试",
    "模拟失败并降级替换",
]


def build_settings(host: str, port: int, db_path: Path) -> Settings:
    return Settings(
        server={"host": host, "port": port},
        store={"db_path": db_path},
        policies={
            "overrides": [
                {"agent_name": "critic", "policy": "human"},
                {"agent_name": "researcher", "policy": "peer_agent"},
                {"agent_name": "writer", "policy": "peer_agent"},
            ]
        },
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
        "前端已构建时直接访问根路径；Ctrl-C 退出。",
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

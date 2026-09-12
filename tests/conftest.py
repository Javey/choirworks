import pytest

from tests.fake_agents.echo_agent import FakeAgent, start_fake_agent


@pytest.fixture(autouse=True)
def reset_sse_app_status():
    from sse_starlette.sse import AppStatus

    AppStatus.should_exit = False
    yield
    AppStatus.should_exit = False


@pytest.fixture
async def echo_agent() -> FakeAgent:
    agent = await start_fake_agent("echo")
    yield agent
    await agent.stop()


@pytest.fixture
async def ask_agent() -> FakeAgent:
    agent = await start_fake_agent("ask")
    yield agent
    await agent.stop()

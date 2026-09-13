import httpx

from choirworks.api.app import create_app
from choirworks.config import Settings


async def test_agent_card_served(tmp_path):
    settings = Settings(
        store={"db_path": tmp_path / "card.db"},
        a2a={"public_url": "http://127.0.0.1:9999"},
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get("/.well-known/agent-card.json")
    assert resp.status_code == 200
    card = resp.json()
    assert card["name"] == "ChoirWorks"
    assert card["capabilities"]["streaming"] is True
    assert card["supportedInterfaces"][0]["protocolBinding"] == "JSONRPC"
    assert card["supportedInterfaces"][0]["url"] == "http://127.0.0.1:9999/v1/a2a"

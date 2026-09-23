import httpx

from choirworks.a2a.card import A2A_ROOM_URI
from choirworks.api.app import create_app
from choirworks.config import Settings


async def test_agent_card_served(tmp_path):
    settings = Settings(
        store={"db_path": tmp_path / "card.db"},
        a2a={"public_url": "http://127.0.0.1:9999"},
    )
    app = await create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/.well-known/agent-card.json")
    assert resp.status_code == 200
    card = resp.json()
    assert card["name"] == "ChoirWorks"
    assert card["capabilities"]["streaming"] is True
    interfaces = {item["protocolBinding"]: item["url"] for item in card["supportedInterfaces"]}
    assert interfaces["JSONRPC"] == "http://127.0.0.1:9999/v1/a2a"
    assert interfaces["HTTP+JSON"] == "http://127.0.0.1:9999/v1"
    extensions = card["capabilities"]["extensions"]
    assert extensions[0]["uri"] == A2A_ROOM_URI
    assert extensions[0].get("required", False) is False

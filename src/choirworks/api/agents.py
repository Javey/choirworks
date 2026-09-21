from __future__ import annotations

from a2a.client.errors import AgentCardResolutionError
from fastapi import APIRouter, HTTPException, Request

from choirworks.models.domain import AgentRecord, AgentRegistration
from choirworks.orchestration.registry import DuplicateAgentName

router = APIRouter(tags=["agents"])


@router.post("/agents", status_code=201, response_model=AgentRecord)
async def register_agent(body: AgentRegistration, request: Request) -> AgentRecord:
    try:
        return await request.app.state.registry.register(body.name, body.card_url)
    except DuplicateAgentName as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except AgentCardResolutionError as exc:
        raise HTTPException(status_code=400, detail=f"failed to resolve agent card: {exc}") from exc


@router.get("/agents", response_model=list[AgentRecord])
async def list_agents(request: Request) -> list[AgentRecord]:
    return await request.app.state.registry.list()


@router.delete("/agents/{agent_id}", status_code=204)
async def delete_agent(agent_id: str, request: Request) -> None:
    if not await request.app.state.registry.delete(agent_id):
        raise HTTPException(status_code=404, detail=f"agent not found: {agent_id}")


@router.post("/agents/{agent_id}/refresh", response_model=AgentRecord)
async def refresh_agent(agent_id: str, request: Request) -> AgentRecord:
    try:
        return await request.app.state.registry.refresh(agent_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=exc.args[0]) from exc
    except AgentCardResolutionError as exc:
        raise HTTPException(status_code=502, detail=f"failed to refresh agent card: {exc}") from exc

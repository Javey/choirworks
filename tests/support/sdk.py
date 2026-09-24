from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import (
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    TaskState,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict, ParseDict

from choirworks.api.app import create_app
from choirworks.config import Settings
from tests.support.fakes import FakeLLM

TERMINAL_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_CANCELED,
    TaskState.TASK_STATE_REJECTED,
}
SETTLED_STATES = TERMINAL_STATES | {TaskState.TASK_STATE_INPUT_REQUIRED}


@contextlib.asynccontextmanager
async def sdk_hub(
    tmp_path,
    db_name: str = "hub.db",
    *,
    settings: Settings | None = None,
    plans: list[Any] | None = None,
    llm: Any | None = None,
    streaming: bool = False,
) -> AsyncIterator[tuple[Any, httpx.AsyncClient, Any]]:
    resolved = settings or Settings(
        store={"db_path": tmp_path / db_name},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = await create_app(resolved, llm=llm or FakeLLM(structured_results=list(plans or [])))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
            card = await A2ACardResolver(httpx_client=http, base_url="http://test").get_agent_card()
            client = await create_client(
                agent=card,
                client_config=ClientConfig(streaming=streaming, httpx_client=http),
            )
            try:
                yield app, http, client
            finally:
                await client.close()


async def wait_for_task(client, task_id: str, states=SETTLED_STATES, timeout_seconds: float = 20.0):
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    task = None
    while asyncio.get_event_loop().time() < deadline:
        task = await client.get_task(GetTaskRequest(id=task_id))
        if task.status.state in states:
            return task
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"task {task_id} did not reach {states}: "
        f"{TaskState.Name(task.status.state) if task else 'no snapshot'}"
    )


def task_metadata(task) -> dict:
    return MessageToDict(task.metadata, preserving_proto_field_name=True)


def task_nodes(task) -> dict[str, dict]:
    meta = task_metadata(task)
    raw = meta.get("choirworks.state")
    if raw is None:
        return {}
    state = json.loads(raw) if isinstance(raw, str) else raw
    return {node["id"]: node for node in state.get("nodes", [])}


def task_state(task) -> dict:
    raw = task_metadata(task).get("choirworks.state")
    if raw is None:
        return {}
    return json.loads(raw) if isinstance(raw, str) else raw


async def context_state(app, context_id: str) -> dict:
    """Canonical conversation state from the contexts table."""
    record = await app.state.context_store.get(context_id)
    if record is None:
        return {}
    return json.loads(record.state)


async def context_nodes(app, context_id: str) -> dict[str, dict]:
    state = await context_state(app, context_id)
    return {node["id"]: node for node in state.get("nodes", [])}


def task_artifact_text(task) -> str:
    return " ".join(
        part.text for artifact in task.artifacts for part in artifact.parts if part.HasField("text")
    )


def pending_intervention_id(task) -> str:
    """Return the id of the first pending intervention in a task snapshot."""
    for item in task_state(task).get("interventions", []):
        if item.get("status") == "pending":
            return str(item.get("id", ""))
    raise AssertionError(f"no pending intervention: {task_state(task).get('interventions')}")


async def wait_for_pending_interventions(
    client, task_id: str, count: int, timeout_seconds: float = 20.0
):
    """Wait until the task snapshot has at least *count* pending interventions."""
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    task = None
    while asyncio.get_event_loop().time() < deadline:
        task = await client.get_task(GetTaskRequest(id=task_id))
        pending = [
            item
            for item in task_state(task).get("interventions", [])
            if item.get("status") == "pending"
        ]
        if len(pending) >= count:
            return task, pending
        await asyncio.sleep(0.05)
    raise AssertionError(f"task {task_id} did not reach {count} pending interventions")


async def wait_for_question_parts(client, task_id: str, count: int, timeout_seconds: float = 20.0):
    """Wait until the task's status.message carries *count* question data parts."""
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    task = None
    while asyncio.get_event_loop().time() < deadline:
        task = await client.get_task(GetTaskRequest(id=task_id))
        parts = [part for part in task.status.message.parts if part.WhichOneof("content") == "data"]
        if len(parts) >= count:
            return task
        await asyncio.sleep(0.05)
    raise AssertionError(f"task {task_id} did not reach {count} question parts")


def question_ids_from_status(task) -> list[str]:
    """Intervention ids carried by the task's current question message."""
    ids: list[str] = []
    for part in task.status.message.parts:
        if part.WhichOneof("content") != "data":
            continue
        kind = part.metadata.fields.get("cw_type")
        if kind is None or kind.string_value != "question":
            continue
        payload = MessageToDict(part.data, preserving_proto_field_name=True)
        ids.append(str(payload.get("intervention_id", "")))
    return ids


async def wait_for_intervention_status(
    client, task_id: str, intervention_id: str, status: str, timeout_seconds: float = 20.0
):
    """Wait until one intervention reaches *status* in the task snapshot."""
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    task = None
    while asyncio.get_event_loop().time() < deadline:
        task = await client.get_task(GetTaskRequest(id=task_id))
        for item in task_state(task).get("interventions", []):
            if item.get("id") == intervention_id and item.get("status") == status:
                return task
        await asyncio.sleep(0.05)
    raise AssertionError(f"intervention {intervention_id} did not reach {status}")


def answer_message(
    intervention_id: str,
    answer: str | list[str] | bool,
    *,
    task_id: str = "",
    context_id: str = "",
    text: str = "",
) -> SendMessageRequest:
    """Build a standard answer message carrying a question_response data part."""
    data_value = struct_pb2.Value()
    ParseDict({"intervention_id": intervention_id, "answer": answer}, data_value)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": "question_response"})
    part = Part()
    part.data.CopyFrom(data_value)
    part.metadata.CopyFrom(meta)
    parts = [Part(text=text)] if text else []
    parts.append(part)
    return SendMessageRequest(
        message=Message(
            message_id=f"m-answer-{intervention_id}",
            role=Role.ROLE_USER,
            parts=parts,
            task_id=task_id,
            context_id=context_id,
        )
    )

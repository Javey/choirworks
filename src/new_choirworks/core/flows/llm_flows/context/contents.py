from __future__ import annotations

import json

from a2a.types.a2a_pb2 import Message, Role, TaskStatusUpdateEvent
from google.protobuf import json_format
from litellm.types.llms.openai import (
    ChatCompletionAssistantToolCall,
    ChatCompletionToolCallFunctionChunk,
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionUserMessage,
)

from new_choirworks.core.events.event import (
    TASK_RESPONSE_BOUNDARY_STATES,
    A2AEvent,
    extract_text,
    is_final_artifact,
    is_function_call_part,
    is_function_response_part,
    is_thought_part,
    meta,
)
from new_choirworks.core.flows.llm_flows.context.fencing import (
    is_other_agent_reply,
    present_other_agent_message,
)
from new_choirworks.core.models.llm_request import ChatMessage
from new_choirworks.core.models.llm_response import FunctionCall, FunctionResponse


def get_contents(
    events: list[A2AEvent],
    *,
    agent_name: str,
    branch: str | None = None,
    isolation_scope: str | None = None,
    node_path: str | None = None,
) -> list[ChatMessage]:
    if not agent_name:
        raise ValueError("agent_name is required for author-aware fencing")

    messages: list[ChatMessage] = []

    for event in events:
        if not _is_visible(
            event,
            branch=branch,
            isolation_scope=isolation_scope,
            node_path=node_path,
        ):
            continue
        if is_other_agent_reply(agent_name, event):
            fenced = present_other_agent_message(event)
            if fenced is None:
                continue
            event = fenced
        if isinstance(event, Message) and event.role == Role.ROLE_USER:
            text = "".join(p.text for p in event.parts if p.text)
            if text:
                messages.append(
                    OpenAIChatCompletionUserMessage(
                        role="user",
                        content=text,
                    )
                )
        elif isinstance(event, TaskStatusUpdateEvent):
            if event.status.state in TASK_RESPONSE_BOUNDARY_STATES:
                text = extract_text(event)
                if text:
                    messages.append(
                        OpenAIChatCompletionAssistantMessage(
                            role="assistant",
                            content=text,
                        )
                    )
        elif is_final_artifact(event):
            text_buf: list[str] = []
            function_calls: list[FunctionCall] = []
            function_responses: list[FunctionResponse] = []

            for part in event.artifact.parts:
                if part.text and not is_thought_part(part):
                    text_buf.append(part.text)
                elif is_function_call_part(part):
                    fc_data = json_format.MessageToDict(part.data)
                    function_calls.append(FunctionCall(**fc_data))
                elif is_function_response_part(part):
                    fr_data = json_format.MessageToDict(part.data)
                    function_responses.append(FunctionResponse(**fr_data))

            text = "".join(text_buf)
            if text or function_calls:
                messages.append(
                    OpenAIChatCompletionAssistantMessage(
                        role="assistant",
                        content=text or None,
                        tool_calls=[
                            ChatCompletionAssistantToolCall(
                                id=fc.id,
                                type="function",
                                function=ChatCompletionToolCallFunctionChunk(
                                    name=fc.name,
                                    arguments=json.dumps(fc.args),
                                ),
                            )
                            for fc in function_calls
                        ]
                        or None,
                    )
                )
            if function_responses:
                for fr in function_responses:
                    messages.append(
                        ChatCompletionToolMessage(
                            role="tool",
                            content=json.dumps(fr.response),
                            tool_call_id=fr.id,
                        )
                    )

    return messages


def _is_visible(
    event: A2AEvent,
    *,
    branch: str | None,
    isolation_scope: str | None,
    node_path: str | None,
) -> bool:
    if meta(event).isolation_scope != isolation_scope:
        return False

    event_branch = meta(event).branch
    if branch and event_branch:
        if event_branch != branch and not branch.startswith(event_branch + "."):
            return False

    if node_path is not None:
        event_path = meta(event).node_info.path
        if event_path and not (event_path == node_path or event_path.startswith(node_path + "/")):
            return False

    return True

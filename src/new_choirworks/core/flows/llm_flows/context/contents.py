from __future__ import annotations

import json

from a2a.types.a2a_pb2 import Message, Role
from google.protobuf import json_format
from litellm.types.llms.openai import (
    ChatCompletionAssistantToolCall,
    ChatCompletionToolCallFunctionChunk,
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionUserMessage,
)

from new_choirworks.core.events.event import (
    A2AEvent,
    is_final_artifact,
    is_function_call_part,
    is_function_response_part,
    is_thought_part,
)
from new_choirworks.core.flows.llm_flows.context.fencing import (
    is_other_agent_reply,
    present_other_agent_message,
)
from new_choirworks.core.models.llm_request import ChatMessage
from new_choirworks.core.models.llm_response import FunctionCall, FunctionResponse


def get_contents(events: list[A2AEvent], *, agent_name: str) -> list[ChatMessage]:
    if not agent_name:
        raise ValueError("agent_name is required for author-aware fencing")

    messages: list[ChatMessage] = []

    # TODO: 尚未处理 own TaskStatusUpdateEvent.status.message；当前只转换
    # user Message 和 final artifact。
    for event in events:
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

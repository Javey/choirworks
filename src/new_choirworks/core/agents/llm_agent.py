import json
import uuid
from collections.abc import AsyncGenerator
from typing import override

from litellm.types.llms.openai import (
    ChatCompletionAssistantToolCall,
    ChatCompletionToolCallFunctionChunk,
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
)

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent, to_a2a_artifact
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.llm_request import ChatMessage, LLMRequest
from new_choirworks.core.models.llm_response import (
    FunctionCall,
    LLMResponse,
    LLMResponseContent,
    LLMResponsePart,
)
from new_choirworks.core.tools.base_tool import BaseTool

from .base_agent import BaseAgent


class LLMAgent(BaseAgent):
    """An agent that uses a large language model (LLM) to generate responses."""

    llm: BaseLLM
    instruction: str = ""
    tools: list[BaseTool] = []

    @override
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        if not ctx.task_id:
            ctx.task_id = str(uuid.uuid4())
        if not ctx.context_id:
            ctx.context_id = str(uuid.uuid4())

        tool_params = [t._get_declaration() for t in self.tools]
        tools_dict = {t.name: t for t in self.tools}

        while True:
            req = LLMRequest(
                messages=ctx.messages,
                tools=tool_params or None,
                system_instruction=self.instruction or None,
            )

            artifact_id = uuid.uuid4().hex
            thought_buf: list[str] = []
            text_buf: list[str] = []
            function_calls: list[FunctionCall] = []

            async for response in self.llm.generate_content_async(req, stream=True):
                if not response.content:
                    continue

                yield to_a2a_artifact(
                    ctx,
                    response,
                    artifact_id=artifact_id,
                    append=bool(thought_buf) or bool(text_buf),
                    last_chunk=False,
                )

                for part in response.content.parts:
                    if part.text is not None:
                        if part.thought:
                            thought_buf.append(part.text)
                        else:
                            text_buf.append(part.text)
                    if part.function_call is not None:
                        function_calls.append(part.function_call)

            final_response = self._close(thought_buf, text_buf, function_calls)
            if final_response:
                yield to_a2a_artifact(
                    ctx,
                    final_response,
                    artifact_id=artifact_id,
                    append=False,
                    last_chunk=True,
                )

            if not function_calls:
                break

            await self._execute_tools(function_calls, tools_dict, ctx.messages, text_buf)

    async def _execute_tools(
        self,
        function_calls: list[FunctionCall],
        tools_dict: dict[str, BaseTool],
        messages: list[ChatMessage],
        text_buf: list[str],
    ) -> None:
        """Execute tools and append assistant message + tool results to messages."""
        messages.append(
            OpenAIChatCompletionAssistantMessage(
                role="assistant",
                content="".join(text_buf) or None,
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
                ],
            )
        )

        for fc in function_calls:
            tool = tools_dict[fc.name]
            result = await tool.run_async(fc.args or {})
            normalized = result if isinstance(result, dict) else {"result": result}
            messages.append(
                ChatCompletionToolMessage(
                    role="tool",
                    content=json.dumps(normalized),
                    tool_call_id=fc.id,
                )
            )

    def _close(
        self,
        thought_buf: list[str],
        text_buf: list[str],
        function_calls: list[FunctionCall],
    ) -> LLMResponse | None:
        """Build the final aggregated response from accumulated buffers."""
        final_parts: list[LLMResponsePart] = []
        if thought_buf:
            final_parts.append(LLMResponsePart(text="".join(thought_buf), thought=True))
        if text_buf:
            final_parts.append(LLMResponsePart(text="".join(text_buf), thought=False))
        for fc in function_calls:
            final_parts.append(LLMResponsePart(function_call=fc))
            
        if not final_parts:
            return None
        return LLMResponse(content=LLMResponseContent(parts=final_parts))

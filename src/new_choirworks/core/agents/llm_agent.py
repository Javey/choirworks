import json
import uuid
from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import Literal, cast, override

from pydantic import BaseModel

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import (
    ARTIFACT_METADATA_STRUCTURED_OUTPUT,
    A2AEvent,
    events_to_llm_messages,
    to_a2a_artifact,
)
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import (
    FunctionCall,
    LLMResponse,
    LLMResponseContent,
    LLMResponsePart,
)
from new_choirworks.core.tools.base_tool import BaseTool
from new_choirworks.core.utils.schema_utils import validate_schema
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.llm_agent_wrapper import run_llm_agent_as_node

from .base_agent import BaseAgent


class LLMAgent(BaseAgent):
    """An agent that uses a large language model (LLM) to generate responses."""

    llm: BaseLLM
    instruction: str = ""
    tools: list[BaseTool] = []
    max_retries: int = 3
    output_schema: type[BaseModel] | None = None
    include_contents: Literal["default", "none"] = "default"

    @override
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        if not ctx.task_id:
            ctx.task_id = str(uuid.uuid4())
        if not ctx.context_id:
            ctx.context_id = str(uuid.uuid4())

        tool_params = [t.get_declaration() for t in self.tools]
        tools_dict = {t.name: t for t in self.tools}

        retry_count = 0

        while True:
            req = LLMRequest(
                messages=events_to_llm_messages(ctx.events, agent_name=self.name),
                tools=tool_params or None,
                system_instruction=self.instruction or None,
                response_schema=self.output_schema,
            )

            artifact_id = uuid.uuid4().hex
            thought_buf: list[str] = []
            text_buf: list[str] = []
            function_calls: list[FunctionCall] = []
            error_message = ""

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

                if response.error_code:
                    error_message = response.error_message or ""

            final_response = self.close(thought_buf, text_buf, function_calls)
            # TODO: 结构化输出adk同构
            structured: str | None = None
            if not function_calls and self.output_schema is not None:
                structured = json.dumps(validate_schema(self.output_schema, "".join(text_buf)))
            if final_response:
                event = to_a2a_artifact(
                    ctx,
                    final_response,
                    artifact_id=artifact_id,
                    append=False,
                    last_chunk=True,
                )
                if structured is not None:
                    event.metadata[ARTIFACT_METADATA_STRUCTURED_OUTPUT] = structured
                yield event

            if not function_calls:
                break

            if error_message:
                retry_count += 1
                for fc in function_calls:
                    yield to_a2a_artifact(
                        ctx,
                        LLMResponse.from_function_response(fc, {"error": error_message}),
                        artifact_id=uuid.uuid4().hex,
                        append=False,
                        last_chunk=True,
                    )
                if retry_count >= self.max_retries:
                    break
                continue

            async for event in self._execute_tools(function_calls, tools_dict, ctx):
                yield event

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        """Runs the agent as a node in a workflow graph."""
        async with aclosing(run_llm_agent_as_node(self, ctx=ctx, node_input=node_input)) as agen:
            async for event in agen:
                yield event

    async def _execute_tools(
        self,
        function_calls: list[FunctionCall],
        tools_dict: dict[str, BaseTool],
        ctx: InvocationContext,
    ) -> AsyncGenerator[A2AEvent, None]:
        """Execute tools and yield function_response events."""
        for fc in function_calls:
            tool = tools_dict[fc.name]
            result = await tool.run_async(fc.args or {})
            normalized = (
                cast(dict[str, object], result) if isinstance(result, dict) else {"result": result}
            )

            yield to_a2a_artifact(
                ctx,
                LLMResponse.from_function_response(fc, normalized),
                artifact_id=uuid.uuid4().hex,
                append=False,
                last_chunk=True,
            )

    def close(
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

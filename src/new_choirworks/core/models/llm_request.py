from typing import ClassVar

from litellm.types.llms.openai import (
    ChatCompletionToolMessage,
    ChatCompletionToolParam,
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionSystemMessage,
    OpenAIChatCompletionUserMessage,
)
from pydantic import BaseModel, ConfigDict, Field

type ChatMessage = (
    ChatCompletionToolMessage
    | OpenAIChatCompletionAssistantMessage
    | OpenAIChatCompletionSystemMessage
    | OpenAIChatCompletionUserMessage
)


class LLMRequest(BaseModel):
    """A request to an LLM, including the model, messages, and any additional parameters."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    model: str | None = None
    messages: list[ChatMessage] = Field(
        description="The messages to pass to the LLM.", default_factory=list
    )

    tools: list[ChatCompletionToolParam] | None = Field(default_factory=list)

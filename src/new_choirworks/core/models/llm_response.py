from litellm.types.utils import Delta
from pydantic import BaseModel, ConfigDict, alias_generator


class LLMResponse(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        alias_generator=alias_generator.to_camel,
        populate_by_name=True,
    )

    content: Delta | None = None

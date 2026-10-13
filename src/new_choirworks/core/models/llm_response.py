from typing import ClassVar

from litellm.types.utils import Delta
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class LLMResponse(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
    )

    content: Delta | None = None

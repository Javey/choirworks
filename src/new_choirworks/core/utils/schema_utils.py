import re

from pydantic import BaseModel

_FENCE_LANG_RE = re.compile(r"^[A-Za-z0-9_]*[ \t]*\n?")


def _strip_json_code_fence(json_text: str) -> str:
    """Removes a markdown code fence wrapping the entire JSON payload, if present.

    A model asked for structured output occasionally wraps it in a
    ```json ... ``` fence, most often when tools are configured alongside an
    output schema and the schema constraint becomes best-effort. Well-formed JSON
    never starts with a fence, so this is a no-op on valid input.

    Design reference: google-adk utils/_schema_utils.py _strip_json_code_fence
    (Apache-2.0, Copyright 2026 Google LLC).
    """
    stripped = json_text.strip()
    if len(stripped) >= 6 and stripped.startswith("```") and stripped.endswith("```"):
        inner = stripped[3:-3]
        inner = _FENCE_LANG_RE.sub("", inner, count=1)
        return inner.strip()
    return json_text


def validate_schema(schema: type[BaseModel], json_text: str) -> dict[str, object]:
    """Validate JSON text against a Pydantic model schema and return the result.

    Design reference: google-adk utils/_schema_utils.py validate_schema
    (Apache-2.0, Copyright 2026 Google LLC).
    """
    json_text = _strip_json_code_fence(json_text)
    return schema.model_validate_json(json_text).model_dump(exclude_none=True)

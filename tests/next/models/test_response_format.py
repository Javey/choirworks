from typing import cast

from pydantic import BaseModel

from new_choirworks.core.models.lite_llm import (
    _enforce_strict_openai_schema,
    _to_response_format,
)


class _Address(BaseModel):
    city: str
    street: str


class _Person(BaseModel):
    name: str
    age: int
    address: _Address
    tags: list[str]


def test_to_response_format_basic():
    fmt = _to_response_format(_Person)
    assert fmt["type"] == "json_schema"
    inner = cast(dict[str, object], fmt["json_schema"])
    assert inner["name"] == "_Person"
    assert inner["strict"] is True


def test_to_response_format_has_additional_properties_false():
    fmt = _to_response_format(_Person)
    inner = cast(dict[str, object], fmt["json_schema"])
    schema = cast(dict[str, object], inner["schema"])
    assert schema["additionalProperties"] is False


def test_to_response_format_all_required():
    fmt = _to_response_format(_Person)
    inner = cast(dict[str, object], fmt["json_schema"])
    schema = cast(dict[str, object], inner["schema"])
    required = cast(list[str], schema["required"])
    assert set(required) == {"name", "age", "address", "tags"}


def test_to_response_format_nested_defs_strict():
    fmt = _to_response_format(_Person)
    inner = cast(dict[str, object], fmt["json_schema"])
    schema = cast(dict[str, object], inner["schema"])
    defs = cast(dict[str, object], schema["$defs"])
    assert "_Address" in defs
    addr_schema = cast(dict[str, object], defs["_Address"])
    assert addr_schema["additionalProperties"] is False


def test_enforce_strict_strips_ref_siblings():
    schema: dict[str, object] = {"$ref": "#/$defs/Foo", "description": "bar"}
    _enforce_strict_openai_schema(schema)
    assert schema == {"$ref": "#/$defs/Foo"}


def test_enforce_strict_array_items():
    schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "tags": {"type": "array", "items": {"type": "string"}},
        },
    }
    _enforce_strict_openai_schema(schema)
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["tags"]

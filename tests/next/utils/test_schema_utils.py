from pydantic import BaseModel

from new_choirworks.core.utils.schema_utils import (
    _strip_json_code_fence,
    validate_schema,
)


class _Person(BaseModel):
    name: str
    age: int


def test_validate_schema_basic():
    json_text = '{"name": "Alice", "age": 30}'
    result = validate_schema(_Person, json_text)
    assert result == {"name": "Alice", "age": 30}


def test_validate_schema_strips_code_fence():
    json_text = '```json\n{"name": "Bob", "age": 25}\n```'
    result = validate_schema(_Person, json_text)
    assert result == {"name": "Bob", "age": 25}


def test_validate_schema_strips_code_fence_no_lang():
    json_text = '```\n{"name": "Bob", "age": 25}\n```'
    result = validate_schema(_Person, json_text)
    assert result == {"name": "Bob", "age": 25}


def test_validate_schema_exclude_none():
    class _Optional(BaseModel):
        name: str
        nick: str | None = None

    result = validate_schema(_Optional, '{"name": "Alice"}')
    assert result == {"name": "Alice"}


def test_strip_json_code_fence_no_fence():
    assert _strip_json_code_fence('{"a": 1}') == '{"a": 1}'


def test_strip_json_code_fence_plain():
    assert _strip_json_code_fence("```\nhello\n```") == "hello"


def test_strip_json_code_fence_short():
    assert _strip_json_code_fence("``") == "``"

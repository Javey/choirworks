from __future__ import annotations

import pytest
from a2a.types.a2a_pb2 import Message, Part, Role
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.a2a.wire import parse_question_response


def _answer_part(data: dict[str, object]) -> Part:
    value = struct_pb2.Value()
    ParseDict(data, value)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": "question_response"})
    part = Part()
    part.data.CopyFrom(value)
    part.metadata.CopyFrom(meta)
    return part


def _message(*parts: Part) -> Message:
    return Message(role=Role.ROLE_USER, parts=list(parts))


def test_parse_question_response_ignores_plain_text():
    assert parse_question_response(_message(Part(text="hello"))) == []


def test_parse_question_response_parses_answers():
    responses = parse_question_response(
        _message(
            Part(text="方案二"),
            _answer_part({"intervention_id": "iv1", "answer": "方案二"}),
        )
    )

    assert len(responses) == 1
    assert responses[0].intervention_id == "iv1"
    assert responses[0].answer == "方案二"


def test_parse_question_response_accepts_list_and_bool():
    responses = parse_question_response(
        _message(
            _answer_part({"intervention_id": "a", "answer": ["x", "y"]}),
            _answer_part({"intervention_id": "b", "answer": True}),
        )
    )

    assert [response.answer for response in responses] == [["x", "y"], True]


def test_parse_question_response_rejects_malformed():
    with pytest.raises(ValueError):
        parse_question_response(_message(_answer_part({"answer": "missing id"})))

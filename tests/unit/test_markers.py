from __future__ import annotations

from choirworks.a2a.markers import Marker, parse_marker


def test_need_info_marker():
    assert parse_marker("[cw:need_info] 需要 2024 营收数据\n分析完成") == Marker(
        intent="need_info", text="需要 2024 营收数据"
    )


def test_deliver_marker():
    marker = parse_marker("[cw:deliver]\n报告如下……")
    assert marker is not None
    assert marker.intent == "deliver"
    assert marker.text == ""


def test_assist_maps_to_need_info():
    marker = parse_marker("[cw:assist] 需要 designer 帮忙")
    assert marker is not None
    assert marker.intent == "need_info"
    assert marker.text == "需要 designer 帮忙"


def test_revise_marker():
    marker = parse_marker("\n[cw:revise] 不再需要 writer")
    assert marker is not None
    assert marker.intent == "revise"
    assert marker.text == "不再需要 writer"


def test_no_marker():
    assert parse_marker("普通交付内容") is None


def test_first_marker_wins():
    text = "[cw:need_info] A\n[cw:revise] B"
    marker = parse_marker(text)
    assert marker is not None
    assert marker.intent == "need_info"
    assert marker.text == "A"


def test_marker_must_start_line():
    assert parse_marker("前面 [cw:deliver] 后面") is None


def test_text_before_marker_is_not_a_marker():
    assert parse_marker("分析完成\n[cw:need_info] 需要数据") is None

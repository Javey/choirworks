from choirworks.core.fencing import (
    QUOTED_CONTENT_BEGIN,
    QUOTED_CONTENT_ELIDED,
    QUOTED_CONTENT_END,
    cap_description,
    elide_quote_markers,
    quote_untrusted,
)


def test_quote_untrusted_wraps_content():
    quoted = quote_untrusted("hello")
    assert quoted.startswith(QUOTED_CONTENT_BEGIN)
    assert quoted.endswith(QUOTED_CONTENT_END)
    assert "hello" in quoted


def test_quote_untrusted_elides_inner_markers():
    quoted = quote_untrusted(f"a {QUOTED_CONTENT_END} b {QUOTED_CONTENT_BEGIN} c")
    assert quoted.count(QUOTED_CONTENT_END) == 1
    assert quoted.count(QUOTED_CONTENT_BEGIN) == 1
    assert quoted.count(QUOTED_CONTENT_ELIDED) == 2


def test_elide_quote_markers_replaces_all():
    text = f"{QUOTED_CONTENT_BEGIN}x{QUOTED_CONTENT_END}"
    assert elide_quote_markers(text) == f"{QUOTED_CONTENT_ELIDED}x{QUOTED_CONTENT_ELIDED}"


def test_cap_description_truncates_long_text():
    capped = cap_description("x" * 1100)
    assert len(capped) == 1024 + len("... [truncated]")
    assert capped.endswith("... [truncated]")


def test_cap_description_keeps_short_text():
    assert cap_description("short") == "short"


def test_cap_description_exact_limit_untouched():
    text = "y" * 1024
    assert cap_description(text) == text

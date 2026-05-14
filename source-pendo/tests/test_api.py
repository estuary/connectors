from source_pendo.api import _escape_filter_string


def test_escape_filter_string_simple_value_is_just_quoted():
    assert _escape_filter_string("acct-123") == '"acct-123"'


def test_escape_filter_string_escapes_embedded_double_quotes():
    # accountIds from XSS-scan payloads contain literal " characters that would
    # otherwise terminate the filter expression's string literal and 400 the request.
    xss_id = 'javascript:xssdetected(1)//*/"/*onmouseover=...'
    assert _escape_filter_string(xss_id) == r'"javascript:xssdetected(1)//*/\"/*onmouseover=..."'


def test_escape_filter_string_escapes_backslashes_before_quotes():
    # A literal \" in the data must become \\\" so Pendo's parser reads it as
    # an escaped backslash followed by an escaped quote, not a closing delimiter.
    assert _escape_filter_string('a\\"b') == r'"a\\\"b"'

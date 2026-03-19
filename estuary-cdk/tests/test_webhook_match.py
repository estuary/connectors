import pytest
from aiohttp.test_utils import make_mocked_request
from pydantic import ValidationError

from estuary_cdk.capture.webhook.match import (
    CATCH_ALL_DISCRIMINATOR,
    BodyDiscriminator,
    BodyMatch,
    HeaderDiscriminator,
    HeaderMatch,
    UrlDiscriminator,
    UrlMatch,
)
from estuary_cdk.pydantic_polyfill import JsonValue


def _make_request(
    path: str = "/",
    headers: dict[str, str] | None = None,
    body: dict[str, JsonValue] | None = None,
):
    req = make_mocked_request("POST", path, headers=headers or {})
    if body is not None:
        req["_parsed_body"] = body
    return req


class TestUrlMatch:
    @pytest.mark.parametrize("value", ["/foo", "/foo/bar", "/foo/{x}/bar", "*"])
    def test_valid_patterns_can_instantiate(self, value: str):
        m = UrlMatch(value=value)
        assert m.value == value

    def test_invalid_pattern_raises(self):
        with pytest.raises(ValidationError):
            _ = UrlMatch(value="no-leading-slash")

    @pytest.mark.parametrize(
        "segment, expected",
        [
            ("{x}", True),
            ("foo", False),
            ("{}", True),
            ("{", False),
            ("}", False),
            ("", False),
        ],
    )
    def test_is_placeholder_segment(self, segment: str, expected: str):
        assert (
            UrlMatch._is_placeholder_segment(  # pyright: ignore[reportPrivateUsage]
                segment
            )
            == expected
        )

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("*", (0, -1)),
            ("/foo/bar/baz", (0, 3)),
            ("/foo/{x}/baz", (0, 2)),
            ("/{x}/{y}/{z}", (0, 0)),
            ("/a", (0, 1)),
        ],
    )
    def test_sort_key(self, value: str, expected: str):
        assert UrlMatch(value=value).sort_key == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "value, path, expected",
        [
            ("*", "/anything", True),
            ("/foo", "/foo", True),
            ("/foo", "/bar", False),
            ("/foo/{x}/baz", "/foo/hello/baz", True),
            ("/foo/{x}/baz", "/foo/hello/qux", False),
            ("/foo/{x}/baz", "/foo/hello", False),
        ],
    )
    async def test_matches(self, value: str, path: str, expected: str):
        req = _make_request(path=path)
        assert await UrlMatch(value=value).matches(req) == expected

    @pytest.mark.parametrize(
        "a, b, has_error",
        [
            # Duplicate wildcard
            ("*", "*", True),
            # Wildcard + concrete is fine
            ("*", "/foo", False),
            # Duplicate path
            ("/foo", "/foo", True),
            # Different concrete paths
            ("/foo", "/bar", False),
            # Different segment counts
            ("/foo/bar", "/foo/bar/baz", False),
            # Ambiguous same specificity
            ("/foo/{x}/baz", "/foo/bar/{y}", True),
            # One is strictly more specific — allowed
            ("/foo/{x}/baz", "/foo/{x}/{y}", False),
            # All placeholders, same structure
            ("/{x}/{y}", "/{a}/{b}", True),
        ],
    )
    def test_list_compatibility_errors(self, a: str, b: str, has_error: str):
        error = UrlMatch(value=a).list_compatibility_errors(UrlMatch(value=b))
        assert (error is not None) == has_error

    def test_compatibility_different_type_returns_none(self):
        assert (
            UrlMatch(value="/foo").list_compatibility_errors(
                HeaderMatch(key="k", value="v")
            )
            is None
        )


class TestHeaderMatch:
    def test_sort_key(self):
        assert HeaderMatch(key="X-Event", value="push").sort_key == (2, 0)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "key, value, headers, expected",
        [
            ("X-Event", "push", {"X-Event": "push"}, True),
            ("X-Event", "push", {"X-Event": "pull"}, False),
            ("X-Event", "push", {}, False),
        ],
    )
    async def test_matches(
        self, key: str, value: str, headers: dict[str, str], expected: bool
    ):
        req = _make_request(headers=headers)
        assert await HeaderMatch(key=key, value=value).matches(req) == expected

    @pytest.mark.parametrize(
        "a_key, a_val, b_key, b_val, has_error",
        [
            ("X-Event", "push", "X-Event", "push", True),
            ("X-Event", "push", "X-Event", "pull", False),
            ("X-Event", "push", "X-Other", "push", False),
        ],
    )
    def test_list_compatibility_errors(
        self, a_key: str, a_val: str, b_key: str, b_val: str, has_error: bool
    ):
        a = HeaderMatch(key=a_key, value=a_val)
        b = HeaderMatch(key=b_key, value=b_val)
        assert (a.list_compatibility_errors(b) is not None) == has_error

    def test_compatibility_different_type_returns_none(self):
        assert (
            HeaderMatch(key="k", value="v").list_compatibility_errors(
                BodyMatch(key="event", value="push")
            )
            is None
        )


class TestBodyMatch:
    @pytest.mark.parametrize("dot_path", ["event", "event.type", "a.b.c", "_private"])
    def test_valid_json_paths_can_instantiate(self, dot_path: str):
        m = BodyMatch(key=dot_path, value="x")
        assert m.key == dot_path

    @pytest.mark.parametrize(
        "dot_path",
        [
            "",
            "123start",
            "invalid..path",
            ".leading",
            "trailing.",
            "has-hyphen",
            "has space",
        ],
    )
    def test_invalid_keys_raise(self, dot_path: str):
        with pytest.raises(ValidationError):
            _ = BodyMatch(key=dot_path, value="x")

    def test_sort_key(self):
        assert BodyMatch(key="event", value="push").sort_key == (1, 0)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "dot_path, value, body, expected",
        [
            # Top-level match
            ("event", "push", {"event": "push"}, True),
            # Wrong value
            ("event", "push", {"event": "pull"}, False),
            # Missing key
            ("event", "push", {}, False),
            # Nested dot-path
            ("event.type", "push", {"event": {"type": "push"}}, True),
            ("event.type", "push", {"event": {"type": "pull"}}, False),
            # Deeply nested
            ("a.b.c", "val", {"a": {"b": {"c": "val"}}}, True),
            # Intermediate key missing
            ("a.b.c", "val", {"a": {"x": 1}}, False),
            # Intermediate key is not a dict
            ("a.b", "val", {"a": "scalar"}, False),
            # Numeric value coerced via str()
            ("count", "42", {"count": 42}, True),
        ],
    )
    async def test_matches(
        self,
        dot_path: str,
        value: str,
        body: dict[str, JsonValue],
        expected: dict[str, JsonValue],
    ):
        req = _make_request(body=body)
        assert await BodyMatch(key=dot_path, value=value).matches(req) == expected

    @pytest.mark.asyncio
    async def test_matches_caches_parsed_body(self):
        req = _make_request(body={"event": "push"})
        m = BodyMatch(key="event", value="push")
        _ = await m.matches(req)
        assert "_parsed_body" in req

    @pytest.mark.parametrize(
        "a_key, a_val, b_key, b_val, has_error",
        [
            ("event", "push", "event", "push", True),
            ("event", "push", "event", "pull", False),
            ("event", "push", "other", "push", False),
        ],
    )
    def test_list_compatibility_errors(
        self, a_key: str, a_val: str, b_key: str, b_val: str, has_error: bool
    ):
        a = BodyMatch(key=a_key, value=a_val)
        b = BodyMatch(key=b_key, value=b_val)
        assert (a.list_compatibility_errors(b) is not None) == has_error


class TestDiscriminators:
    def test_url_discriminator_for_value(self):
        m = UrlDiscriminator().for_value("/test")
        assert isinstance(m, UrlMatch)
        assert m.value == "/test"

    def test_url_discriminator_create_match_rules(self):
        d = UrlDiscriminator(known_values={"/foo", "/bar"})
        rules = d.create_match_rules()
        assert len(rules) == 2
        assert all(isinstance(r, UrlMatch) for r in rules)
        assert {r.value for r in rules} == {"/foo", "/bar"}

    def test_url_discriminator_validates_paths(self):
        with pytest.raises(ValidationError):
            _ = UrlDiscriminator(known_values={"no-leading-slash"})

    def test_url_discriminator_allows_wildcard(self):
        d = UrlDiscriminator(known_values={"*"})
        assert "*" in d.known_values

    def test_url_discriminator_empty_known_values_returns_catch_all(self):
        rules = UrlDiscriminator().create_match_rules()
        assert len(rules) == 1
        assert isinstance(rules[0], UrlMatch)
        assert rules[0].value == "*"

    def test_header_discriminator_for_value(self):
        m = HeaderDiscriminator(key="X-Event", known_values={"push"}).for_value("push")
        assert isinstance(m, HeaderMatch)
        assert m.key == "X-Event"
        assert m.value == "push"

    def test_header_discriminator_create_match_rules(self):
        d = HeaderDiscriminator(key="X-Event", known_values={"push", "pull"})
        rules = d.create_match_rules()
        assert len(rules) == 2
        for r in rules:
            assert isinstance(r, HeaderMatch)
            assert r.key == "X-Event"

    def test_header_discriminator_rejects_empty_known_values(self):
        with pytest.raises(ValidationError):
            _ = HeaderDiscriminator(key="X-Event", known_values=set())

    def test_body_discriminator_for_value(self):
        m = BodyDiscriminator(key="event.type", known_values={"push"}).for_value("push")
        assert isinstance(m, BodyMatch)
        assert m.key == "event.type"
        assert m.value == "push"

    def test_body_discriminator_create_match_rules(self):
        d = BodyDiscriminator(key="event.type", known_values={"push", "pull"})
        rules = d.create_match_rules()
        assert len(rules) == 2
        assert all(isinstance(r, BodyMatch) for r in rules)

    def test_body_discriminator_rejects_empty_known_values(self):
        with pytest.raises(ValidationError):
            _ = BodyDiscriminator(key="event", known_values=set())

    def test_catch_all_discriminator(self):
        assert isinstance(CATCH_ALL_DISCRIMINATOR, UrlMatch)
        assert CATCH_ALL_DISCRIMINATOR.value == "*"

    @pytest.mark.asyncio
    async def test_catch_all_matches_any_request(self):
        req = _make_request(path="/anything/at/all")
        assert await CATCH_ALL_DISCRIMINATOR.matches(req) is True

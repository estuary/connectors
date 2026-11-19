import re

import pytest
import xmltodict

from source_sage_intacct.models import EndpointConfig
from source_sage_intacct.sage import (
    SHOW_PRIVATE_OBJECTS,
    api_session_request,
    get_all_records_from_date_request,
    get_all_records_request,
    get_creations_at_request,
    get_creations_since_request,
    get_deletions_at_request,
    get_deletions_since_request,
    get_gldetail_batch_request,
    get_gldetail_page_request,
    get_records_at_request,
    get_records_since_request,
    get_user_by_id_request,
    object_definition_request,
    user_datetime_prefs_request,
)

cfg = EndpointConfig(
    sender_id="sender_id",
    sender_password="sender_password",
    company_id="company_id",
    user_id="user_id",
    password="password",
)

session_id = "some_session_id"
object_name = "OBJECT_NAME"
fields = ["FIRST_FIELD", "SECOND_FIELD", "THIRD_FIELD"]
ts = "2023-01-02 03:04:56"


@pytest.mark.parametrize(
    "name,func,args",
    [
        ("api_session_request.txt", api_session_request, (cfg,)),
        (
            "user_datetime_prefs_request.txt",
            user_datetime_prefs_request,
            (cfg, session_id, 1234),
        ),
        (
            "get_user_by_id_request.txt",
            get_user_by_id_request,
            (cfg, session_id, "user_id"),
        ),
        (
            "object_definition_request.txt",
            object_definition_request,
            (cfg, session_id, object_name),
        ),
        (
            "get_records_since_request.txt",
            get_records_since_request,
            (cfg, session_id, object_name, fields, ts),
        ),
        (
            "get_records_at_request.txt",
            get_records_at_request,
            (cfg, session_id, object_name, fields, ts, 1234),
        ),
        (
            "get_records_at_request.txt",
            get_records_at_request,
            (cfg, session_id, object_name, fields, ts, None),
        ),
        (
            "get_all_records_request.txt",
            get_all_records_request,
            (cfg, session_id, object_name, fields, 1234),
        ),
        (
            "get_all_records_request.txt",
            get_all_records_request,
            (cfg, session_id, object_name, fields, None),
        ),
        (
            "get_deletions_since_request.txt",
            get_deletions_since_request,
            (cfg, session_id, object_name, ts),
        ),
        (
            "get_deletions_at_request.txt",
            get_deletions_at_request,
            (cfg, session_id, object_name, ts, "123:123456"),
        ),
        (
            "get_deletions_at_request.txt",
            get_deletions_at_request,
            (cfg, session_id, object_name, ts, None),
        ),
        (
            "get_creations_since_request.txt",
            get_creations_since_request,
            (cfg, session_id, object_name, fields, ts),
        ),
        (
            "get_creations_at_request.txt",
            get_creations_at_request,
            (cfg, session_id, object_name, fields, ts, 1234),
        ),
        (
            "get_creations_at_request.txt",
            get_creations_at_request,
            (cfg, session_id, object_name, fields, ts, None),
        ),
        # General Ledger objects set `showprivate` on each query.
        (
            "get_records_since_request.txt",
            get_records_since_request,
            (cfg, session_id, "GLBATCH", fields, ts),
        ),
        (
            "get_all_records_from_date_request.txt",
            get_all_records_from_date_request,
            (cfg, session_id, "GLENTRY", fields, "ENTRY_DATE", "01/01/1900", 1234),
        ),
        (
            "get_all_records_from_date_request.txt",
            get_all_records_from_date_request,
            (cfg, session_id, "GLENTRY", fields, "ENTRY_DATE", "01/01/1900", None),
        ),
        (
            "get_gldetail_page_request.txt",
            get_gldetail_page_request,
            (cfg, session_id, fields, "01/01/1900", None),
        ),
        (
            "get_gldetail_page_request.txt",
            get_gldetail_page_request,
            (cfg, session_id, fields, "01/01/1900", 5501),
        ),
        (
            "get_gldetail_batch_request.txt",
            get_gldetail_batch_request,
            (cfg, session_id, fields, "01/01/1900", 5501, None),
        ),
        (
            "get_gldetail_batch_request.txt",
            get_gldetail_batch_request,
            (cfg, session_id, fields, "01/01/1900", 5501, "5501-90001-<77>--A"),
        ),
    ],
)
def test_xml_requests(snapshot, name, func, args):
    got = func(*args)
    assert snapshot(name) == sanitize_xml(got)


def sanitize_xml(xml_string: str) -> str:
    xml_string = re.sub(
        r"<controlid>\d+</controlid>",
        "<controlid>CONTROLID_PLACEHOLDER</controlid>",
        xml_string,
    )
    xml_string = re.sub(r'controlid=".+?"', 'controlid="UUID_PLACEHOLDER"', xml_string)
    return xml_string


GL_REQUESTS = [
    (get_records_since_request, (fields, ts)),
    (get_records_at_request, (fields, ts, 1234)),
    (get_records_at_request, (fields, ts, None)),
    (get_creations_since_request, (fields, ts)),
    (get_creations_at_request, (fields, ts, 1234)),
    (get_creations_at_request, (fields, ts, None)),
    (get_all_records_from_date_request, (fields, "BATCH_DATE", "01/01/1900", 1234)),
    (get_all_records_from_date_request, (fields, "BATCH_DATE", "01/01/1900", None)),
]


@pytest.mark.parametrize("object", sorted(SHOW_PRIVATE_OBJECTS))
@pytest.mark.parametrize("func,args", GL_REQUESTS)
def test_gl_requests_are_filtered_and_show_private(object, func, args):
    got = func(cfg, session_id, object, *args)

    assert "<showprivate>true</showprivate>" in got
    assert got.index("<showprivate>") < got.index("<returnformat>")
    assert_one_condition_under_filter(got)


@pytest.mark.parametrize("func,args", GL_REQUESTS[:6])
def test_other_requests_do_not_show_private(func, args):
    assert "showprivate" not in func(cfg, session_id, object_name, *args)


def assert_one_condition_under_filter(xml_string: str):
    """Sage allows one element directly under <filter>."""
    query = xmltodict.parse(xml_string)["request"]["operation"]["content"]["function"][
        "query"
    ]
    filter = query["filter"]
    assert len(filter) == 1, filter
    ((operator, body),) = filter.items()
    assert not isinstance(body, list), f"{operator} repeats under <filter>"


@pytest.mark.parametrize(
    "func,args",
    [
        (get_gldetail_page_request, ("01/01/1900", None)),
        (get_gldetail_page_request, ("01/01/1900", 5501)),
        (get_gldetail_batch_request, ("01/01/1900", 5501, None)),
        (get_gldetail_batch_request, ("01/01/1900", 5501, "5501-90001-<77>--A")),
    ],
)
def test_gldetail_backfill_requests_are_filtered_and_show_private(func, args):
    got = func(cfg, session_id, fields, *args)

    assert "<showprivate>true</showprivate>" in got
    assert "<pagesize>2000</pagesize>" in got
    assert_one_condition_under_filter(got)

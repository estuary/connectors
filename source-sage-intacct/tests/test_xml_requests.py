import re

import pytest

from source_sage_intacct.models import EndpointConfig
from source_sage_intacct.sage import *

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

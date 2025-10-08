import json
import subprocess

COMMON_FIELDS_TO_REDACT = [
    'created_at',
    'updated_at',
    'id',
    'global_id',
    'graphql_id',
]

TRANSACTION_FIELDS_TO_REDACT = [
    'authorization_expires_at',
    'disbursement_details',
    'network_transaction_id',
    'payment_receipt',
    'processor_authorization_code',
    'settlement_batch_id',
    'status_history',
    'subscription',
    'subscription_details',
]


SUBSCRIPTION_FIELDS_TO_REDACT = [
    'billing_period_end_date',
    'billing_period_start_date',
    'next_billing_date',
    'paid_through_date',
]

def redact_nested_fields(input: list | dict):
    if isinstance(input, list):
        for index, element in enumerate(input):
            if isinstance(element, list) or isinstance(element, dict):
                redact_nested_fields(element)
            else:
                input[index] = 'redacted'
    elif isinstance(input, dict):
        for key in list(input.keys()):
            if isinstance(input[key], list) or isinstance(input[key], dict):
                redact_nested_fields(input[key])
            else:
                input[key] = 'redacted'


def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "preview",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "25s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    # NOTE: Commenting out the below redactions are temporary while I work on improving
    # how the connector parses XML and transforms XML into Pydantic documents.
    #
    # for l in unique_stream_lines:
    #     stream, rec = l[0], l[1]

    #     for field in COMMON_FIELDS_TO_REDACT:
    #         if field in rec:
    #             rec[field] = 'redacted'

    #     if 'evidence' in rec:
    #         evidence: list = rec['evidence']
    #         for e in evidence:
    #             if 'url' in e:
    #                 e['url'] = 'redacted'
    #     if stream == 'acmeCo/subscriptions':
    #         for field in SUBSCRIPTION_FIELDS_TO_REDACT:
    #             rec[field] = 'redacted'
    #         rec['current_billing_cycle'] = 0

    #         rec['status_history'] = [rec['status_history'][-1]]
    #         rec['transactions'] = [rec['transactions'][-1]]
    #     if stream == 'acmeCo/transactions':
    #         for field in TRANSACTION_FIELDS_TO_REDACT:
    #             if isinstance(rec[field], list) or isinstance(rec[field], dict):
    #                 redact_nested_fields(rec[field])
    #             else:
    #                 rec[field] = 'redacted'

    assert snapshot("capture.stdout.json") == unique_stream_lines


def test_discover(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "discover",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "-o",
            "json",
            "--emit-raw",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # Sort lines to keep a consistent ordering of discovered bindings.
    sorted_lines = sorted(lines, key=lambda l: l["recommendedName"])

    assert snapshot("capture.stdout.json") == sorted_lines


def test_spec(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "spec",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    assert snapshot("capture.stdout.json") == lines

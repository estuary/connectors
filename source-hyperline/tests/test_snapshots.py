import json
import subprocess

# Fields redacted from capture snapshots. The test account holds production
# billing data, so this list is deliberately broad: PII and customer-
# identifying fields (kept in sync with the bruno collection's PII_KEYS in
# bruno/opencollection.yml), plus financial values and volatile fields that
# would churn between runs.
REDACTED = "REDACTED"
REDACTED_FIELDS = {
    # PII / customer-identifying (mirror of bruno PII_KEYS)
    "name",
    "billing_email",
    "invoice_emails",
    "email",
    "owner_email",
    "domain",
    "billing_address",
    "shipping_address",
    "address",
    "tax_ids",
    "tax_id",
    "local_tax_number",
    "registration_number",
    "external_id",
    "owner",
    "public_url",
    "url",
    "signed_file",
    "phone",
    "custom_properties",
    "properties",
    "comments",
    "terms",
    "additional_info",
    "custom_note",
    "footer",
    "document_name",
    "integrations",
    "first_name",
    "last_name",
    "display_name",
    "company_name",
    "legal_name",
    "trade_name",
    "picture_url",
    "logo_url",
    "website",
    "invoice_late_fees",
    "invoice_footer",
    "document_footer",
    "invoice_memo",
    "bank_details",
    "address_line1",
    "address_line2",
    "city",
    "zip_code",
    "state",
    "vat_number",
    # financial values (production amounts don't belong in a public repo)
    "amount",
    "amount_due",
    "amount_paid",
    "amount_remaining",
    "total_amount",
    "subtotal",
    "total",
    "balance",
    "projected_balance",
    "arr",
    "mrr",
    "unit_price",
    "prices",
    # volatile between runs
    "last_refreshed_at",
}


def _redact(value):
    if isinstance(value, dict):
        return {
            k: (REDACTED if k in REDACTED_FIELDS else _redact(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


def test_capture(request, snapshot):
    result = subprocess.run(
        [
            "flowctl",
            "raw",
            "preview-next",
            "--source",
            request.fspath.dirname + "/../test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "10s",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
    lines = [json.loads(l) for l in result.stdout.splitlines()]

    # Keep exactly one document per stream: enough to catch structural
    # regressions without snapshotting the account's full contents.
    unique_stream_lines = []
    seen = set()

    for line in lines:
        stream = line[0]
        if stream not in seen:
            unique_stream_lines.append(line)
            seen.add(stream)

    assert snapshot("capture.stdout.json") == _redact(unique_stream_lines)


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

    lines = sorted(
        [json.loads(l) for l in result.stdout.splitlines()],
        key=lambda l: l["recommendedName"],
    )

    assert snapshot("stdout.json") == lines


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

    assert snapshot("stdout.json") == lines

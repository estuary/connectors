from datetime import datetime

from pydantic import AwareDatetime

from ..models import (
    ConditionalField,
    ShopifyGraphQLResource,
    StoreCapabilities,
    requires_any_scope,
)
from ..utils import dt_to_str, str_to_dt


class Disputes(ShopifyGraphQLResource):
    NAME = "disputes"
    QUERY_ROOT = "disputes"
    SHOULD_USE_BULK_QUERIES = False
    # The disputes query offers no datetime sortKey, so the connector routes
    # this stream to the unsorted incremental path.
    SORT_KEY = None
    QUALIFYING_SCOPES = {"read_shopify_payments_disputes"}
    # Disputes are filtered based off of the initiatedAt field, which lets the
    # connector incrementally capture creations of new records but miss updates
    # to older records. To capture updates, we re-backfill the binding every day.
    BACKFILL_SCHEDULE = "55 23 * * *"
    QUERY = """
    id
    legacyResourceId
    amount {
        amount
        currencyCode
    }
    reasonDetails {
        reason
        networkReasonCode
    }
    status
    type
    initiatedAt
    evidenceDueBy
    evidenceSentOn
    finalizedOn
    order {
        id
        legacyResourceId
    }
    # {{ disputeEvidence }}
    """
    # disputeEvidence requires the read_shopify_payments_dispute_evidences scope, which is
    # distinct from the read_shopify_payments_disputes scope that gates the stream itself. A
    # store can grant disputes access without granting evidence access, so query this block
    # only when the evidence scope is present.
    CONDITIONAL_FIELDS = [
        ConditionalField(
            placeholder="# {{ disputeEvidence }}",
            fields="""disputeEvidence {
        id
        accessActivityLog
        cancellationPolicyDisclosure
        cancellationRebuttal
        customerEmailAddress
        customerFirstName
        customerLastName
        customerPurchaseIp
        productDescription
        refundPolicyDisclosure
        refundRefusalExplanation
        submitted
        uncategorizedText
        billingAddress {
            address1
            address2
            city
            company
            country
            countryCodeV2
            firstName
            lastName
            latitude
            longitude
            name
            phone
            province
            provinceCode
            zip
        }
        shippingAddress {
            address1
            address2
            city
            company
            country
            countryCodeV2
            firstName
            lastName
            latitude
            longitude
            name
            phone
            province
            provinceCode
            zip
        }
        cancellationPolicyFile {
            id
            fileType
            fileSize
            url
            originalFileName
            disputeEvidenceType
        }
        customerCommunicationFile {
            id
            fileType
            fileSize
            url
            originalFileName
            disputeEvidenceType
        }
        refundPolicyFile {
            id
            fileType
            fileSize
            url
            originalFileName
            disputeEvidenceType
        }
        serviceDocumentationFile {
            id
            fileType
            fileSize
            url
            originalFileName
            disputeEvidenceType
        }
        shippingDocumentationFile {
            id
            fileType
            fileSize
            url
            originalFileName
            disputeEvidenceType
        }
        uncategorizedFile {
            id
            fileType
            fileSize
            url
            originalFileName
            disputeEvidenceType
        }
        disputeFileUploads {
            id
            fileType
            fileSize
            url
            originalFileName
            disputeEvidenceType
        }
        fulfillments {
            id
            shippingCarrier
            shippingDate
            shippingTrackingNumber
        }
    }""",
            is_available=requires_any_scope(
                "read_shopify_payments_dispute_evidences"
            ),
        ),
    ]

    def get_cursor_value(self) -> AwareDatetime:
        raw_value = getattr(self, "initiatedAt")

        if isinstance(raw_value, str):
            return str_to_dt(raw_value)
        elif isinstance(raw_value, datetime):
            return raw_value
        else:
            raise ValueError(f"Expected datetime string, got {type(raw_value)}")

    # Since disputes are filtered based off of the unique initiatedAt field, we can't reuse
    # ShopifyGraphQLResource's build_query_with_fragment method that filters based off
    # of the updatedAt field.
    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(end)
        query_body = Disputes._resolve_conditional_fields(capabilities)
        return f"""
        {{
            disputes(
                query: "initiated_at:>='{lower_bound}' AND initiated_at:<='{upper_bound}'"
                {f"first: {first}" if first else ""}
                {f'after: "{after}"' if after else ""}
            ) {{
                edges {{
                    node {{
                        {query_body}
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
        """

from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ...models import ShopifyGraphQLResource


class OrderRisks(ShopifyGraphQLResource):
    NAME = "order_risks"
    QUERY = """
    risk {
        recommendation
        assessments {
            riskLevel
            facts {
                description
                sentiment
            }
            provider {
                features
                description
                handle
                embedded
                title
                published
                developerName
                developerType
                appStoreAppUrl
                installUrl
                appStoreDeveloperUrl
                isPostPurchaseAppInUse
                previouslyInstalled
                pricingDetailsSummary
                pricingDetails
                privacyPolicyUrl
                publicCategory
                uninstallMessage
                webhookApiVersion
                shopifyDeveloped
                id
                failedRequirements {
                    message
                    action {
                        title
                        url
                        id
                    }
                }
                feedback {
                    link {
                        label
                        url
                    }
                    messages {
                        field
                        message
                    }
                }
            }
        }
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return OrderRisks.build_query_with_fragment(
            "orders",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in OrderRisks._process_result(
            log, lines, "gid://shopify/Order/"
        ):
            yield record

from datetime import datetime, timedelta

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


# Shopify's bulk GraphQL queries round our "updated_at" datetime queries down to the earliest midnight.
# Meaning, if our end datetime was 2025-01-16T12:00:00Z, then Shopify would round it down to 2025-01-16T00:00:00Z.
# Effectively, we would miss all updates in the current day until midnight of the next day. To avoid this, the
# round_to_latest_midnight function rounds queries' end datetimes to midnight of the next day.
def round_to_latest_midnight(dt: datetime) -> datetime:
    if dt.time() == datetime.min.time():
        return dt
    else:
        return (dt + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )


money_bag_fragment = """
fragment _MoneyBagFields on MoneyBag {
    shopMoney {
        amount
        currencyCode
    }
    presentmentMoney {
        amount
        currencyCode
    }
}
"""

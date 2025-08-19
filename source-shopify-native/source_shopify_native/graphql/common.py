from datetime import datetime, timedelta

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
VERSION = "2025-04"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


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

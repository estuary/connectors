from datetime import datetime, timedelta

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
VERSION = "2026-01"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def str_to_dt(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))


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

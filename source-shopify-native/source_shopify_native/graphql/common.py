VERSION = "2026-01"


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


money_v2_fragment = """
fragment _MoneyV2Fields on MoneyV2 {
    amount
    currencyCode
}
"""

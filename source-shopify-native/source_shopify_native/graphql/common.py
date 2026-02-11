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

# Pure functions for provider API interaction live here; `add-stream` fills them in.

# The SaaSOptics base URL is per-tenant, built from the `subdomain` and
# `account_name` config fields.
API_URL_TEMPLATE = "https://{subdomain}.saasoptics.com/{account_name}/api/v1.0"


def base_url(subdomain: str, account_name: str) -> str:
    return API_URL_TEMPLATE.format(subdomain=subdomain, account_name=account_name)

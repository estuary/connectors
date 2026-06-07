# Pin the Zuora REST API minor version so responses don't shift when a tenant's
# default version is upgraded. 2025-08-12 is the latest date-formatted minor
# version. https://developer.zuora.com/v1-api-reference/api-versions
ZUORA_API_VERSION = "2025-08-12"
VERSION_HEADERS: dict[str, str] = {"Zuora-Version": ZUORA_API_VERSION}

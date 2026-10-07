# Pure functions for provider API interaction live here; `add-stream` fills them in.


def base_url(domain: str) -> str:
    """The Management API root of an Okta org.

    Okta has no single API host: every org is served from its own domain
    (`dev-123456.okta.com`, `acme.okta-emea.com`, `acme.oktapreview.com`, or a
    custom domain). `domain` is the bare host the config validator guarantees.
    """
    return f"https://{domain}/api/v1"

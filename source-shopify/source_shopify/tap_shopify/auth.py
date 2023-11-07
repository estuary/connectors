"""tap_shopify Authentication."""

from singer_sdk.authenticators import APIKeyAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes the streams reuse the same authenticator instance.
class tap_shopifyAuthenticator(APIKeyAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for tap_shopify."""

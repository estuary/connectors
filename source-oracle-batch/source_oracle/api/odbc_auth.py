import time

from authlib.common.security import generate_token
from oauthlib.oauth1.rfc5849.signature import sign_hmac_sha256

from source_netsuite.models import EndpointConfig


def generate_base_string(account: str, auth: EndpointConfig.TokenAuth) -> str:
    """
    Normally the base string is generated using a request which contains a host
    but netsuite has a different format for HMAC
    """
    nonce = generate_token(32)
    timestamp = str(int(time.time()))
    base_string = f"{account}&{auth.consumer_key}&{auth.token_id}&{nonce}&{timestamp}"
    return base_string


def generate_token_password(config: EndpointConfig) -> str:
    """
    Generates a token password for use with SuiteAnalytics Connect ODBC driver.
    This is a non-standard format used exclusively by the NetSuite ODBC driver.
    """
    assert isinstance(config.authentication, EndpointConfig.TokenAuth)

    base_string = generate_base_string(config.account, config.authentication)
    signature = sign_hmac_sha256(base_string, config.authentication.consumer_secret, config.authentication.token_secret)
    return f"{base_string}&{signature}&HMAC-SHA256"


def generate_connection_config(cfg: EndpointConfig) -> dict:
    """
    See bin/sanitize-netsuite-driver for more details
    """
    # the account ID for sandboxes is formatted differently, we need to extract the account number to properly
    # construct the ODBC connection URL, which is different in prod vs sandbox.

    # TODO consider moving some of this logic into the core NS lib
    account_id = cfg.account
    parts = account_id.split("_")
    numeric_part = parts[0]
    sandbox_number_part = "-".join(parts[1:]).lower()

    if sandbox_number_part:
        # 7139061-sb1.connect.api.netsuite.com
        dynamic_host_segment = f"{numeric_part}-{sandbox_number_part}"
    else:
        # 7139061.connect.api.netsuite.com
        dynamic_host_segment = numeric_part

    host = f"{dynamic_host_segment}.connect.api.netsuite.com"

    odbc_params = {
        "DSN": "NetSuite",
        # this strange semicolon-within-semicolon-string formatting is correct, as weird as it looks
        "CustomProperties": f"AccountID={account_id};RoleID={cfg.role_id}",
        "Host": host,
        "ServerDataSource": cfg.suiteanalytics_data_source,
        # pretty certain this port is constant, even across data sources
        "Port": 1708,
    }

    if isinstance(cfg.authentication, EndpointConfig.TokenAuth):
        odbc_params["LogonId"] = "TBA"
        odbc_params["Password"] = generate_token_password(cfg)
    elif isinstance(cfg.authentication, EndpointConfig.UsernamePasswordAuth):
        # from NS docs ("Connecting Using a Connection String")
        #
        # > For example, a connection string may look like the following. Only the data source name (DSN),
        # > user name (UID), and password (PWD) are required:

        odbc_params = odbc_params | {
            "UID": cfg.authentication.username,
            "PWD": cfg.authentication.password,
        }

    return odbc_params


def generate_connection_string(cfg: EndpointConfig) -> str:
    odbc_connection_params = generate_connection_config(cfg)
    return ";".join(f"{k}={v}" for k, v in odbc_connection_params.items()) + ";"

from logging import Logger

from estuary_cdk.http import HTTPMixin, HTTPError
from estuary_cdk.flow import ValidationError

from .constants import BASE_URL


async def validate_credentials(log: Logger, http: HTTPMixin) -> None:
    try:
        await http.request(
            log, f"{BASE_URL}/me", params={"fields": "id,name"}
        )
    except HTTPError as e:
        if e.code == 401:
            raise ValidationError(
                ["Invalid Facebook API credentials. Please check your access token."]
            )
        elif e.code == 403:
            raise ValidationError(
                [
                    "Insufficient permissions for Facebook API. Please ensure the access token has the required scopes."
                ]
            )
        else:
            raise ValidationError(
                [f"Failed to authenticate with Facebook API: {e.message}"]
            )


async def validate_access_to_accounts(
    log: Logger, http: HTTPMixin, accounts: list[str]
) -> None:
    for account_id in accounts:
        try:
            await http.request(
                log,
                f"{BASE_URL}/act_{account_id}",
                params={
                    "fields": "account_id",
                },
            )
        except HTTPError as e:
            common_msg = [
                f"Error: {e.code}, {e.message}.",
                "Please also verify your Account ID: See the https://www.facebook.com/business/help/1492627900875762 for more information.",
            ]

            if e.code == 401:
                raise ValidationError(
                    [f"Invalid credentials. Unable to access Ad Account {account_id}."]
                    + common_msg,
                )
            elif e.code == 403:
                raise ValidationError(
                    [
                        f"Insufficient permissions to access Facebook Ad Account {account_id}."
                    ]
                    + common_msg,
                )
            else:
                raise ValidationError(
                    [f"Failed to fetch Facebook Ad Account {account_id}: {e.message}"]
                    + common_msg,
                )


def str_to_list(value: str) -> list[str]:
    values = [v for v in value.split(",")]
    stripped_values = [v.strip() for v in values if v.strip()]
    return stripped_values

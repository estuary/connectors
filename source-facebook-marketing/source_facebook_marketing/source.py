#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import re
from typing import Any, List, Mapping, Optional, Tuple, Type

import facebook_business
import pendulum
import requests
from airbyte_cdk.models import ConnectorSpecification, DestinationSyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from pydantic.error_wrappers import ValidationError
from source_facebook_marketing.api import API
from source_facebook_marketing.spec import ConnectorConfig
from source_facebook_marketing.streams import (
    Activities,
    AdAccount,
    AdCreatives,
    Ads,
    AdSets,
    AdsInsights,
    AdsInsightsActionType,
    AdsInsightsAgeAndGender,
    AdsInsightsCountry,
    AdsInsightsDma,
    AdsInsightsPlatformAndDevice,
    AdsInsightsRegion,
    Campaigns,
    CustomConversions,
    Images,
    Videos,
)

from .utils import validate_end_date, validate_start_date, validate_account_ids

logger = logging.getLogger("airbyte")
UNSUPPORTED_FIELDS = {"unique_conversions", "unique_ctr", "unique_clicks",
                      "age_targeting", "gender_targeting", "labels", "location",
                      "estimated_ad_recall_rate_lower_bound", "estimated_ad_recall_rate_upper_bound", "estimated_ad_recallers_lower_bound", "estimated_ad_recallers_upper_bound" }

ACCOUNT_ID_DELIMITER = r",+"

class SourceFacebookMarketing(AbstractSource):
    def _validate_and_transform(self, config: Mapping[str, Any]):
        config.setdefault("action_breakdowns_allow_empty", False)
        if config.get("end_date") == "":
            config.pop("end_date")

        # Backwards compatible support for configs with the older
        # account_id field instead of the newer account_ids.
        if not config.get("account_ids", ""):
            config["account_ids"] = config.get("account_id")

        config = ConnectorConfig.parse_obj(config)

        # Convert account_ids to a list so it's easier to work with throughout the connector.
        config.account_ids = [n for n in re.split(ACCOUNT_ID_DELIMITER, config.account_ids)]

        config.start_date = pendulum.instance(config.start_date)
        config.end_date = pendulum.instance(config.end_date)
        return config

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        """Connection check to validate that the user-provided config can be used to connect to the underlying API

        :param logger: source logger
        :param config:  the user-input config object conforming to the connector's spec.json
        :return Tuple[bool, Any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            config = self._validate_and_transform(config)

            if config.end_date > pendulum.now():
                return False, "Date range can not be in the future."
            if config.end_date < config.start_date:
                return False, "end_date must be equal or after start_date."

            api = API(access_token=config.credentials.access_token)
        except (requests.exceptions.RequestException, ValidationError) as e:
            return False, e

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Type[Stream]]:
        """Discovery method, returns available streams

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        :return: list of the stream instances
        """
        config = self._validate_and_transform(config)
        config.start_date = validate_start_date(config.start_date)
        config.end_date = validate_end_date(config.start_date, config.end_date)

        api = API(access_token=config.credentials.access_token)

        validate_account_ids(api, config.account_ids)

        insights_args = dict(
            api=api, start_date=config.start_date, end_date=config.end_date, insights_lookback_window=config.insights_lookback_window, account_ids=config.account_ids,
        )
        streams = [
            AdAccount(
                api=api,
                account_ids=config.account_ids,
                source_defined_primary_key=["account_id"],
            ),
            AdSets(
                api=api,
                start_date=config.start_date,
                account_ids=config.account_ids,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["id"],
            ),
            Ads(
                api=api,
                start_date=config.start_date,
                account_ids=config.account_ids,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["id"],
            ),
            AdCreatives(
                api=api,
                fetch_thumbnail_images=config.fetch_thumbnail_images,
                account_ids=config.account_ids,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["id"],
            ),
            AdsInsights(page_size=config.page_size, max_batch_size=config.max_batch_size, source_defined_primary_key=["date_start", "account_id", "ad_id"], **insights_args),
            AdsInsightsAgeAndGender(page_size=config.page_size, max_batch_size=config.max_batch_size,source_defined_primary_key=["id"], **insights_args),
            AdsInsightsCountry(page_size=config.page_size, max_batch_size=config.max_batch_size, source_defined_primary_key=["date_start", "account_id", "ad_id"], **insights_args),
            AdsInsightsRegion(page_size=config.page_size, max_batch_size=config.max_batch_size, source_defined_primary_key=["date_start", "account_id", "ad_id"], **insights_args),
            AdsInsightsDma(page_size=config.page_size, max_batch_size=config.max_batch_size, source_defined_primary_key=["date_start", "account_id", "ad_id"], **insights_args),
            AdsInsightsPlatformAndDevice(page_size=config.page_size, max_batch_size=config.max_batch_size, source_defined_primary_key=["date_start", "account_id", "ad_id"], **insights_args),
            AdsInsightsActionType(page_size=config.page_size, max_batch_size=config.max_batch_size, source_defined_primary_key=["id"], **insights_args),
            Campaigns(
                api=api,
                account_ids=config.account_ids,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["id"],
            ),
            CustomConversions(
                api=api,
                account_ids=config.account_ids,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["id"],
            ),
            Images(
                api=api,
                account_ids=config.account_ids,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["id"],
            ),
            Videos(
                api=api,
                account_ids=config.account_ids,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["id"],
            ),
            Activities(
                api=api,
                account_ids=config.account_ids,
                start_date=config.start_date,
                end_date=config.end_date,
                include_deleted=config.include_deleted,
                page_size=config.page_size,
                max_batch_size=config.max_batch_size,
                source_defined_primary_key=["object_id", "actor_id", "application_id", "event_time", "event_type"],
            ),
        ]

        return streams + self.get_custom_insights_streams(api, config)

    def spec(self, *args, **kwargs) -> ConnectorSpecification:
        """Returns the spec for this integration.
        The spec is a JSON-Schema object describing the required configurations
        (e.g: username and password) required to run this integration.
        """
        return ConnectorSpecification(
            documentationUrl="https://go.estuary.dev/facebook-marketing",
            supportsIncremental=True,
            supported_destination_sync_modes=[DestinationSyncMode.append],
            connectionSpecification=ConnectorConfig.schema(),
            authSpecification=None
        )

    def get_custom_insights_streams(self, api: API, config: ConnectorConfig) -> List[Type[Stream]]:
        """return custom insights streams"""
        streams = []
        for insight in config.custom_insights or []:
            insight_fields = set(insight.fields)
            if insight_fields.intersection(UNSUPPORTED_FIELDS):
                # https://github.com/airbytehq/oncall/issues/1137
                mes = (
                    f"Please remove Following fields from the Custom {insight.name} fields list due to possible "
                    f"errors on Facebook side: {insight_fields.intersection(UNSUPPORTED_FIELDS)}"
                )
                raise ValueError(mes)
            stream = AdsInsights(
                api=api,
                account_ids=config.account_ids,
                name=f"Custom{insight.name}",
                fields=list(insight_fields),
                breakdowns=list(set(insight.breakdowns)),
                action_breakdowns=list(set(insight.action_breakdowns)),
                action_breakdowns_allow_empty=config.action_breakdowns_allow_empty,
                time_increment=insight.time_increment,
                start_date=insight.start_date or config.start_date,
                end_date=insight.end_date or config.end_date,
                insights_lookback_window=insight.insights_lookback_window or config.insights_lookback_window,
                level=insight.level,
                is_custom_stream=True,
            )
            streams.append(stream)
        return streams

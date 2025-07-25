#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import pendulum
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v19.errors.types.authorization_error import AuthorizationErrorEnum
from google.ads.googleads.v19.errors.types.request_error import RequestErrorEnum
from google.ads.googleads.v19.services.services.google_ads_service.pagers import SearchPager

from .google_ads import GoogleAds
from .models import Customer


class cyclic_sieve:
    def __init__(self, logger: logging.Logger, fraction: int = 10):
        self._logger = logger
        self._cycle_counter = 0
        self._fraction = fraction

    def __getattr__(self, item):
        if self._cycle_counter % self._fraction == 0:
            return getattr(self._logger, item)
        return self.stub

    def stub(self, *args, **kwargs):
        pass

    def bump(self):
        self._cycle_counter += 1


def parse_dates(stream_slice):
    start_date = pendulum.parse(stream_slice["start_date"])
    end_date = pendulum.parse(stream_slice["end_date"])
    return start_date, end_date


def get_date_params(start_date: str, time_zone=None, range_days: int = None, end_date: pendulum.datetime = None) -> Tuple[str, str]:
    """
    Returns `start_date` and `end_date` for the given stream_slice.
    If (end_date - start_date) is a big date range (>= 1 month), it can take more than 2 hours to process all the records from the given slice.
    After 2 hours next page tokens will be expired, finally resulting in page token expired error
    Currently this method returns `start_date` and `end_date` with 15 days difference.
    """

    end_date = end_date or pendulum.yesterday(tz=time_zone)
    start_date = pendulum.parse(start_date)
    if start_date > pendulum.now():
        return start_date.to_date_string(), start_date.add(days=1).to_date_string()
    end_date = min(end_date, start_date.add(days=range_days))

    # Fix issue #4806, start date should always be lower than end date.
    if start_date.add(days=1).date() >= end_date.date():
        return start_date.add(days=1).to_date_string(), start_date.add(days=2).to_date_string()
    return start_date.add(days=1).to_date_string(), end_date.to_date_string()


def chunk_date_range(
    logger: logging.Logger,
    start_date: str,
    conversion_window: int,
    field: str,
    end_date: str = None,
    days_of_data_storage: int = None,
    range_days: int = None,
    time_zone=None,
) -> Iterable[Optional[MutableMapping[str, any]]]:
    """
    Passing optional parameter end_date for testing
    Returns a list of the beginning and ending timestamps of each `range_days` between the start date and now.
    The return value is a list of dicts {'date': str} which can be used directly with the Slack API
    """
    logger.debug("Entering chunk_date_range.")
    intervals = []
    end_date = pendulum.parse(end_date) if end_date else pendulum.now()
    start_date = pendulum.parse(start_date)

    # For some metrics we can only get data not older than N days, it is Google Ads policy
    if days_of_data_storage:
        start_date = max(start_date, pendulum.now().subtract(days=days_of_data_storage - conversion_window))

    # As in to return some state when state in abnormal
    if start_date > end_date:
        logger.debug("Exiting chunk_date_range - returned [None]")
        return [None]

    # applying conversion window
    start_date = start_date.subtract(days=conversion_window)

    while start_date < end_date:
        logger.debug("In 'while start_date < end_date' loop", {
            "start_date": start_date,
            "end_date": end_date,
        })
        start, end = get_date_params(start_date.to_date_string(), time_zone=time_zone, range_days=range_days, end_date=end_date)
        intervals.append(
            {
                "start_date": start,
                "end_date": end,
            }
        )
        start_date = start_date.add(days=range_days)

    logger.debug("Exiting chunk date range.", {
        "intervals": intervals,
    })
    return intervals


class GoogleAdsStream(Stream, ABC):
    CATCH_API_ERRORS = True

    def __init__(self, api: GoogleAds, customers: List[Customer]):
        self.google_ads_client = api
        self.customers = customers
        self.base_sieve_logger = cyclic_sieve(self.logger, 10)

    def get_query(self, stream_slice: Mapping[str, Any]) -> str:
        self.logger.debug("Entering GoogleAdsStream.get_query")
        query = GoogleAds.convert_schema_into_query(schema=self.get_json_schema(), report_name=self.name)
        self.logger.debug("Exiting GoogleAdsStream.get_query", {
            "query": query,
        })
        return query

    def parse_response(self, response: SearchPager) -> Iterable[Mapping]:
        for result in response:
            yield self.google_ads_client.parse_single_result(self.get_json_schema(), result)

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for customer in self.customers:
            yield {"customer_id": customer.id}

    def read_records(self, sync_mode, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        self.logger.debug("Entering GoogleAdsStream.read_records")
        self.base_sieve_logger.bump()
        self.base_sieve_logger.info(f"Read records using g-ads client. Stream slice is {stream_slice}")
        if stream_slice is None:
            self.logger.debug("No slice - Exiting GoogleAdsStream.read_records.")
            return []

        customer_id = stream_slice["customer_id"]
        try:
            self.logger.debug("Sending request.")
            response_records = self.google_ads_client.send_request(self.get_query(stream_slice), customer_id=customer_id)
            self.logger.debug("Received response. Parsing response")
            for response in response_records:
                yield from self.parse_response(response)
        except GoogleAdsException as exc:
            self.logger.debug("Received exception", {
                "exc": exc,
                "customer_id": customer_id,
            })
            exc.customer_id = customer_id
            if not self.CATCH_API_ERRORS:
                raise
            for error in exc.failure.errors:
                if error.error_code.authorization_error == AuthorizationErrorEnum.AuthorizationError.CUSTOMER_NOT_ENABLED:
                    self.logger.error("Ignoring CUSTOMER_NOT_ENABLED error", {
                        "error.message": error.message
                        }
                    )
                    continue
                # log and ignore only CUSTOMER_NOT_ENABLED error, otherwise - raise further
                raise

        self.logger.debug("Exiting GoogleAdsStream.read_records.")


class IncrementalGoogleAdsStream(GoogleAdsStream, IncrementalMixin, ABC):
    days_of_data_storage = None
    cursor_field = "segments.date"
    primary_key = None
    # Date range is set to 15 days, because for conversion_window_days default value is 14.
    # Range less than 15 days will break the integration tests.
    range_days = 15

    def __init__(self, start_date: str, conversion_window_days: int, end_date: str = None, **kwargs):
        self.conversion_window_days = conversion_window_days
        self._start_date = start_date
        self._end_date = end_date
        self._state = {}
        super().__init__(**kwargs)
        self.incremental_sieve_logger = cyclic_sieve(self.logger, 10)

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state.update(value)

    def current_state(self, customer_id, default=None):
        default = default or self.state.get(self.cursor_field)
        return self.state.get(customer_id, {}).get(self.cursor_field) or default

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[MutableMapping[str, any]]]:
        self.logger.debug("Entering stream slices.")
        for customer in self.customers:
            logger = cyclic_sieve(self.logger, 10)
            stream_state = stream_state or {}
            if stream_state.get(customer.id):
                start_date = stream_state[customer.id].get(self.cursor_field) or self._start_date

            # We should keep backward compatibility with the previous version
            elif stream_state.get(self.cursor_field) and len(self.customers) == 1:
                start_date = stream_state.get(self.cursor_field) or self._start_date
            else:
                start_date = self._start_date

            end_date = self._end_date
            logger.info(f"Generating slices for customer {customer.id}. Start date is {start_date}, end date is {end_date}")

            for chunk in chunk_date_range(
                logger=self.logger,
                start_date=start_date,
                end_date=end_date,
                conversion_window=self.conversion_window_days,
                field=self.cursor_field,
                days_of_data_storage=self.days_of_data_storage,
                range_days=self.range_days,
                time_zone=customer.time_zone,
            ):
                if chunk:
                    chunk["customer_id"] = customer.id
                logger.info(f"Next slice is {chunk}")
                logger.bump()
                yield chunk

        self.logger.debug("Exiting stream slices.")

    def read_records(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: MutableMapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        """
        This method is overridden to handle GoogleAdsException with EXPIRED_PAGE_TOKEN error code,
        and update `start_date` key in the `stream_slice` with the latest read record's cursor value, then retry the sync.
        """
        self.logger.debug("Entering IncrementalGooglAdsStream.read_records")
        self.incremental_sieve_logger.bump()
        previous_log_message = ""
        while True:
            self.logger.info("Starting a while loop iteration")
            customer_id = stream_slice and stream_slice["customer_id"]
            try:
                records = super().read_records(sync_mode, stream_slice=stream_slice)
                for record in records:
                    current_state = self.current_state(customer_id)
                    if current_state:
                        date_in_current_stream = pendulum.parse(current_state)
                        date_in_latest_record = pendulum.parse(record[self.cursor_field])
                        cursor_value = (max(date_in_current_stream, date_in_latest_record)).to_date_string()
                        self.state = {customer_id: {self.cursor_field: cursor_value}}

                        # If the log_message hasn't changed, don't log it.
                        log_message = f"Updated state for customer {customer_id}. Full state is {self.state}."
                        if log_message != previous_log_message:
                            self.logger.info(log_message)
                            previous_log_message = log_message

                        yield record
                        continue
                    self.state = {customer_id: {self.cursor_field: record[self.cursor_field]}}
                    self.logger.info(f"Initialized state for customer {customer_id}. Full state is {self.state}.")
                    yield record
                    continue
            except GoogleAdsException as exception:
                self.logger.info(f"Caught a GoogleAdsException: {str(exception)}")
                error = next(iter(exception.failure.errors))
                if error.error_code.request_error == RequestErrorEnum.RequestError.EXPIRED_PAGE_TOKEN:
                    start_date, end_date = parse_dates(stream_slice)
                    current_state = self.current_state(customer_id)
                    self.logger.info(
                        f"Start date is {start_date}. End date is {end_date}. Current state is {current_state}"
                    )
                    if (end_date - start_date).days == 1:
                        # If range days is 1, no need in retry, because it's the minimum date range
                        self.logger.error("Page token has expired.")
                        raise exception
                    elif current_state == stream_slice["start_date"]:
                        # It couldn't read all the records within one day, it will enter an infinite loop,
                        # so raise the error
                        self.logger.error("Page token has expired.")
                        raise exception
                    # Retry reading records from where it crushed
                    stream_slice["start_date"] = self.current_state(customer_id, default=stream_slice["start_date"])
                    self.logger.info(f"Retry reading records from where it crushed with a modified slice: {stream_slice}")
                else:
                    # raise caught error for other error statuses
                    raise exception
            else:
                # return the control if no exception is raised
                self.logger.info("Current slice has been read. Exiting IncrementalGoogleAdsStream.read_records()")
                return

    def get_query(self, stream_slice: Mapping[str, Any] = None) -> str:
        self.logger.debug("Entering IncrementalGoogleAdsStream.get_query")
        query = GoogleAds.convert_schema_into_query(
            schema=self.get_json_schema(),
            report_name=self.name,
            from_date=stream_slice.get("start_date"),
            to_date=stream_slice.get("end_date"),
            cursor_field=self.cursor_field,
        )
        self.logger.debug("Exiting IncrementalGoogleAdsStream.get_query")
        return query


class Accounts(IncrementalGoogleAdsStream):
    """
    Accounts stream: https://developers.google.com/google-ads/api/fields/v19/customer
    """

    primary_key = ["customer.id", "segments.date"]


class ServiceAccounts(GoogleAdsStream):
    """
    This stream is intended to be used as a service class, not exposed to a user
    """

    CATCH_API_ERRORS = False
    primary_key = ["customer.id"]


class Campaigns(IncrementalGoogleAdsStream):
    """
    Campaigns stream: https://developers.google.com/google-ads/api/fields/v19/campaign
    """

    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)
    primary_key = ["campaign.id", "segments.date", "segments.hour"]


class CampaignLabels(GoogleAdsStream):
    """
    Campaign labels stream: https://developers.google.com/google-ads/api/fields/v19/campaign_label
    """

    # Note that this is a string type. Google doesn't return a more convenient identifier.
    primary_key = ["campaign_label.resource_name"]


class AdGroups(IncrementalGoogleAdsStream):
    """
    AdGroups stream: https://developers.google.com/google-ads/api/fields/v19/ad_group
    """

    primary_key = ["ad_group.id", "segments.date"]


class AdGroupLabels(GoogleAdsStream):
    """
    Ad Group Labels stream: https://developers.google.com/google-ads/api/fields/v19/ad_group_label
    """

    # Note that this is a string type. Google doesn't return a more convenient identifier.
    primary_key = ["ad_group_label.resource_name"]


class AdGroupAds(IncrementalGoogleAdsStream):
    """
    AdGroups stream: https://developers.google.com/google-ads/api/fields/v19/ad_group_ad
    """

    primary_key = ["ad_group_ad.ad.id", "segments.date"]


class AdGroupAdLabels(GoogleAdsStream):
    """
    Ad Group Ad Labels stream: https://developers.google.com/google-ads/api/fields/v19/ad_group_ad_label
    """

    # Note that this is a string type. Google doesn't return a more convenient identifier.
    primary_key = ["ad_group_ad_label.resource_name"]


class AccountPerformanceReport(IncrementalGoogleAdsStream):
    """
    AccountPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v19/customer
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#account_performance
    """
    primary_key = [
        "segments.ad_network_type",
        "segments.device",
        "segments.date",
        "customer.id",
    ]


class AdGroupAdReport(IncrementalGoogleAdsStream):
    """
    AdGroupAdReport stream: https://developers.google.com/google-ads/api/fields/v19/ad_group_ad
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#ad_performance
    """
    primary_key = [
        "segments.ad_network_type",
        "segments.date",
        "ad_group.id",
        "campaign.id",
        "customer.id",
        "ad_group_ad.ad.id"
    ]


class DisplayKeywordPerformanceReport(IncrementalGoogleAdsStream):
    """
    DisplayKeywordPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v19/display_keyword_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#display_keyword_performance
    """
    primary_key = [
        "segments.ad_network_type",
        "segments.device",
        "segments.date",
        "ad_group.id",
        "customer.id",
        "campaign.id",
        "ad_group_criterion.criterion_id"
    ]


class DisplayTopicsPerformanceReport(IncrementalGoogleAdsStream):
    """
    DisplayTopicsPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v19/topic_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#display_topics_performance
    """
    primary_key = [
        "segments.ad_network_type",
        "segments.device",
        "segments.date",
        "ad_group.id",
        "customer.id",
        "campaign.id",
        "ad_group_criterion.criterion_id"
    ]


class ShoppingPerformanceReport(IncrementalGoogleAdsStream):
    """
    ShoppingPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v19/shopping_performance_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#shopping_performance
    """
    primary_key = [
        "segments.ad_network_type",
        "segments.device",
        "segments.date",
        "ad_group.id",
        "customer.id",
        "campaign.id",
        "segments.product_item_id"
    ]


class UserLocationReport(IncrementalGoogleAdsStream):
    """
    UserLocationReport stream: https://developers.google.com/google-ads/api/fields/v19/user_location_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#geo_performance
    """
    primary_key = [
        "segments.ad_network_type",
        "segments.date",
        "customer.id",
        "campaign.id",
        "user_location_view.resource_name"
    ]


class GeographicReport(IncrementalGoogleAdsStream):
    """
    UserLocationReport stream: https://developers.google.com/google-ads/api/fields/v19/geographic_view
    """
    primary_key = [
        "segments.date",
        "ad_group.id",
        "geographic_view.country_criterion_id",
        "customer.descriptive_name"
    ]


class KeywordReport(IncrementalGoogleAdsStream):
    """
    UserLocationReport stream: https://developers.google.com/google-ads/api/fields/v19/keyword_view
    """
    primary_key = [
        "segments.date",
        "ad_group.id",
        "ad_group_criterion.criterion_id"
    ]


class ClickView(IncrementalGoogleAdsStream):
    """
    ClickView stream: https://developers.google.com/google-ads/api/reference/rpc/v19/ClickView
    """

    primary_key = ["click_view.gclid", "segments.date", "segments.ad_network_type"]
    days_of_data_storage = 90
    range_days = 1

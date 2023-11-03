"""Criteo tap class."""

from typing import Dict, List, Type

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_criteo.client import CriteoStream
from tap_criteo.streams import v202007, v202104, v202107

OBJECT_STREAMS: Dict[str, List[Type[CriteoStream]]] = {
    "legacy": [
        v202007.AudiencesStream,
        v202007.CampaignsStream,
        v202007.CategoriesStream,
    ],
    "current": [
        v202104.AudiencesStream,
        v202104.AdvertisersStream,
        v202104.AdSetsStream,
    ],
    "preview": [
        v202107.CampaignsStream,
    ],
}

REPORTS_BASE = v202104.StatsReportStream


class TapCriteo(Tap):
    """Criteo tap class."""

    name = "tap-criteo"

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("advertiser_ids", th.ArrayType(th.StringType), required=True),
        th.Property("start_date", th.DateTimeType, required=True),
        th.Property(
            "reports",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property(
                        "dimensions",
                        th.ArrayType(th.StringType),
                        required=True,
                    ),
                    th.Property("metrics", th.ArrayType(th.StringType), required=True),
                    th.Property("currency", th.StringType, default="USD"),
                ),
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams.

        Returns:
            List of stream instances.
        """
        objects = [
            stream_class(tap=self)
            for api in ("current", "preview")
            for stream_class in OBJECT_STREAMS[api]
        ]

        reports = [
            REPORTS_BASE(tap=self, report=report) for report in self.config["reports"]
        ]

        return objects + reports

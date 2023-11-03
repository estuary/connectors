"""Stream type classes for Criteo version 2021.07."""

from pathlib import Path

from tap_criteo.client import CriteoSearchStream

SCHEMAS_DIR = Path(__file__).parent.parent / "./schemas"


class CampaignsStream(CriteoSearchStream):
    """Campaigns stream."""

    name = "campaigns"
    path = "/preview/marketing-solutions/campaigns/search"
    schema_filepath = SCHEMAS_DIR / "campaign.json"

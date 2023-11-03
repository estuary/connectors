"""Stream type classes for Criteo version 2020.07."""

from pathlib import Path

from tap_criteo.client import CriteoStream

SCHEMAS_DIR = Path(__file__).parent.parent / "./schemas/v2020.07"


class AudiencesStream(CriteoStream):
    """Audiences stream."""

    name = "legacy_audiences"
    records_jsonpath = "$.audiences[*]"
    path = "/legacy/marketing/v1/audiences?advertiserId=77480"
    schema_filepath = SCHEMAS_DIR / "audience.json"


class CampaignsStream(CriteoStream):
    """Campaigns stream."""

    name = "legacy_campaigns"
    path = "/legacy/marketing/v1/campaigns"
    schema_filepath = SCHEMAS_DIR / "campaign.json"


class CategoriesStream(CriteoStream):
    """Categories stream."""

    name = "legacy_categories"
    path = "/legacy/marketing/v1/categories"
    schema_filepath = SCHEMAS_DIR / "category.json"

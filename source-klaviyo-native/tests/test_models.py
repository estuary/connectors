from source_klaviyo_native.models import Events, Metrics, Profiles


def test_profiles_normalizes_space_separated_datetimes():
    profile = Profiles.model_validate(
        {
            "type": "profile",
            "id": "00AA0A0AA0AA000AAAAAAA0AA0",
            "attributes": {
                "email": "name@example.com",
                "updated": "2023-03-10T20:36:36+00:00",
                "properties": {
                    # Documented consent field, and an arbitrary custom property that also
                    # happens to be a space-separated datetime: both should be normalized.
                    "$consent_timestamp": "2026-06-23 00:15:23.918411+00:00",
                    "custom_signup_date": "2026-06-23 00:15:23+00:00",
                    # Free-text custom properties that merely contain spaces must be left alone.
                    "favorite_month": "May 5, 2021",
                    "status": "onboarding complete",
                    # A trailing newline means it isn't a clean datetime; leave it untouched
                    # rather than dropping the newline.
                    "notes": "2026-06-23 00:15:23+00:00\n",
                    "tags": ["2026-06-23 00:15:23+00:00", "plain tag"],
                },
                "subscriptions": {
                    "email": {
                        "marketing": {
                            "consent": "SUBSCRIBED",
                            "consent_timestamp": "2026-06-23 00:15:23.918411+00:00",
                        }
                    },
                },
            },
        }
    )

    properties = getattr(profile.attributes, "properties")
    assert properties["$consent_timestamp"] == "2026-06-23T00:15:23.918411+00:00"
    assert properties["custom_signup_date"] == "2026-06-23T00:15:23+00:00"
    assert properties["favorite_month"] == "May 5, 2021"
    assert properties["status"] == "onboarding complete"
    assert properties["notes"] == "2026-06-23 00:15:23+00:00\n"
    assert properties["tags"] == ["2026-06-23T00:15:23+00:00", "plain tag"]

    subscriptions = getattr(profile.attributes, "subscriptions")
    assert (
        subscriptions["email"]["marketing"]["consent_timestamp"]
        == "2026-06-23T00:15:23.918411+00:00"
    )


def test_events_normalizes_datetime_cursor_and_keeps_transform():
    event = Events.model_validate(
        {
            "type": "event",
            "id": "3rdp5zEXAMPLE",
            "attributes": {
                "datetime": "2026-06-23 00:15:23+00:00",
                "timestamp": 1750637723,
                "event_properties": {
                    "$flow": "FLOW_ID",
                    "Last Opened": "2026-06-23 00:15:23.918411+00:00",
                },
            },
        }
    )

    assert getattr(event.attributes, "datetime") == "2026-06-23T00:15:23+00:00"
    assert getattr(event.attributes, "event_properties")["Last Opened"] == (
        "2026-06-23T00:15:23.918411+00:00"
    )
    # The pre-existing Events validator that hoists flow/campaign ids must still run.
    assert getattr(event, "flow_id") == "FLOW_ID"


def test_full_refresh_stream_normalizes_space_separated_datetimes():
    metric = Metrics.model_validate(
        {
            "type": "metric",
            "id": "abc123",
            "attributes": {
                "name": "Clicked Email",
                "created": "2026-06-23 00:15:23+00:00",
            },
        }
    )

    assert getattr(metric, "attributes")["created"] == "2026-06-23T00:15:23+00:00"

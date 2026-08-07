"""Unit tests for the creatives destination_url / destination_type flattening.

These tests run fully offline (no Facebook credentials). The creative payloads
are trimmed copies of real Graph API responses, one per ad format that carries
its landing page somewhere different.
"""

from __future__ import annotations

import pytest

from tap_facebook.streams.creative import (
    ADVANCED_FIELDS,
    BASIC_FIELDS,
    DESTINATION_SOURCE_FIELDS,
    CreativeStream,
    extract_destination,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
}

LANDING_PAGE = "https://www.nekt.com/pt?utm_source=meta&utm_medium=paid"


def test_link_ad_uses_link_data_link() -> None:
    creative = {"object_story_spec": {"link_data": {"link": LANDING_PAGE}}}
    assert extract_destination(creative) == (LANDING_PAGE, "website")


def test_video_ad_uses_call_to_action_link() -> None:
    creative = {
        "object_story_spec": {
            "video_data": {
                "video_id": "1",
                "call_to_action": {
                    "type": "LEARN_MORE",
                    "value": {"link": LANDING_PAGE},
                },
            }
        }
    }
    assert extract_destination(creative) == (LANDING_PAGE, "website")


def test_dynamic_creative_uses_asset_feed_spec() -> None:
    creative = {"asset_feed_spec": {"link_urls": [{"website_url": LANDING_PAGE}]}}
    assert extract_destination(creative) == (LANDING_PAGE, "website")


def test_asset_feed_spec_wins_over_empty_story_spec() -> None:
    creative = {
        "asset_feed_spec": {"link_urls": [{"website_url": LANDING_PAGE}]},
        "object_story_spec": {"link_data": {"message": "no link here"}},
    }
    assert extract_destination(creative) == (LANDING_PAGE, "website")


def test_lead_ad_reports_lead_form_and_no_url() -> None:
    creative = {
        "object_story_spec": {
            "video_data": {
                "call_to_action": {
                    "type": "SIGN_UP",
                    "value": {"lead_gen_form_id": "1034662619343700"},
                }
            }
        }
    }
    assert extract_destination(creative) == (None, "lead_form")


def test_lead_ad_placeholder_link_is_not_reported_as_landing_page() -> None:
    """Lead ads carry a `http://fb.me/` placeholder that is not a destination."""
    creative = {
        "object_story_spec": {
            "link_data": {
                "link": "http://fb.me/",
                "call_to_action": {
                    "type": "SIGN_UP",
                    "value": {"lead_gen_form_id": "42"},
                },
            }
        }
    }
    assert extract_destination(creative) == (None, "lead_form")


def test_placeholder_link_without_lead_form_yields_nothing() -> None:
    creative = {"object_story_spec": {"link_data": {"link": "https://fb.me/"}}}
    assert extract_destination(creative) == (None, None)


def test_carousel_falls_back_to_first_card_link() -> None:
    creative = {
        "object_story_spec": {
            "link_data": {
                "child_attachments": [
                    {"link": LANDING_PAGE},
                    {"link": "https://www.nekt.com/other"},
                ]
            }
        }
    }
    assert extract_destination(creative) == (LANDING_PAGE, "website")


def test_link_url_is_reported_as_facebook_page() -> None:
    creative = {"link_url": "https://www.facebook.com/nekt/app_12345"}
    assert extract_destination(creative) == (
        "https://www.facebook.com/nekt/app_12345",
        "facebook_page",
    )


def test_object_url_fallback() -> None:
    creative = {"object_url": LANDING_PAGE}
    assert extract_destination(creative) == (LANDING_PAGE, "website")


def test_existing_organic_post_yields_no_destination() -> None:
    """Boosted posts keep the link on the post, not on the creative."""
    creative = {"effective_object_story_id": "357780360758431_122152919252394844"}
    assert extract_destination(creative) == (None, None)


@pytest.mark.parametrize(
    "creative",
    [
        {},
        {"object_story_spec": None},
        {"object_story_spec": {"link_data": None}},
        {"asset_feed_spec": {"link_urls": []}},
        {"asset_feed_spec": {"link_urls": [None]}},
        {"object_story_spec": {"link_data": {"link": "   "}}},
        {"object_story_spec": {"link_data": {"link": 12345}}},
    ],
)
def test_malformed_payloads_never_raise(creative: dict) -> None:
    assert extract_destination(creative) == (None, None)


def _creative_stream(config: dict | None = None) -> CreativeStream:
    tap = TapFacebook(config={**SAMPLE_CONFIG, **(config or {})}, validate_config=False)
    return tap.streams["creatives"]


def test_destination_source_fields_requested_in_both_modes() -> None:
    for mode, base in (("basic", BASIC_FIELDS), ("advanced", ADVANCED_FIELDS)):
        columns = _creative_stream({"creative_fields_mode": mode}).columns
        assert set(base).issubset(columns)
        assert set(DESTINATION_SOURCE_FIELDS).issubset(columns)


def test_get_records_emits_destination_and_drops_raw_specs() -> None:
    stream = _creative_stream()
    context = {
        "creative": {
            "id": "creative-1",
            "name": "Some ad",
            "object_story_spec": {"link_data": {"link": LANDING_PAGE}},
        },
        "ad_id": "ad-1",
        "ad_updated_time": "2026-08-01T12:00:00+0000",
    }

    (record,) = list(stream.get_records(context))

    assert record["destination_url"] == LANDING_PAGE
    assert record["destination_type"] == "website"
    assert record["ad_id"] == "ad-1"
    assert record["id"] == "creative-1"
    for field in DESTINATION_SOURCE_FIELDS:
        assert field not in record


def test_get_records_without_creative_context_yields_nothing() -> None:
    stream = _creative_stream()
    assert list(stream.get_records(None)) == []
    assert list(stream.get_records({"ad_id": "ad-1"})) == []

"""Unit tests for the ads stream degradation ladder.

These tests run fully offline (no Facebook credentials): every HTTP
interaction is mocked. They cover the automatic degradation ladder
(tracking split -> creative split -> page-size reduction -> two-phase mode),
the adaptive batch chunking, the pagination loop guard and the two-phase
record merging.
"""

from __future__ import annotations

import json
from unittest import mock

import pytest
import requests
from nekt_singer_sdk.exceptions import RetriableAPIError
from nekt_singer_sdk.streams import RESTStream

from tap_facebook.streams.ads import (
    BASE_AD_COLUMNS,
    MIN_ADS_PAGE_SIZE,
    AdsStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
}

DATA_LIMIT_ERROR_BODY = {
    "error": {
        "code": 1,
        "message": "Please reduce the amount of data you're asking for, then retry your request",
    }
}


def make_response(
    status: int = 200,
    body: dict | None = None,
    url: str = "https://graph.facebook.com/v24.0/act_123/ads?limit=100",
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body if body is not None else {}).encode()
    response.url = url
    response.reason = "OK" if status == 200 else "Error"
    return response


def make_stream(**config_overrides) -> AdsStream:
    tap = TapFacebook(config={**SAMPLE_CONFIG, **config_overrides})
    stream = tap.streams["ads"]
    # Avoid state machinery in get_url_params during unit tests.
    stream.forced_replication_method = "FULL_TABLE"
    return stream


class TestDegradationLadder:
    def test_full_ladder_order(self):
        stream = make_stream()

        assert stream._advance_degradation() is True
        assert stream._split_tracking_fields is True

        assert stream._advance_degradation() is True
        assert stream._split_creative_fields is True

        assert stream._advance_degradation() is True
        assert stream._reduced_page_size == MIN_ADS_PAGE_SIZE

        assert stream._advance_degradation() is True
        assert stream._two_phase_mode is True

        # Ladder exhausted.
        assert stream._advance_degradation() is False

    def test_each_rung_can_be_disabled_by_config(self):
        stream = make_stream(
            include_ads_tracking_fields=False,
            split_creative_on_error=False,
            ads_auto_reduce_page_size=False,
            ads_two_phase_on_error=False,
        )
        assert stream._advance_degradation() is False
        assert stream._split_tracking_fields is False
        assert stream._split_creative_fields is False
        assert stream._reduced_page_size is None
        assert stream._two_phase_mode is False

    def test_page_size_rung_skipped_when_already_at_floor(self):
        stream = make_stream(
            include_ads_tracking_fields=False,
            split_creative_on_error=False,
            ads_page_size="50",
        )
        # First applicable rung is two-phase: page size is already at the floor.
        assert stream._advance_degradation() is True
        assert stream._two_phase_mode is True
        assert stream._reduced_page_size is None

    def test_page_size_property_reflects_reduction(self):
        stream = make_stream()
        assert stream.page_size == 100
        stream._reduced_page_size = MIN_ADS_PAGE_SIZE
        assert stream.page_size == MIN_ADS_PAGE_SIZE

    def test_degradation_active_property(self):
        stream = make_stream()
        assert stream._degradation_active is False
        stream._split_tracking_fields = True
        assert stream._degradation_active is True


class TestPath:
    def test_default_path_includes_tracking_and_creative_expansion(self):
        stream = make_stream()
        assert "tracking_specs" in stream.path
        assert "creative.thumbnail_width" in stream.path

    def test_tracking_split_removes_tracking_fields(self):
        stream = make_stream()
        stream._split_tracking_fields = True
        assert "tracking_specs" not in stream.path
        assert "creative.thumbnail_width" in stream.path

    def test_creative_split_requests_bare_creative(self):
        stream = make_stream()
        stream._split_tracking_fields = True
        stream._split_creative_fields = True
        assert "creative.thumbnail_width" not in stream.path
        assert stream.path.endswith(",creative")

    def test_two_phase_path_is_minimal(self):
        stream = make_stream()
        stream._two_phase_mode = True
        assert stream.path == "/ads?fields=id,updated_time"


class TestValidateResponse:
    def test_records_error_code_and_raises_retriable(self):
        stream = make_stream()
        response = make_response(500, DATA_LIMIT_ERROR_BODY)
        with pytest.raises(RetriableAPIError):
            stream.validate_response(response)
        assert stream._last_error_code == 1

    def test_clears_error_code_on_success(self):
        stream = make_stream()
        stream._last_error_code = 1
        stream.validate_response(make_response(200, {"data": []}))
        assert stream._last_error_code is None

    def test_handles_non_json_500_body(self):
        stream = make_stream()
        response = make_response(500)
        response._content = b"<html>Internal Server Error</html>"
        with pytest.raises(RetriableAPIError):
            stream.validate_response(response)
        assert stream._last_error_code is None


class TestRequestLadderProgression:
    def _prepared_request(self) -> requests.PreparedRequest:
        return requests.Request(
            "GET",
            "https://graph.facebook.com/v24.0/act_123/ads",
            params={"limit": "100"},
        ).prepare()

    def test_advances_through_all_rungs_then_succeeds(self):
        stream = make_stream()
        success = make_response(200, {"data": []})
        calls = {"count": 0}

        def fake_request(self_, prepared_request, context):
            calls["count"] += 1
            if calls["count"] <= 4:
                self_._last_error_code = 1
                raise RetriableAPIError("data limit", make_response(500, DATA_LIMIT_ERROR_BODY))
            return success

        with (
            mock.patch.object(RESTStream, "_request", autospec=True, side_effect=fake_request),
            mock.patch("tap_facebook.streams.ads.time.sleep"),
        ):
            result = stream._request(self._prepared_request(), None)

        assert result is success
        assert calls["count"] == 5
        assert stream._split_tracking_fields is True
        assert stream._split_creative_fields is True
        assert stream._reduced_page_size == MIN_ADS_PAGE_SIZE
        assert stream._two_phase_mode is True

    def test_raises_after_ladder_exhausted(self):
        stream = make_stream()

        def always_fail(self_, prepared_request, context):
            self_._last_error_code = 1
            raise RetriableAPIError("data limit", make_response(500, DATA_LIMIT_ERROR_BODY))

        with (
            mock.patch.object(RESTStream, "_request", autospec=True, side_effect=always_fail),
            mock.patch("tap_facebook.streams.ads.time.sleep"),
        ):
            with pytest.raises(RetriableAPIError):
                stream._request(self._prepared_request(), None)

        # All rungs were tried before giving up.
        assert stream._two_phase_mode is True

    def test_does_not_degrade_on_other_retriable_errors(self):
        stream = make_stream()

        def server_error(self_, prepared_request, context):
            self_._last_error_code = 2
            raise RetriableAPIError("service unavailable", make_response(500, {"error": {"code": 2}}))

        with mock.patch.object(RESTStream, "_request", autospec=True, side_effect=server_error):
            with pytest.raises(RetriableAPIError):
                stream._request(self._prepared_request(), None)

        assert stream._degradation_active is False

    def test_rebuilds_url_when_degraded(self):
        stream = make_stream()
        stream._split_tracking_fields = True
        stream._split_creative_fields = True
        seen_urls = []

        def capture(self_, prepared_request, context):
            seen_urls.append(prepared_request.url)
            return make_response(200, {"data": []})

        with mock.patch.object(RESTStream, "_request", autospec=True, side_effect=capture):
            stream._request(self._prepared_request(), None)

        assert len(seen_urls) == 1
        assert "tracking_specs" not in seen_urls[0]
        assert "creative.thumbnail_width" not in seen_urls[0]


class TestBatchIdLookup:
    def test_happy_path_single_chunk(self):
        stream = make_stream()
        ids = ["a", "b", "c"]

        def ok(method, params=None, data=None, label=""):
            chunk = params["ids"].split(",")
            return make_response(200, {i: {"id": i} for i in chunk})

        stream._graph_batch_request = ok
        result = stream._batch_id_lookup(ids, params={"fields": "id"}, label="test")

        assert set(result) == {"a", "b", "c"}

    def test_splits_chunk_on_data_limit_error(self):
        stream = make_stream()
        ids = [f"ad{i}" for i in range(8)]
        chunk_sizes = []

        def limited(method, params=None, data=None, label=""):
            chunk = params["ids"].split(",")
            chunk_sizes.append(len(chunk))
            if len(chunk) > 2:
                return make_response(500, DATA_LIMIT_ERROR_BODY)
            return make_response(200, {i: {"id": i} for i in chunk})

        stream._graph_batch_request = limited
        result = stream._batch_id_lookup(ids, params={"fields": "id"}, label="test")

        assert set(result) == set(ids)
        # 8 fails -> [4, 4]; first 4 fails -> [2, 2] served before the second 4.
        assert chunk_sizes == [8, 4, 2, 2, 4, 2, 2]

    def test_skips_single_id_that_keeps_failing(self):
        stream = make_stream()
        ids = ["good1", "bad", "good2"]

        def poisoned(method, params=None, data=None, label=""):
            chunk = params["ids"].split(",")
            if "bad" in chunk:
                return make_response(500, DATA_LIMIT_ERROR_BODY)
            return make_response(200, {i: {"id": i} for i in chunk})

        stream._graph_batch_request = poisoned
        result = stream._batch_id_lookup(ids, params={"fields": "id"}, label="test")

        assert set(result) == {"good1", "good2"}

    def test_does_not_split_on_non_data_limit_errors(self):
        stream = make_stream()
        ids = ["a", "b", "c"]
        call_count = {"count": 0}

        def forbidden(method, params=None, data=None, label=""):
            call_count["count"] += 1
            return make_response(403, {"error": {"code": 200, "message": "permission"}})

        stream._graph_batch_request = forbidden
        result = stream._batch_id_lookup(ids, params={"fields": "id"}, label="test")

        assert result == {}
        assert call_count["count"] == 1  # chunk skipped, not split

    def test_skips_chunk_on_network_failure(self):
        stream = make_stream()

        stream._graph_batch_request = lambda *args, **kwargs: None
        result = stream._batch_id_lookup(["a", "b"], params={"fields": "id"}, label="test")

        assert result == {}

    def test_empty_ids(self):
        stream = make_stream()
        assert stream._batch_id_lookup([], params={}, label="test") == {}


class TestPagination:
    def test_returns_cursor_when_next_present(self):
        stream = make_stream()
        response = make_response(
            200,
            {"data": [], "paging": {"next": "https://next", "cursors": {"after": "abc"}}},
        )
        assert stream.get_next_page_token(response, previous_token=None) == "abc"

    def test_returns_none_on_last_page(self):
        stream = make_stream()
        response = make_response(200, {"data": [], "paging": {"cursors": {"after": "abc"}}})
        assert stream.get_next_page_token(response, previous_token="prev") is None

    def test_aborts_on_repeated_cursor(self):
        stream = make_stream()
        response = make_response(
            200,
            {"data": [], "paging": {"next": "https://next", "cursors": {"after": "abc"}}},
        )
        with pytest.raises(SystemExit):
            stream.get_next_page_token(response, previous_token="abc")


class TestParseResponse:
    def test_passthrough_without_degradation(self):
        stream = make_stream()
        records = [{"id": "a1", "name": "Ad 1"}, {"id": "a2", "name": "Ad 2"}]
        parsed = list(stream.parse_response(make_response(200, {"data": records})))
        assert parsed == records

    def test_split_mode_enriches_tracking_and_creative(self):
        stream = make_stream()
        stream._split_tracking_fields = True
        stream._split_creative_fields = True
        stream._fetch_tracking_fields = lambda ids: {"a1": {"tracking_specs": [{"page": ["1"]}]}}
        stream._fetch_creative_fields = lambda records: {"a1": {"id": "c1", "name": "Creative 1"}}

        records = [{"id": "a1", "creative": {"id": "c1"}}, {"id": "a2", "creative": {"id": "c2"}}]
        parsed = list(stream.parse_response(make_response(200, {"data": records})))

        assert parsed[0]["tracking_specs"] == [{"page": ["1"]}]
        assert parsed[0]["creative"] == {"id": "c1", "name": "Creative 1"}
        # a2's creative lookup failed: keeps the bare reference.
        assert parsed[1]["creative"] == {"id": "c2"}

    def test_two_phase_merges_base_fields_and_full_creative(self):
        stream = make_stream()
        stream._split_tracking_fields = True
        stream._split_creative_fields = True
        stream._two_phase_mode = True
        stream._fetch_base_fields = lambda ids: {
            "a1": {"id": "a1", "name": "Ad 1", "status": "ACTIVE", "creative": {"id": "c1"}},
        }
        stream._fetch_tracking_fields = lambda ids: {}
        stream._fetch_creative_fields = lambda records: {"a1": {"id": "c1", "name": "Creative 1"}}

        listing = [
            {"id": "a1", "updated_time": "2026-07-01T00:00:00+0000"},
            {"id": "a2", "updated_time": "2026-07-02T00:00:00+0000"},
        ]
        parsed = list(stream.parse_response(make_response(200, {"data": listing})))

        # a1 merged listing + base fields + full creative.
        assert len(parsed) == 1
        assert parsed[0]["id"] == "a1"
        assert parsed[0]["updated_time"] == "2026-07-01T00:00:00+0000"
        assert parsed[0]["name"] == "Ad 1"
        assert parsed[0]["creative"] == {"id": "c1", "name": "Creative 1"}
        # a2 had no base data: skipped instead of emitting a skeleton row.

    def test_two_phase_fetches_creatives_even_without_split_flag(self):
        stream = make_stream()
        stream._two_phase_mode = True
        stream._fetch_base_fields = lambda ids: {
            "a1": {"id": "a1", "creative": {"id": "c1"}},
        }
        fetched = {"called": False}

        def fetch_creatives(records):
            fetched["called"] = True
            return {"a1": {"id": "c1", "name": "Creative 1"}}

        stream._fetch_creative_fields = fetch_creatives
        listing = [{"id": "a1", "updated_time": "2026-07-01T00:00:00+0000"}]
        parsed = list(stream.parse_response(make_response(200, {"data": listing})))

        assert fetched["called"] is True
        assert parsed[0]["creative"] == {"id": "c1", "name": "Creative 1"}


class TestRequestRecordsIntegration:
    """Drive the real SDK request loop (request_records) against a mocked
    HTTP session: first response is a data-limit 500, the ladder activates,
    and pagination proceeds to the end."""

    def test_degrades_then_paginates_to_completion(self):
        stream = make_stream()
        responses = [
            make_response(500, DATA_LIMIT_ERROR_BODY),
            make_response(
                200,
                {
                    "data": [{"id": "a1", "updated_time": "2026-07-01T00:00:00+0000"}],
                    "paging": {"next": "https://next", "cursors": {"after": "cur1"}},
                },
            ),
            make_response(
                200,
                {
                    "data": [{"id": "a2", "updated_time": "2026-07-02T00:00:00+0000"}],
                    "paging": {"cursors": {"after": "cur2"}},
                },
            ),
        ]
        sent_urls = []

        def fake_send(prepared_request, **kwargs):
            sent_urls.append(prepared_request.url)
            return responses.pop(0)

        stream._fetch_tracking_fields = lambda ids: {}
        with (
            mock.patch.object(stream.requests_session, "send", side_effect=fake_send),
            mock.patch("tap_facebook.streams.ads.time.sleep"),
        ):
            records = list(stream.request_records(context=None))

        assert [r["id"] for r in records] == ["a1", "a2"]
        assert stream._split_tracking_fields is True
        # After the first 500, every request runs without tracking fields.
        assert all("tracking_specs" not in url for url in sent_urls[1:])
        assert len(sent_urls) == 3


class TestSchemaCompatibility:
    def test_base_columns_match_schema_properties(self):
        """Every base column must exist in the declared schema (drop-in deploy)."""
        stream = make_stream()
        schema_props = set(stream.schema["properties"])
        for column in BASE_AD_COLUMNS:
            if column in {"bid_info", "ad_acive_time"}:
                continue  # requested from the API but intentionally not in the schema
            assert column in schema_props, f"{column} missing from schema"

    def test_primary_and_replication_keys_unchanged(self):
        stream = make_stream()
        assert stream.primary_keys == ["id", "updated_time"]
        assert stream.replication_key == "updated_time"

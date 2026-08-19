"""Unit tests for the insights async-job polling loop.

These tests run fully offline (no Facebook credentials): the AdReportRun
instance is mocked. They cover the defensive retry around api_get() added
for the case where Meta answers a poll with a non-JSON body (e.g. an
edge/CDN error page) and the facebook-business SDK crashes inside its
parser with a plain TypeError instead of raising a FacebookRequestError
(NEKT-4724).
"""

from __future__ import annotations

from unittest import mock

import pytest
from facebook_business.exceptions import FacebookRequestError

from tap_facebook.streams.ad_insights import (
    MAX_CONSECUTIVE_POLL_FAILURES,
    AdsInsightStream,
)
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "enable_advanced_reports": True,
}

COMPLETED_JOB = {
    "async_status": "Job Completed",
    "async_percent_completion": 100,
    "id": "report-1",
}

PARSER_CRASH = TypeError("string indices must be integers, not 'str'")


def make_stream() -> AdsInsightStream:
    tap = TapFacebook(config=SAMPLE_CONFIG)
    return tap.streams["adsinsights"]


def make_facebook_request_error() -> FacebookRequestError:
    return FacebookRequestError(
        message="Call was not successful",
        request_context={},
        http_status=500,
        http_headers={},
        body='{"error": {"code": 2, "message": "Service temporarily unavailable"}}',
    )


@mock.patch("tap_facebook.streams.ad_insights.time.sleep")
class TestPollingRetry:
    def test_happy_path_returns_job(self, mock_sleep):
        stream = make_stream()
        report = mock.Mock()
        report.api_get.return_value = COMPLETED_JOB

        job = stream._run_job_to_completion(report_instance=report, report_date="2024-01-01")

        assert job == COMPLETED_JOB
        assert report.api_get.call_count == 1

    def test_transient_parser_crash_is_retried(self, mock_sleep):
        stream = make_stream()
        report = mock.Mock()
        report.api_get.side_effect = [PARSER_CRASH, PARSER_CRASH, COMPLETED_JOB]

        job = stream._run_job_to_completion(report_instance=report, report_date="2024-01-01")

        assert job == COMPLETED_JOB
        assert report.api_get.call_count == 3

    def test_persistent_parser_crash_gives_up_without_raising(self, mock_sleep):
        stream = make_stream()
        report = mock.Mock()
        report.api_get.side_effect = PARSER_CRASH

        job = stream._run_job_to_completion(report_instance=report, report_date="2024-01-01")

        # None feeds the report-retry ladder in _process_report_batch.
        assert job is None
        assert report.api_get.call_count == MAX_CONSECUTIVE_POLL_FAILURES

    def test_failure_counter_resets_after_successful_poll(self, mock_sleep):
        stream = make_stream()
        report = mock.Mock()
        running_job = {
            "async_status": "Job Running",
            "async_percent_completion": 50,
            "id": "report-1",
        }
        # One failure short of the cap, a good poll, then a fresh failure
        # streak: the counter must have reset, so the job still completes.
        report.api_get.side_effect = (
            [PARSER_CRASH] * (MAX_CONSECUTIVE_POLL_FAILURES - 1)
            + [running_job]
            + [PARSER_CRASH] * (MAX_CONSECUTIVE_POLL_FAILURES - 1)
            + [COMPLETED_JOB]
        )

        job = stream._run_job_to_completion(report_instance=report, report_date="2024-01-01")

        assert job == COMPLETED_JOB

    def test_facebook_request_error_propagates(self, mock_sleep):
        stream = make_stream()
        report = mock.Mock()
        report.api_get.side_effect = make_facebook_request_error()

        with pytest.raises(FacebookRequestError):
            stream._run_job_to_completion(report_instance=report, report_date="2024-01-01")

    def test_malformed_job_payload_is_retried(self, mock_sleep):
        stream = make_stream()
        report = mock.Mock()
        # JSON body without the async fields: the status lookup raises KeyError.
        report.api_get.side_effect = [{"unexpected": "shape"}, COMPLETED_JOB]

        job = stream._run_job_to_completion(report_instance=report, report_date="2024-01-01")

        assert job == COMPLETED_JOB
        assert report.api_get.call_count == 2

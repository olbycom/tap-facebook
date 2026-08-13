"""Stream class for Ad Account Activities including Billing Charges."""

from __future__ import annotations

import time
import typing as t
from http import HTTPStatus
from urllib.parse import parse_qs, urlparse

import pendulum
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.exceptions import RetriableAPIError
from nekt_singer_sdk.helpers import types
from nekt_singer_sdk.typing import NumberType, PropertiesList, Property, StringType

from tap_facebook.client import FacebookStream

if t.TYPE_CHECKING:
    import requests

# The stream promises a rolling window of recent activity, not the account's
# full history — without a `since` filter Facebook paginates the entire
# history, and on high-activity accounts the deep cursors deterministically
# fail with error code 1 / subcode 99 (NEKT-4611).
ACTIVITIES_WINDOW_DAYS = 7
DEFAULT_PAGE_SIZE = 25
MIN_PAGE_SIZE = 1
DEGRADATION_RETRY_SLEEP_SECONDS = 5


class ActivitiesStream(FacebookStream):
    """Fetch account activities from Facebook Ads, including billing charges.

    API Reference:
    https://developers.facebook.com/docs/marketing-api/reference/ad-account/activities/

    This stream returns account activities. Billing-related event types include:
    - ad_account_billing_charge (charges made to credit card)
    - ad_account_billing_charge_failed
    - ad_account_billing_refund

    To get billing data, we fetch all activities and the event_type field will
    contain billing event types. The extra_data field contains transaction details.
    """

    columns = [  # noqa: RUF012
        "event_time",
        "event_type",
        "extra_data",
        "actor_id",
        "actor_name",
    ]

    name = "activities"
    path = f"/activities?fields={','.join(columns)}"
    tap_stream_id = "activities"
    primary_keys = ["event_time", "event_type", "actor_id"]  # noqa: RUF012
    replication_key = "event_time"

    _reduced_page_size: int | None = None
    _last_error_code: int | None = None
    _window_start_ts: int | None = None

    schema = PropertiesList(
        Property(
            "event_time",
            StringType,
            description="The time when the event occurred (ISO 8601 format)",
        ),
        Property(
            "event_type",
            StringType,
            description="The type of event (e.g., ad_account_billing_charge, ad_account_billing_refund)",
        ),
        Property(
            "extra_data",
            StringType,
            description="Additional data about the event, typically includes transaction value in JSON format",
        ),
        Property(
            "actor_id",
            StringType,
            description="ID of the user/entity who triggered the activity",
        ),
        Property(
            "actor_name",
            StringType,
            description="Name of the user/entity who triggered the activity",
        ),
        Property(
            "charge_amount",
            NumberType,
            description="Extracted charge amount from extra_data (if available and event is billing-related)",
        ),
        Property(
            "currency",
            StringType,
            description="Currency code extracted from extra_data (if available)",
        ),
        Property(
            "is_billing_event",
            StringType,
            description="Boolean flag indicating if this is a billing-related event",
        ),
    ).to_dict()

    @property
    def page_size(self) -> int:
        if self._reduced_page_size is not None:
            return self._reduced_page_size
        return DEFAULT_PAGE_SIZE

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        params = super().get_url_params(context, next_page_token)
        # Computed once per sync (in `sync`) so every page of one run queries
        # the same window — moving `since` mid-pagination would invalidate the
        # cursor.
        if self._window_start_ts is not None:
            params["since"] = self._window_start_ts
        return params

    def validate_response(self, response: requests.Response) -> None:
        # Record the Facebook error code so _request can shrink the page size
        # when the retriable error surfaces.
        self._last_error_code = None
        if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            try:
                self._last_error_code = response.json().get("error", {}).get("code")
            except Exception:
                self._last_error_code = None
        super().validate_response(response)

    def _reduce_page_size(self) -> bool:
        """Halve the page size after Facebook's error code 1.

        Returns False once the minimum is reached, letting the caller fall
        back to the regular backoff retries.
        """
        if self.page_size <= MIN_PAGE_SIZE:
            return False
        new_size = max(self.page_size // 2, MIN_PAGE_SIZE)
        user_logger.info(
            f"[{self.name}] Facebook rejected the request as too heavy — "
            f"retrying with smaller pages ({new_size} records per request). "
            "The extraction continues; it may just take a bit longer."
        )
        internal_logger.warning(
            f"[{self.name}] Graph error code 1 on /activities; reducing page "
            f"size {self.page_size} -> {new_size} and retrying the same cursor."
        )
        self._reduced_page_size = new_size
        return True

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: dict | None,
    ) -> requests.Response:
        while True:
            if self._reduced_page_size is not None:
                qs = parse_qs(urlparse(prepared_request.url).query)
                cursor = qs.get("after", [None])[0]
                new_params = self.get_url_params(context, cursor)
                prepared_request.prepare_url(self.url_base + self.path, new_params)
            try:
                return super()._request(prepared_request, context)
            except RetriableAPIError:
                if self._last_error_code == 1 and self._reduce_page_size():
                    # A smaller page was activated: retry right away instead
                    # of burning the SDK's backoff budget on a payload
                    # Facebook already refused.
                    time.sleep(DEGRADATION_RETRY_SLEEP_SECONDS)
                    continue
                raise

    def sync(self, context: types.Context | None = None) -> None:
        window_start = pendulum.now("UTC").subtract(days=ACTIVITIES_WINDOW_DAYS)
        self._window_start_ts = int(window_start.timestamp())
        user_logger.info(f"[{self.name}] Retrieving activities from the last {ACTIVITIES_WINDOW_DAYS} days.")
        internal_logger.info(
            f"[{self.name}] Starting sync with since={self._window_start_ts} "
            f"({window_start.isoformat()}), page_size={self.page_size}."
        )
        return super().sync(context)

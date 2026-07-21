"""Stream class for AdsStream."""

from __future__ import annotations

import json
import re
import time
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Dict
from urllib.parse import parse_qs, urlparse

import requests
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.streams.core import REPLICATION_INCREMENTAL
from nekt_singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import IncrementalFacebookStream

if TYPE_CHECKING:
    from tap_facebook.streams.creative import CreativeStream


class AdsStream(IncrementalFacebookStream):
    """Ads stream class.

    columns: columns which will be added to fields parameter in api
    name: stream name
    account_id: facebook account
    path: path which will be added to api url in client.py
    schema: instream schema
    tap_stream_id = stream id.
    """

    name = "ads"
    filter_entity = "ad"
    _split_tracking_fields: bool = False
    _split_creative_fields: bool = False

    @property
    def path(self) -> str:
        base_columns = [
            "id",
            "account_id",
            "adset_id",
            "campaign_id",
            "bid_type",
            "bid_info",
            "status",
            "updated_time",
            "created_time",
            "name",
            "effective_status",
            "last_updated_by_app_id",
            "source_ad_id",
            "configured_status",
            "conversion_domain",
            "bid_amount",
        ]

        tracking_fields = []
        if not self._split_tracking_fields and self.config.get("include_ads_tracking_fields", True):
            tracking_fields = ["tracking_specs", "conversion_specs", "recommendations"]

        columns = [*base_columns, *tracking_fields]

        if "creatives" in self._tap.streams and not self._split_creative_fields:
            creative_stream: CreativeStream = self._tap.streams["creatives"]
            thumbnail_width = self.config.get("creative_thumbnail_width", 1024)
            thumbnail_height = self.config.get("creative_thumbnail_height", 1024)
            return (
                f"/ads?fields={','.join(columns)},"
                f"creative.thumbnail_width({thumbnail_width}).thumbnail_height({thumbnail_height}){{{','.join(creative_stream.columns)}}}"
            )
        # Either no creatives stream selected, or _split_creative_fields is active:
        # request only the creative id here and fetch full fields separately
        # (see _fetch_creative_fields) to keep the /ads payload small.
        return f"/ads?fields={','.join([*columns, 'creative'])}"

    primary_keys = ["id", "updated_time"]  # noqa: RUF012
    replication_key = "updated_time"

    schema = PropertiesList(
        Property(
            "bid_type",
            StringType,
            description="The bid type for this ad (e.g. CPC, CPM)",
        ),
        Property(
            "account_id",
            StringType,
            description="ID of the ad account this ad belongs to",
        ),
        Property(
            "ad_acive_time",
            StringType,
            description="Time the ad was active",
        ),
        Property(
            "ad_schedule_end_time",
            DateTimeType,
            description="Scheduled end time for the ad",
        ),
        Property(
            "ad_schedule_start_time",
            DateTimeType,
            description="Scheduled start time for the ad",
        ),
        Property(
            "campaign_id",
            StringType,
            description="ID of the campaign this ad belongs to",
        ),
        Property(
            "adset_id",
            StringType,
            description="ID of the ad set this ad belongs to",
        ),
        Property(
            "bid_amount",
            IntegerType,
            description="Bid amount in the smallest currency unit (e.g. cents)",
        ),
        Property(
            "status",
            StringType,
            description="Configured status of the ad (ACTIVE, PAUSED, etc.)",
        ),
        Property(
            "creative",
            ObjectType(
                Property("creative_id", StringType, description="ID of the creative"),
                Property("id", StringType, description="Creative node ID"),
            ),
            description="Creative object defining the ad's appearance and content",
        ),
        Property(
            "id",
            StringType,
            description="Unique ID for the ad",
        ),
        Property(
            "updated_time",
            StringType,
            description="When the ad was last updated (ISO 8601)",
        ),
        Property(
            "created_time",
            StringType,
            description="When the ad was created (ISO 8601)",
        ),
        Property(
            "conversion_domain",
            StringType,
            description="Domain for conversion tracking",
        ),
        Property(
            "name",
            StringType,
            description="Name of the ad",
        ),
        Property(
            "effective_status",
            StringType,
            description="Effective status considering parent campaign/ad set status",
        ),
        Property(
            "last_updated_by_app_id",
            StringType,
            description="ID of the app that last updated the ad",
        ),
        Property(
            "recommendations",
            ArrayType(
                ObjectType(
                    Property("blame_field", StringType, description="Field the recommendation blames"),
                    Property("code", IntegerType, description="Recommendation code"),
                    Property("confidence", StringType, description="Confidence level of the recommendation"),
                    Property("importance", StringType, description="Importance of the recommendation"),
                    Property("message", StringType, description="Recommendation message"),
                    Property("title", StringType, description="Recommendation title"),
                ),
            ),
            description="Recommendations to improve ad performance",
        ),
        Property(
            "source_ad_id",
            StringType,
            description="ID of the source ad if this ad was copied",
        ),
        Property(
            "tracking_specs",
            ArrayType(
                ObjectType(
                    Property(
                        "application",
                        ArrayType(Property("items", StringType, description="Application tracking item")),
                        description="Application tracking spec",
                    ),
                    Property("post", ArrayType(StringType), description="Post tracking spec"),
                    Property("conversion_id", ArrayType(StringType), description="Conversion ID tracking spec"),
                    Property("action_type", ArrayType(StringType), description="Action type tracking spec"),
                    Property("post_type", ArrayType(StringType), description="Post type tracking spec"),
                    Property("page", ArrayType(StringType), description="Page tracking spec"),
                    Property("creative", ArrayType(StringType), description="Creative tracking spec"),
                    Property("dataset", ArrayType(StringType), description="Dataset tracking spec"),
                    Property("event", ArrayType(StringType), description="Event tracking spec"),
                    Property("event_creator", ArrayType(StringType), description="Event creator tracking spec"),
                    Property("event_type", ArrayType(StringType), description="Event type tracking spec"),
                    Property("fb_pixel", ArrayType(StringType), description="Facebook Pixel tracking spec"),
                    Property(
                        "fb_pixel_event",
                        ArrayType(StringType),
                        description="Facebook Pixel event tracking spec",
                    ),
                    Property("leadgen", ArrayType(StringType), description="Lead gen tracking spec"),
                    Property("object", ArrayType(StringType), description="Object tracking spec"),
                    Property("object_domain", ArrayType(StringType), description="Object domain tracking spec"),
                    Property("offer", ArrayType(StringType), description="Offer tracking spec"),
                    Property("offer_creator", ArrayType(StringType), description="Offer creator tracking spec"),
                    Property("offsite_pixel", ArrayType(StringType), description="Offsite pixel tracking spec"),
                    Property("page_parent", ArrayType(StringType), description="Page parent tracking spec"),
                    Property("post_object", ArrayType(StringType), description="Post object tracking spec"),
                    Property("question", ArrayType(StringType), description="Question tracking spec"),
                    Property(
                        "post_object_wall",
                        ArrayType(StringType),
                        description="Post object wall tracking spec",
                    ),
                    Property(
                        "post_wall",
                        ArrayType(StringType),
                        description="Post wall tracking spec",
                    ),
                    Property(
                        "question_creator",
                        ArrayType(StringType),
                        description="Question creator tracking spec",
                    ),
                    Property("response", ArrayType(StringType), description="Response tracking spec"),
                    Property("subtype", ArrayType(StringType), description="Subtype tracking spec"),
                ),
            ),
            description="Tracking specifications for conversion attribution",
        ),
        Property(
            "conversion_specs",
            ArrayType(
                ObjectType(
                    Property("action_type", ArrayType(StringType), description="Conversion action type"),
                    Property("conversion_id", ArrayType(StringType), description="Conversion ID"),
                ),
            ),
            description="Conversion specifications for optimization",
        ),
        Property(
            "configured_status",
            StringType,
            description="User-configured status of the ad",
        ),
        Property(
            "preview_shareable_link",
            StringType,
            description="Official Facebook ad preview URL (facebook.com/ads/api/preview_iframe.php?...). Only populated when include_ad_preview_link is enabled.",
        ),
    ).to_dict()

    tap_stream_id = "ads"

    @property
    def page_size(self) -> int:
        return int(self.config.get("ads_page_size", "100"))

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            try:
                error_code = response.json().get("error", {}).get("code")
                include_tracking = self.config.get("include_ads_tracking_fields", True)
                if error_code == 1:
                    if include_tracking and not self._split_tracking_fields:
                        # First degradation step: tracking fields are the usual
                        # culprit and cheapest to drop, so try that first.
                        self._split_tracking_fields = True
                        user_logger.info(
                            f"[{self.name}] Facebook error code 1 — switching to "
                            "split-request mode (fetching tracking fields "
                            "separately to avoid payload size limit)"
                        )
                    elif (
                        self.config.get("split_creative_on_error", True)
                        and "creatives" in self._tap.streams
                        and not self._split_creative_fields
                    ):
                        # Tracking fields are already excluded (by config or by
                        # the step above) and the error persists: the remaining
                        # payload weight is the inline creative expansion, so
                        # split that out too.
                        self._split_creative_fields = True
                        user_logger.info(
                            f"[{self.name}] Facebook error code 1 persists — "
                            "switching to split-request mode for creative "
                            "fields as well (fetching creative details "
                            "separately)"
                        )
            except Exception:
                pass
        super().validate_response(response)

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: dict | None,
    ) -> requests.Response:
        if self._split_tracking_fields or self._split_creative_fields:
            qs = parse_qs(urlparse(prepared_request.url).query)
            cursor = qs.get("after", [None])[0]
            new_params = self.get_url_params(context, cursor)
            prepared_request.prepare_url(self.url_base + self.path, new_params)
        return super()._request(prepared_request, context)

    def parse_response(self, response: requests.Response) -> Any:
        include_preview = self.config.get("include_ad_preview_link", False)
        split_active = self._split_tracking_fields or self._split_creative_fields
        if split_active and response.status_code == HTTPStatus.OK:
            records = response.json().get("data", [])
            ad_ids = [r["id"] for r in records if "id" in r]
            tracking_data = {}
            if self._split_tracking_fields:
                tracking_data = self._fetch_tracking_fields(ad_ids)
            creative_data = {}
            if self._split_creative_fields:
                creative_data = self._fetch_creative_fields(records)
            preview_data = self._fetch_preview_links(ad_ids) if include_preview else {}
            for record in records:
                ad_id = record.get("id")
                record.update(tracking_data.get(ad_id, {}))
                if ad_id in creative_data:
                    record["creative"] = creative_data[ad_id]
                if include_preview:
                    record["preview_shareable_link"] = preview_data.get(ad_id)
                yield record
        else:
            if include_preview:
                records = list(super().parse_response(response))
                ad_ids = [r["id"] for r in records if "id" in r]
                preview_data = self._fetch_preview_links(ad_ids)
                for record in records:
                    record["preview_shareable_link"] = preview_data.get(record.get("id"))
                    yield record
            else:
                yield from super().parse_response(response)

    _GRAPH_HELPER_MAX_ATTEMPTS = 3
    _GRAPH_HELPER_TIMEOUT = 60

    def _redact_token(self, text: str) -> str:
        """Strip the access token from text destined for logs."""
        token = self.config.get("access_token")
        if token:
            text = text.replace(token, "***")
        return re.sub(r"access_token=[^&\s\"']+", "access_token=***", text)

    def _redacted_response_body(self, response: requests.Response, limit: int = 500) -> str:
        """Return the response body redacted and truncated for logging."""
        body = self._redact_token(response.text or "")
        return body[:limit] + ("…" if len(body) > limit else "")

    def _graph_batch_request(
        self,
        method: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
        label: str,
    ) -> requests.Response | None:
        """Call the Graph API root endpoint with timeout and network-error retries.

        The split-request helpers run outside the SDK request machinery, so
        connection errors here would otherwise crash the whole sync. Retries
        with exponential backoff and returns None once attempts are exhausted,
        letting callers skip the chunk instead of failing the run.
        """
        version = self.config["api_version"]
        url = f"https://graph.facebook.com/{version}/"
        for attempt in range(1, self._GRAPH_HELPER_MAX_ATTEMPTS + 1):
            try:
                return requests.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    timeout=self._GRAPH_HELPER_TIMEOUT,
                )
            except requests.exceptions.RequestException as exc:
                error = self._redact_token(f"{type(exc).__name__}: {exc}")
                if attempt == self._GRAPH_HELPER_MAX_ATTEMPTS:
                    user_logger.warning(
                        f"[{self.name}] {label}: request failed after "
                        f"{attempt} attempts ({error}) — skipping this chunk"
                    )
                    return None
                wait = 2**attempt
                user_logger.warning(
                    f"[{self.name}] {label}: request error ({error}) — "
                    f"retrying in {wait}s (attempt {attempt}/{self._GRAPH_HELPER_MAX_ATTEMPTS})"
                )
                time.sleep(wait)
        return None

    def _fetch_tracking_fields(self, ad_ids: list[str]) -> dict[str, dict]:
        """Batch-fetch tracking_specs, conversion_specs and recommendations for ad IDs.

        Uses the Facebook batch ID lookup endpoint, chunked to 50 ids per call
        (the endpoint's limit):
        GET /{version}/?ids=id1,id2,...&fields=tracking_specs,conversion_specs,recommendations
        Returns a dict keyed by ad ID. Ads whose chunk failed are simply absent
        from the result (tracking fields will be null for those rows on this page).
        """
        if not ad_ids:
            return {}
        result: dict[str, dict] = {}
        for chunk in [ad_ids[i : i + 50] for i in range(0, len(ad_ids), 50)]:
            resp = self._graph_batch_request(
                "get",
                params={
                    "ids": ",".join(chunk),
                    "fields": "tracking_specs,conversion_specs,recommendations",
                    "access_token": self.config["access_token"],
                },
                label="tracking fields",
            )
            if resp is None:
                continue
            if resp.status_code != HTTPStatus.OK:
                user_logger.warning(
                    f"[{self.name}] Failed to fetch tracking fields for "
                    f"{len(chunk)} ads (status {resp.status_code}) — tracking "
                    "fields will be null for this chunk. "
                    f"Response: {self._redacted_response_body(resp)}"
                )
                continue
            result.update(resp.json())
        return result

    def _fetch_creative_fields(self, records: list[dict]) -> dict[str, dict]:
        """Batch-fetch full creative field data for a page of split-creative ads.

        When `_split_creative_fields` is active, `path` only requests the bare
        `creative` field on each ad, so each record's `creative` is just
        `{"id": ...}`. This fetches the full set of creative columns (matching
        what `creatives` stream needs, based on `creative_fields_mode`) via the
        Facebook batch ID lookup endpoint, keyed by creative ID, chunked to 50
        ids per call, then remaps the result back to ad ID so callers can
        replace `record["creative"]` with the complete object — matching what
        the inline expansion used to return, so `get_child_context` keeps
        feeding `creatives` the same data as before.

        Returns a dict keyed by ad ID. Ads whose creative could not be fetched
        are simply absent from the result (callers should keep the bare
        {"id": ...} in that case).
        """
        creative_id_by_ad: dict[str, str] = {}
        for record in records:
            creative = record.get("creative")
            if isinstance(creative, dict) and creative.get("id") and record.get("id"):
                creative_id_by_ad[record["id"]] = creative["id"]
        if not creative_id_by_ad:
            return {}

        columns: list[str] = []
        if "creatives" in self._tap.streams:
            creative_stream: CreativeStream = self._tap.streams["creatives"]
            columns = creative_stream.columns
        fields = ",".join([*columns, "id"])
        thumbnail_width = self.config.get("creative_thumbnail_width", 1024)
        thumbnail_height = self.config.get("creative_thumbnail_height", 1024)

        creative_ids = list(dict.fromkeys(creative_id_by_ad.values()))
        id_chunks = [creative_ids[i : i + 50] for i in range(0, len(creative_ids), 50)]
        creatives_by_id: dict[str, dict] = {}
        for chunk in id_chunks:
            resp = self._graph_batch_request(
                "get",
                params={
                    "ids": ",".join(chunk),
                    "fields": fields,
                    "thumbnail_width": thumbnail_width,
                    "thumbnail_height": thumbnail_height,
                    "access_token": self.config["access_token"],
                },
                label="creative fields",
            )
            if resp is None:
                continue
            if resp.status_code != HTTPStatus.OK:
                user_logger.warning(
                    f"[{self.name}] Failed to fetch creative fields for "
                    f"{len(chunk)} creatives (status {resp.status_code}) — "
                    "creative fields will be incomplete for this chunk. "
                    f"Response: {self._redacted_response_body(resp)}"
                )
                continue
            creatives_by_id.update(resp.json())

        return {
            ad_id: creatives_by_id[creative_id]
            for ad_id, creative_id in creative_id_by_ad.items()
            if creative_id in creatives_by_id
        }

    def _fetch_preview_links(self, ad_ids: list[str]) -> dict[str, str | None]:
        """Batch-fetch preview URLs for a list of ad IDs via the Facebook Batch API.

        Calls /{ad_id}/previews?ad_format=FORMAT for each ad in chunks of 50,
        then extracts the preview URL from the src attribute of the iframe body.
        Returns a dict keyed by ad ID (value is None if no preview is available).
        """
        if not ad_ids:
            return {}
        ad_format = self.config.get("preview_ad_format", "DESKTOP_FEED_STANDARD")
        result: dict[str, str | None] = {}
        for chunk in [ad_ids[i : i + 50] for i in range(0, len(ad_ids), 50)]:
            batch = [
                {"method": "GET", "relative_url": f"{ad_id}/previews?ad_format={ad_format}"}
                for ad_id in chunk
            ]
            resp = self._graph_batch_request(
                "post",
                data={"batch": json.dumps(batch), "access_token": self.config["access_token"]},
                label="preview links",
            )
            if resp is None:
                for ad_id in chunk:
                    result[ad_id] = None
                continue
            if resp.status_code != HTTPStatus.OK:
                user_logger.warning(
                    f"[{self.name}] Failed to fetch preview links (status {resp.status_code}) "
                    "— preview_shareable_link will be null for this page. "
                    f"Response: {self._redacted_response_body(resp)}"
                )
                continue
            for ad_id, batch_item in zip(chunk, resp.json()):
                if not batch_item or batch_item.get("code") != 200:
                    result[ad_id] = None
                    continue
                body_json = json.loads(batch_item["body"])
                data = body_json.get("data", [])
                if not data:
                    result[ad_id] = None
                    continue
                iframe_body = data[0].get("body", "")
                match = re.search(r'src="([^"]+)"', iframe_body)
                result[ad_id] = match.group(1).replace("&amp;", "&") if match else None
        return result

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return next page token, using paging.next to detect the last page.

        Facebook cursor pagination can loop indefinitely at small page sizes
        because paging.cursors.after is always present, even on the last page.
        At page_size >= 50 (we use 100), paging.next reliably indicates whether
        more pages exist.
        """
        resp_json = response.json()
        paging = resp_json.get("paging", {})
        if "next" not in paging:
            return None
        return paging.get("cursors", {}).get("after")

    def sanitize_field_names(self, record):
        if isinstance(record, dict):
            updated_record = {}
            for key, value in record.items():
                new_key = key.replace(".", "_")
                updated_record[new_key] = self.sanitize_field_names(value)
            return updated_record
        elif isinstance(record, list):
            return [self.sanitize_field_names(item) for item in record]
        else:
            return record

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Provide context for child streams (creatives)."""
        creative_data = record.get("creative")
        if creative_data and isinstance(creative_data, dict):
            return {
                "creative": creative_data,
                "ad_id": record.get("id"),
                "ad_updated_time": record.get("updated_time"),
            }
        return None

    def post_process(self, row: Dict[str, Any], context: Dict | None = None) -> dict | None:
        return super().post_process(self.sanitize_field_names(row), context)

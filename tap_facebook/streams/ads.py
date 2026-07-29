"""Stream class for AdsStream."""

from __future__ import annotations

import json
import re
import sys
import time
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Dict
from urllib.parse import parse_qs, urlparse

import requests
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.exceptions import RetriableAPIError
from nekt_singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.client import API_VERSION, IncrementalFacebookStream

if TYPE_CHECKING:
    from tap_facebook.streams.creative import CreativeStream

# Fields requested inline on /ads. Tracking fields and the creative expansion
# are layered on top of these depending on the degradation state.
BASE_AD_COLUMNS = [
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

# Facebook cursor pagination is unreliable below 50 (paging.next may be absent
# or loop), so the automatic ladder never reduces the page size further.
MIN_ADS_PAGE_SIZE = 50

DEGRADATION_RETRY_SLEEP_SECONDS = 5


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

    # Degradation ladder state. Reset at the start of every run — each run
    # starts optimistic and descends only as far as Facebook's error code 1
    # ("Please reduce the amount of data you're asking for") forces it to.
    _split_tracking_fields: bool = False
    _split_creative_fields: bool = False
    _reduced_page_size: int | None = None
    _two_phase_mode: bool = False
    _last_error_code: int | None = None

    @property
    def path(self) -> str:
        if self._two_phase_mode:
            # Ultra-minimal listing: only the id and the replication key. All
            # remaining fields are batch-fetched by id in parse_response (see
            # _fetch_base_fields).
            return "/ads?fields=id,updated_time"

        tracking_fields = []
        if not self._split_tracking_fields and self.config.get("include_ads_tracking_fields", True):
            tracking_fields = ["tracking_specs", "conversion_specs", "recommendations"]

        columns = [*BASE_AD_COLUMNS, *tracking_fields]

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
        if self._reduced_page_size is not None:
            return self._reduced_page_size
        return int(self.config.get("ads_page_size", "100"))

    @property
    def _degradation_active(self) -> bool:
        return (
            self._split_tracking_fields
            or self._split_creative_fields
            or self._reduced_page_size is not None
            or self._two_phase_mode
        )

    def validate_response(self, response: requests.Response) -> None:
        # Record the Facebook error code so _request can drive the degradation
        # ladder when the retriable error surfaces.
        self._last_error_code = None
        if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            try:
                self._last_error_code = response.json().get("error", {}).get("code")
            except Exception:
                self._last_error_code = None
        super().validate_response(response)

    def _advance_degradation(self) -> bool:
        """Activate the next rung of the degradation ladder.

        Called when Facebook keeps answering error code 1 ('Please reduce the
        amount of data you're asking for') on /ads. Each rung shrinks the
        request further. Returns False once every applicable rung is already
        active, letting the caller fall back to the regular backoff retries.
        """
        if not self._split_tracking_fields and self.config.get("include_ads_tracking_fields", True):
            # First rung: tracking fields are the usual culprit and cheapest
            # to drop, so try that first.
            self._split_tracking_fields = True
            user_logger.info(
                f"[{self.name}] Facebook error code 1 — switching to "
                "split-request mode (fetching tracking fields "
                "separately to avoid payload size limit)"
            )
            return True
        if (
            not self._split_creative_fields
            and self.config.get("split_creative_on_error", True)
            and "creatives" in self._tap.streams
        ):
            # Tracking fields are already excluded (by config or by the rung
            # above) and the error persists: the remaining payload weight is
            # the inline creative expansion, so split that out too.
            self._split_creative_fields = True
            user_logger.info(
                f"[{self.name}] Facebook error code 1 persists — "
                "switching to split-request mode for creative "
                "fields as well (fetching creative details "
                "separately)"
            )
            return True
        if (
            self._reduced_page_size is None
            and self.config.get("ads_auto_reduce_page_size", True)
            and self.page_size > MIN_ADS_PAGE_SIZE
        ):
            self._reduced_page_size = MIN_ADS_PAGE_SIZE
            user_logger.info(
                f"[{self.name}] Facebook error code 1 persists — reducing ads "
                f"page size to {MIN_ADS_PAGE_SIZE} (the minimum reliably "
                "supported by Facebook cursor pagination)"
            )
            return True
        if not self._two_phase_mode and self.config.get("ads_two_phase_on_error", True):
            self._two_phase_mode = True
            user_logger.info(
                f"[{self.name}] Facebook error code 1 persists — switching to "
                "two-phase mode (listing ad ids only, then batch-fetching ad "
                "fields separately)"
            )
            return True
        return False

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: dict | None,
    ) -> requests.Response:
        while True:
            if self._degradation_active:
                qs = parse_qs(urlparse(prepared_request.url).query)
                cursor = qs.get("after", [None])[0]
                new_params = self.get_url_params(context, cursor)
                prepared_request.prepare_url(self.url_base + self.path, new_params)
            try:
                return super()._request(prepared_request, context)
            except RetriableAPIError:
                if self._last_error_code == 1 and self._advance_degradation():
                    # A new rung was activated: retry right away with the
                    # smaller request instead of burning the SDK's backoff
                    # budget on a payload Facebook already refused.
                    time.sleep(DEGRADATION_RETRY_SLEEP_SECONDS)
                    continue
                raise

    def parse_response(self, response: requests.Response) -> Any:
        include_preview = self.config.get("include_ad_preview_link", False)
        enrichment_active = (
            self._split_tracking_fields
            or self._split_creative_fields
            or self._two_phase_mode
            or include_preview
        )
        if not enrichment_active or response.status_code != HTTPStatus.OK:
            yield from super().parse_response(response)
            return

        records = list(response.json().get("data", []))
        ad_ids = [r["id"] for r in records if "id" in r]

        if self._two_phase_mode:
            base_data = self._fetch_base_fields(ad_ids)
            merged = []
            for record in records:
                ad_id = record.get("id")
                if ad_id in base_data:
                    record.update(base_data[ad_id])
                    merged.append(record)
                else:
                    # Emitting a skeleton row here could shadow a previously
                    # complete row in the warehouse, so skip it instead: the
                    # loss is bounded to this one ad and explicitly logged.
                    user_logger.warning(
                        f"[{self.name}] Could not fetch fields for ad {ad_id} "
                        "even after splitting requests — skipping this ad for "
                        "this run"
                    )
            records = merged

        tracking_data = self._fetch_tracking_fields(ad_ids) if self._split_tracking_fields else {}
        # In two-phase mode the inline creative expansion is impossible (the
        # listing only carries ids), so the creatives child stream depends on
        # this separate fetch regardless of the split_creative_on_error flag.
        fetch_full_creatives = self._split_creative_fields or (
            self._two_phase_mode and "creatives" in self._tap.streams
        )
        creative_data = self._fetch_creative_fields(records) if fetch_full_creatives else {}
        preview_data = self._fetch_preview_links(ad_ids) if include_preview else {}

        for record in records:
            ad_id = record.get("id")
            record.update(tracking_data.get(ad_id, {}))
            if ad_id in creative_data:
                record["creative"] = creative_data[ad_id]
            if include_preview:
                record["preview_shareable_link"] = preview_data.get(ad_id)
            yield record

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
        url = f"https://graph.facebook.com/{API_VERSION}/"
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

    _BATCH_LOOKUP_CHUNK_SIZE = 50

    def _is_data_limit_error(self, response: requests.Response) -> bool:
        """Return True when the response is Facebook's data-limit error (code 1)."""
        if response.status_code != HTTPStatus.INTERNAL_SERVER_ERROR:
            return False
        try:
            return response.json().get("error", {}).get("code") == 1
        except Exception:
            return False

    def _batch_id_lookup(self, ids: list[str], params: dict, label: str) -> dict[str, dict]:
        """Fetch objects by id via the Graph batch lookup endpoint, keyed by id.

        Requests _BATCH_LOOKUP_CHUNK_SIZE ids per call (the endpoint's limit).
        A chunk that hits the Facebook data-limit error (code 1) is split in
        half and retried, down to a single id — so at worst one problematic
        object is skipped (with an explicit warning) instead of a whole chunk.
        Ids that ultimately fail are absent from the result.
        """
        if not ids:
            return {}
        result: dict[str, dict] = {}
        pending = [
            ids[i : i + self._BATCH_LOOKUP_CHUNK_SIZE]
            for i in range(0, len(ids), self._BATCH_LOOKUP_CHUNK_SIZE)
        ]
        while pending:
            chunk = pending.pop(0)
            resp = self._graph_batch_request(
                "get",
                params={**params, "ids": ",".join(chunk)},
                label=label,
            )
            if resp is None:
                # Network failure after retries — already logged by
                # _graph_batch_request; skip this chunk.
                continue
            if resp.status_code == HTTPStatus.OK:
                result.update(resp.json())
                continue
            if self._is_data_limit_error(resp) and len(chunk) > 1:
                mid = len(chunk) // 2
                user_logger.warning(
                    f"[{self.name}] {label}: Facebook data-limit error on a "
                    f"chunk of {len(chunk)} ids — splitting the chunk and retrying"
                )
                pending[:0] = [chunk[:mid], chunk[mid:]]
                continue
            user_logger.warning(
                f"[{self.name}] {label}: failed to fetch {len(chunk)} object(s) "
                f"(status {resp.status_code}) — skipping. "
                f"Response: {self._redacted_response_body(resp)}"
            )
        return result

    def _fetch_tracking_fields(self, ad_ids: list[str]) -> dict[str, dict]:
        """Batch-fetch tracking_specs, conversion_specs and recommendations for ad IDs.

        Returns a dict keyed by ad ID. Ads whose lookup ultimately failed are
        simply absent from the result (tracking fields will be null for those
        rows on this page).
        """
        return self._batch_id_lookup(
            ad_ids,
            params={
                "fields": "tracking_specs,conversion_specs,recommendations",
                "access_token": self.config["access_token"],
            },
            label="tracking fields",
        )

    def _fetch_base_fields(self, ad_ids: list[str]) -> dict[str, dict]:
        """Batch-fetch the inline ad fields for two-phase mode, keyed by ad id.

        In two-phase mode the /ads listing only returns id and updated_time;
        this fetches the remaining base columns (plus the bare creative
        reference) via the batch id lookup endpoint.
        """
        return self._batch_id_lookup(
            ad_ids,
            params={
                "fields": ",".join([*BASE_AD_COLUMNS, "creative"]),
                "access_token": self.config["access_token"],
            },
            label="ad base fields",
        )

    def _fetch_creative_fields(self, records: list[dict]) -> dict[str, dict]:
        """Batch-fetch full creative field data for a page of split-creative ads.

        When `_split_creative_fields` is active, `path` only requests the bare
        `creative` field on each ad, so each record's `creative` is just
        `{"id": ...}`. This fetches the full set of creative columns (matching
        what `creatives` stream needs, based on `creative_fields_mode`) via the
        Facebook batch ID lookup endpoint, keyed by creative ID (see
        _batch_id_lookup for chunking), then remaps the result back to ad ID so callers can
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
        creatives_by_id = self._batch_id_lookup(
            creative_ids,
            params={
                "fields": fields,
                "thumbnail_width": thumbnail_width,
                "thumbnail_height": thumbnail_height,
                "access_token": self.config["access_token"],
            },
            label="creative fields",
        )

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
        At page_size >= 50, paging.next reliably indicates whether more pages
        exist — which is why the degradation ladder never reduces the page
        size below MIN_ADS_PAGE_SIZE. As a safety net, a repeated cursor
        aborts the run loudly instead of re-extracting the same page forever.
        """
        resp_json = response.json()
        paging = resp_json.get("paging", {})
        if "next" not in paging:
            return None
        token = paging.get("cursors", {}).get("after")
        if token is not None and token == previous_token:
            user_logger.error(
                f"[{self.name}] Pagination loop detected: Facebook returned "
                f"the same cursor twice at page_size={self.page_size}. "
                "Aborting to avoid re-extracting the same page indefinitely; "
                "if this persists, increase ads_page_size."
            )
            sys.exit(1)
        return token

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

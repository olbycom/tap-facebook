"""Stream class for Creative."""

from __future__ import annotations

import typing as t

from nekt_singer_sdk.streams.core import REPLICATION_INCREMENTAL, Stream
from nekt_singer_sdk.typing import (
    BooleanType,
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook.streams.ads import AdsStream

if t.TYPE_CHECKING:
    from collections.abc import Mapping

    from nekt_singer_sdk.helpers.types import Context

# Field sets for different extraction modes
BASIC_FIELDS = [
    "account_id",
    "body",
    "id",
    "name",
    "status",
    "title",
    "actor_id",
    "authorization_category",
    "call_to_action_type",
    "enable_direct_install",
    "link_url",
    "object_id",
    "object_type",
    "object_url",
    "use_page_actor_override",
]

ADVANCED_FIELDS = [
    *BASIC_FIELDS,
    "applink_treatment",
    "branded_content_sponsor_page_id",
    "bundle_folder_id",
    "categorization_criteria",
    "category_media_source",
    "degrees_of_freedom_spec",
    "destination_set_id",
    "dynamic_ad_voice",
    "effective_authorization_category",
    "effective_instagram_media_id",
    "effective_instagram_story_id",
    "effective_object_story_id",
    "image_hash",
    "image_url",
    "instagram_user_id",
    "instagram_permalink_url",
    "instagram_story_id",
    "link_destination_display_url",
    "link_og_id",
    "object_store_url",
    "object_story_id",
    "place_page_set_id",
    "playable_asset_id",
    "source_instagram_media_id",
    "template_url",
    "thumbnail_id",
    "thumbnail_url",
    "url_tags",
    "video_id",
]

# Requested from the API in every mode purely to derive `destination_url` and
# `destination_type`. Facebook has no flat "final URL" field: the landing page
# lives inside these nested specs, and which one carries it depends on the ad
# format. They are never emitted as columns — `get_records` flattens them and
# drops the raw payload, so no nested arrays reach the warehouse.
DESTINATION_SOURCE_FIELDS = [
    "object_story_spec",
    "asset_feed_spec",
]

DESTINATION_TYPE_WEBSITE = "website"
DESTINATION_TYPE_LEAD_FORM = "lead_form"
DESTINATION_TYPE_FACEBOOK_PAGE = "facebook_page"

# Facebook returns this placeholder as the `link` of lead ads that send people
# to a native lead form rather than to a website.
PLACEHOLDER_LINKS = frozenset({"http://fb.me/", "https://fb.me/"})

# Settings of the `creative_files` child stream (see streams/creative_files.py).
# They are declared here because this stream owns the two URL fields that stream
# downloads, and has to request them from the Graph API on its behalf.
CREATIVE_FILES_ENABLE_CONFIG_KEY = "enable_creative_files_stream"
CREATIVE_FILES_VOLUME_CONFIG_KEY = "nekt_volume_to_upload_creative_files"
CREATIVE_FILES_TO_UPLOAD_CONFIG_KEY = "creative_files_to_upload"

# Which creative field each selectable file kind is downloaded from. Iteration
# order is the emission order of the child stream's records.
CREATIVE_FILE_SOURCE_FIELDS = {
    "image": "image_url",
    "thumbnail": "thumbnail_url",
}


def creative_files_enabled(config: Mapping[str, t.Any]) -> bool:
    """Whether the user opted into uploading creative files to a Nekt volume.

    Only the switch is checked here: the destination volume and the Nekt access
    token are validated by the upload mixin at sync time, which warns the user
    when either is missing. Keeping the check to the switch means the stream
    still appears in the catalog (and its fields are still requested) in an
    environment where the token is not set, e.g. during discovery.
    """
    return bool(config.get(CREATIVE_FILES_ENABLE_CONFIG_KEY))


def selected_creative_file_sources(config: Mapping[str, t.Any]) -> list[str]:
    """The creative file kinds to upload, as keys of CREATIVE_FILE_SOURCE_FIELDS.

    Accepts a comma-separated list so the setting can grow a value without
    becoming a new setting. An empty/absent value means "all of them"; a value
    that matches nothing returns an empty list, which the child stream reports.
    """
    raw = str(config.get(CREATIVE_FILES_TO_UPLOAD_CONFIG_KEY) or "").strip()
    if not raw:
        return list(CREATIVE_FILE_SOURCE_FIELDS)
    requested = {part.strip().lower() for part in raw.split(",") if part.strip()}
    return [source for source in CREATIVE_FILE_SOURCE_FIELDS if source in requested]


def _clean_url(value: object) -> str | None:
    """Return the value as a usable destination URL, or None if it is not one."""
    if not isinstance(value, str):
        return None
    url = value.strip()
    if not url or url in PLACEHOLDER_LINKS:
        return None
    return url


def _destination_from_story_data(story_data: object) -> tuple[str | None, str | None]:
    """Extract (url, type) from a link_data / video_data / photo_data block.

    The lead form is checked before any link because lead ads still carry a
    placeholder `link`, and reporting that as the landing page would be wrong.
    """
    if not isinstance(story_data, dict):
        return None, None

    call_to_action = story_data.get("call_to_action") or {}
    cta_value = call_to_action.get("value") or {}
    if cta_value.get("lead_gen_form_id"):
        return None, DESTINATION_TYPE_LEAD_FORM

    for candidate in (cta_value.get("link"), story_data.get("link")):
        url = _clean_url(candidate)
        if url:
            return url, DESTINATION_TYPE_WEBSITE

    # Carousel ads may leave the top-level link empty and carry one URL per card;
    # the first card is the ad's effective destination.
    for child in story_data.get("child_attachments") or []:
        url = _clean_url((child or {}).get("link"))
        if url:
            return url, DESTINATION_TYPE_WEBSITE

    return None, None


def extract_destination(creative: dict) -> tuple[str | None, str | None]:
    """Flatten a creative's landing page into (destination_url, destination_type).

    Returns (None, None) when neither could be determined — most often an ad
    built from an existing organic post, where the link lives on the post itself
    and not on the creative.
    """
    asset_feed_spec = creative.get("asset_feed_spec")
    if isinstance(asset_feed_spec, dict):
        for link in asset_feed_spec.get("link_urls") or []:
            url = _clean_url((link or {}).get("website_url"))
            if url:
                return url, DESTINATION_TYPE_WEBSITE

    story_spec = creative.get("object_story_spec")
    if isinstance(story_spec, dict):
        fallback_type = None
        for key in ("link_data", "video_data", "photo_data"):
            url, destination_type = _destination_from_story_data(story_spec.get(key))
            if url:
                return url, destination_type
            fallback_type = fallback_type or destination_type
        if fallback_type:
            return None, fallback_type

    # Scalar fallbacks. `template_url` is only requested in advanced mode, so it
    # is simply absent (not an error) in basic mode.
    for candidate, destination_type in (
        (creative.get("template_url"), DESTINATION_TYPE_WEBSITE),
        (creative.get("object_url"), DESTINATION_TYPE_WEBSITE),
        (creative.get("link_url"), DESTINATION_TYPE_FACEBOOK_PAGE),
    ):
        url = _clean_url(candidate)
        if url:
            return url, destination_type

    return None, None


class CreativeStream(Stream):
    """Facebook Ad Creative stream using child context from AdsStream.

    This stream extracts creative data from the ads stream without making
    additional API calls, providing true incremental sync and avoiding
    rate limiting issues.
    """

    name = "creatives"
    tap_stream_id = "creatives"
    replication_key = "ad_updated_time"
    primary_keys = ["id"]  # noqa: RUF012
    parent_stream_type = AdsStream
    state_partitioning_keys = []

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the stream and the per-run creative dedupe set."""
        super().__init__(*args, **kwargs)
        # The same creative is attached to many ads, so this stream emits it once
        # per ad. Child contexts are deduped by creative id to download each
        # creative's files only once per extraction.
        self._creative_ids_with_files: set[str] = set()

    @property
    def columns(self) -> list[str]:
        """Get columns based on creative_fields_mode configuration."""
        fields_mode = self.config.get("creative_fields_mode", "basic")

        fields = ADVANCED_FIELDS if fields_mode == "advanced" else BASIC_FIELDS
        # Requested in both modes: destination_url is useless if it only works
        # for sources that opted into advanced mode.
        extra_fields = [*DESTINATION_SOURCE_FIELDS]
        # The image URLs the `creative_files` stream downloads only belong to the
        # advanced field set, so opting into the upload also opts into requesting
        # them -- otherwise the stream would silently upload nothing on the
        # default (basic) mode.
        if creative_files_enabled(self.config):
            extra_fields += [
                CREATIVE_FILE_SOURCE_FIELDS[source]
                for source in selected_creative_file_sources(self.config)
            ]
        # Advanced mode already asks for the image URLs; Facebook rejects a
        # repeated field in the `fields` expansion.
        return list(dict.fromkeys([*fields, *extra_fields]))

    schema = PropertiesList(
        Property(
            "id",
            StringType,
            description="Unique ID for the ad creative",
        ),
        Property(
            "account_id",
            StringType,
            description="Ad account ID this creative belongs to",
        ),
        Property(
            "actor_id",
            StringType,
            description="The actor ID (Page ID) of this creative",
        ),
        Property(
            "applink_treatment",
            StringType,
            description="For Dynamic Ads: action when app is not installed (e.g. open webpage or app store)",
        ),
        Property(
            "authorization_category",
            StringType,
            description="Whether ad is labeled as political (e.g. POLITICAL, POLITICAL_WITH_DIGITALLY_CREATED_MEDIA)",
        ),
        Property(
            "body",
            StringType,
            description="The body text of the ad; not supported for video post creatives",
        ),
        Property(
            "branded_content_sponsor_page_id",
            StringType,
            description="Page ID representing business that runs Branded Content ads",
        ),
        Property(
            "bundle_folder_id",
            StringType,
            description="Dynamic Ad bundle folder ID",
        ),
        Property(
            "call_to_action_type",
            StringType,
            description="Type of CTA button (e.g. SHOP_NOW, LEARN_MORE, INSTALL_APP)",
        ),
        Property(
            "categorization_criteria",
            StringType,
            description="Dynamic Category Ad categorization field (e.g. brand)",
        ),
        Property(
            "category_media_source",
            StringType,
            description="Dynamic Ad rendering mode for category ads",
        ),
        Property(
            "destination_set_id",
            StringType,
            description="Destination set ID for the creative",
        ),
        Property(
            "dynamic_ad_voice",
            StringType,
            description="Voice setting for dynamic ads",
        ),
        Property(
            "effective_authorization_category",
            StringType,
            description="Effective authorization category applied to the creative",
        ),
        Property(
            "effective_instagram_media_id",
            StringType,
            description="Effective Instagram media ID when using Instagram content",
        ),
        Property(
            "effective_instagram_story_id",
            StringType,
            description="Effective Instagram story ID",
        ),
        Property(
            "effective_object_story_id",
            StringType,
            description="ID of the page post or story used in the creative",
        ),
        Property(
            "enable_direct_install",
            BooleanType,
            description="Whether direct install is enabled for app ads",
        ),
        Property(
            "image_hash",
            StringType,
            description="Hash of the image from ad account image library",
        ),
        Property(
            "image_url",
            StringType,
            description="URL of the image used in the creative",
        ),
        Property(
            "instagram_user_id",
            StringType,
            description="Instagram user ID when creative uses Instagram content",
        ),
        Property(
            "instagram_permalink_url",
            StringType,
            description="Permanent link to the Instagram content",
        ),
        Property(
            "instagram_story_id",
            StringType,
            description="Instagram story ID",
        ),
        Property(
            "link_destination_display_url",
            StringType,
            description="Display URL for the link destination",
        ),
        Property(
            "link_og_id",
            StringType,
            description="Open Graph object ID for the link",
        ),
        Property(
            "link_url",
            StringType,
            description="URL the ad links to",
        ),
        Property(
            "name",
            StringType,
            description="Name of the creative",
        ),
        Property(
            "object_id",
            StringType,
            description="ID of the object (e.g. link, video) in the creative",
        ),
        Property(
            "object_store_url",
            StringType,
            description="App store or destination URL for the object",
        ),
        Property(
            "object_story_id",
            StringType,
            description="ID of the story (page post) used in the creative",
        ),
        Property(
            "object_type",
            StringType,
            description="Type of object (SHARE, PHOTO, VIDEO, etc.)",
        ),
        Property(
            "object_url",
            StringType,
            description="URL of the object",
        ),
        Property(
            "place_page_set_id",
            IntegerType,
            description="Place page set ID for location-based creatives",
        ),
        Property(
            "playable_asset_id",
            IntegerType,
            description="Playable asset ID for playable ad creatives",
        ),
        Property(
            "source_instagram_media_id",
            StringType,
            description="Source Instagram media ID when repurposing Instagram content",
        ),
        Property(
            "status",
            StringType,
            description="Status of the creative (ACTIVE, DELETED, etc.)",
        ),
        Property(
            "template_url",
            StringType,
            description="URL of the template for dynamic creatives",
        ),
        Property(
            "thumbnail_id",
            StringType,
            description="ID of the thumbnail image",
        ),
        Property(
            "thumbnail_url",
            StringType,
            description="URL of the thumbnail for the creative",
        ),
        Property(
            "title",
            StringType,
            description="Title of the ad",
        ),
        Property(
            "url_tags",
            StringType,
            description="URL parameters for tracking",
        ),
        Property(
            "use_page_actor_override",
            BooleanType,
            description="Whether to use page actor override for the creative",
        ),
        Property(
            "video_id",
            StringType,
            description="ID of the video used in the creative",
        ),
        Property(
            "destination_url",
            StringType,
            description=(
                "Landing page the ad sends people to, flattened from the "
                "creative's object_story_spec or asset_feed_spec. Null when the "
                "ad has no website destination — check destination_type to tell "
                "that apart from missing data."
            ),
        ),
        Property(
            "destination_type",
            StringType,
            description=(
                "What the ad sends people to: 'website' (destination_url holds "
                "the landing page), 'lead_form' (a native Meta lead form, so "
                "there is no URL), or 'facebook_page' (a tab of the advertiser's "
                "Facebook page). Null when it could not be determined, which is "
                "typically an ad built from an existing organic post, where the "
                "link belongs to the post and not to the creative."
            ),
        ),
        Property(
            "ad_id",
            StringType,
            description="ID of the parent ad (from context)",
        ),
        Property(
            "ad_updated_time",
            DateTimeType,
            description="When the parent ad was last updated (from context)",
        ),
    ).to_dict()

    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        """Get creative records from parent ads stream context.

        Args:
            context: Context from parent AdsStream containing creative data.

        Yields:
            Individual creative record dictionaries.
        """
        if not context or "creative" not in context:
            return

        creative_data = {
            **context["creative"],
            "ad_id": context.get("ad_id"),
            "ad_updated_time": context.get("ad_updated_time"),
        }

        destination_url, destination_type = extract_destination(creative_data)
        # Drop the raw specs: they exist only to feed the two fields above, and
        # emitting them would add nested arrays to the stream's output.
        for field in DESTINATION_SOURCE_FIELDS:
            creative_data.pop(field, None)
        creative_data["destination_url"] = destination_url
        creative_data["destination_type"] = destination_type

        yield creative_data

    def get_child_context(
        self,
        record: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """Feed the creative's image URLs to the creative_files child stream.

        Returns None (skipping the child) unless the upload was enabled, this
        creative was not already handled in this extraction, and it carries at
        least one of the URLs -- so the common skipped cases cost nothing.
        """
        if not creative_files_enabled(self.config):
            return None
        creative_id = record.get("id")
        if not creative_id or creative_id in self._creative_ids_with_files:
            return None
        urls = {
            field: record.get(field)
            for field in CREATIVE_FILE_SOURCE_FIELDS.values()
        }
        if not any(urls.values()):
            return None
        self._creative_ids_with_files.add(creative_id)
        return {
            "creative_id": creative_id,
            "ad_id": record.get("ad_id"),
            "ad_updated_time": record.get("ad_updated_time"),
            **urls,
        }

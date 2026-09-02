"""Stream class for AdAccounts."""

from __future__ import annotations

import typing as t
from http import HTTPStatus

from facebook_business.adobjects.adaccount import AdAccount
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.streams.core import REPLICATION_INCREMENTAL
from nekt_singer_sdk.typing import (
    ArrayType,
    BooleanType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)
from singer_sdk.typing import DateTimeType

from tap_facebook.client import API_VERSION, FacebookStream

if t.TYPE_CHECKING:
    import requests

# Field names the Graph API accepts on a direct ``GET /act_{id}``. The edge
# listing (``/me/adaccounts``) silently drops unknown names, but an object read
# rejects the whole request with "(#100) Tried accessing nonexisting field".
# Several EXTENDED_COLUMNS are flattened names of nested objects
# (agency_client_declaration_*, business_manager_*, extended_credit_invoice_group_*)
# that the API has never returned; they stay in the schema for compatibility and
# are simply not requested on the direct read.
GRAPH_AD_ACCOUNT_FIELDS: frozenset[str] = frozenset(
    value
    for name, value in vars(AdAccount.Field).items()
    if not name.startswith("_") and isinstance(value, str)
)

# Basic columns - core fields that work with limited permissions
BASIC_COLUMNS = [
    "account_id",
    "account_status",
    "age",
    "amount_spent",
    "balance",
    "business_city",
    "business_country_code",
    "business_name",
    "business_street",
    "business_street2",
    "business_state",
    "business_zip",
    "can_create_brand_lift_study",
    "capabilities",
    "created_time",
    "currency",
    "disable_reason",
    "end_advertiser",
    "end_advertiser_name",
    "has_migrated_permissions",
    "id",
    "is_attribution_spec_system_default",
    "is_direct_deals_enabled",
    "is_in_3ds_authorization_enabled_market",
    "is_notifications_enabled",
    "is_personal",
    "is_prepay_account",
    "is_tax_id_required",
    "min_campaign_group_spend_cap",
    "min_daily_budget",
    "name",
    "offsite_pixels_tos_accepted",
    "spend_cap",
    "timezone_id",
    "timezone_name",
    "timezone_offset_hours_utc",
]

# Extended columns - require elevated permissions (admin/finance access)
# These may fail if user doesn't have sufficient permissions on all ad accounts
EXTENDED_COLUMNS = [
    # Owner and tax info
    "owner",
    "tax_id",
    "tax_id_status",
    "tax_id_type",
    # Agency client declaration fields
    "agency_client_declaration_agency_representing_client",
    "agency_client_declaration_client_based_in_france",
    "agency_client_declaration_client_city",
    "agency_client_declaration_client_country_code",
    "agency_client_declaration_client_email_address",
    "agency_client_declaration_client_name",
    "agency_client_declaration_client_postal_code",
    "agency_client_declaration_client_province",
    "agency_client_declaration_client_street",
    "agency_client_declaration_client_street2",
    "agency_client_declaration_has_written_mandate_from_advertiser",
    "agency_client_declaration_is_client_paying_invoices",
    # Business manager fields
    "business_manager_block_offline_analytics",
    "business_manager_created_by",
    "business_manager_created_time",
    "business_manager_extended_updated_time",
    "business_manager_is_hidden",
    "business_manager_link",
    "business_manager_name",
    "business_manager_payment_account_id",
    "business_manager_primary_page",
    "business_manager_profile_picture_uri",
    "business_manager_timezone_id",
    "business_manager_two_factor_type",
    "business_manager_updated_by",
    "business_manager_update_time",
    "business_manager_verification_status",
    "business_manager_vertical",
    "business_manager_vertical_id",
    "business_manager_manager_id",
    # Extended credit and invoice fields
    "extended_credit_invoice_group_id",
    "extended_credit_invoice_group_auto_enroll",
    "extended_credit_invoice_group_customer_po_number",
    "extended_credit_invoice_group_email",
    "extended_credit_invoice_group_emails",
    "extended_credit_invoice_group_name",
    # Other extended fields
    "io_number",
    "media_agency",
    "partner",
    "salesforce_invoice_group_id",
    "funding_source",
    "funding_source_details",
]

# Basic schema properties
BASIC_SCHEMA_PROPERTIES = [
    Property("account_id", StringType, description="The ID of the ad account"),
    Property("account_status", IntegerType, description="Status of the account (1=ACTIVE, 2=DISABLED, etc.)"),
    Property("age", NumberType, description="Amount of time the ad account has been open, in days"),
    Property("amount_spent", IntegerType, description="Current amount spent by the account"),
    Property("balance", IntegerType, description="Bill amount due for this ad account"),
    Property("business_city", StringType, description="City for business address"),
    Property("business_country_code", StringType, description="Country code for the business address"),
    Property("business_name", StringType, description="The business name for the account"),
    Property("business_street", StringType, description="First line of the business street address"),
    Property("business_street2", StringType, description="Second line of the business street address"),
    Property("business_state", StringType, description="State abbreviation for business address"),
    Property("business_zip", StringType, description="Zip code for business address"),
    Property("can_create_brand_lift_study", BooleanType, description="If a new brand lift study can be created"),
    Property("capabilities", ArrayType(StringType), description="List of capabilities the ad account has"),
    Property("created_time", StringType, description="When the account was created (ISO 8601)"),
    Property("currency", StringType, description="Currency used for the account"),
    Property("disable_reason", IntegerType, description="Reason the account was disabled (0=NONE, 1=ADS_INTEGRITY_POLICY, etc.)"),
    Property("end_advertiser", StringType, description="Entity the ads will target (Page ID or App ID)"),
    Property("end_advertiser_name", StringType, description="Name of the entity the ads will target"),
    Property("has_migrated_permissions", BooleanType, description="Whether this account has migrated permissions"),
    Property("id", StringType, description="The string act_{ad_account_id}"),
    Property("is_attribution_spec_system_default", BooleanType, description="If attribution spec is system default"),
    Property("is_direct_deals_enabled", BooleanType, description="Whether Direct Deals are enabled"),
    Property("is_in_3ds_authorization_enabled_market", BooleanType, description="If account is in 3DS authorization market"),
    Property("is_notifications_enabled", BooleanType, description="Whether notifications are enabled for this account"),
    Property("is_personal", IntegerType, description="If account is for private non-business use (affects VAT)"),
    Property("is_prepay_account", BooleanType, description="If this is a prepay account"),
    Property("is_tax_id_required", BooleanType, description="If tax ID is required for this account"),
    Property("min_campaign_group_spend_cap", IntegerType, description="Minimum campaign group spend cap"),
    Property("min_daily_budget", IntegerType, description="Minimum daily budget"),
    Property("name", StringType, description="Name of the ad account"),
    Property("offsite_pixels_tos_accepted", BooleanType, description="Whether offsite pixels ToS are accepted"),
    Property("spend_cap", IntegerType, description="Spend cap for the account"),
    Property("timezone_id", IntegerType, description="Timezone ID for the account"),
    Property("timezone_name", StringType, description="Timezone name"),
    Property("timezone_offset_hours_utc", NumberType, description="Timezone offset from UTC in hours"),
]

# Extended schema properties
EXTENDED_SCHEMA_PROPERTIES = [
    Property("owner", StringType, description="Owner of the ad account"),
    Property("tax_id", StringType, description="Tax ID for the account"),
    Property("tax_id_status", IntegerType, description="Status of the tax ID"),
    Property("tax_id_type", StringType, description="Type of tax ID"),
    Property(
        "agency_client_declaration_agency_representing_client",
        IntegerType,
        description="Agency representing client declaration",
    ),
    Property(
        "agency_client_declaration_client_based_in_france",
        IntegerType,
        description="Whether client is based in France",
    ),
    Property(
        "agency_client_declaration_client_city",
        StringType,
        description="Client city for agency declaration",
    ),
    Property(
        "agency_client_declaration_client_country_code",
        StringType,
        description="Client country code for agency declaration",
    ),
    Property(
        "agency_client_declaration_client_email_address",
        StringType,
        description="Client email for agency declaration",
    ),
    Property(
        "agency_client_declaration_client_name",
        StringType,
        description="Client name for agency declaration",
    ),
    Property(
        "agency_client_declaration_client_postal_code",
        StringType,
        description="Client postal code for agency declaration",
    ),
    Property(
        "agency_client_declaration_client_province",
        StringType,
        description="Client province for agency declaration",
    ),
    Property(
        "agency_client_declaration_client_street",
        StringType,
        description="Client street for agency declaration",
    ),
    Property(
        "agency_client_declaration_client_street2",
        StringType,
        description="Client street line 2 for agency declaration",
    ),
    Property(
        "agency_client_declaration_has_written_mandate_from_advertiser",
        IntegerType,
        description="Whether agency has written mandate from advertiser",
    ),
    Property(
        "agency_client_declaration_is_client_paying_invoices",
        IntegerType,
        description="Whether client is paying invoices",
    ),
    Property(
        "business_manager_block_offline_analytics",
        BooleanType,
        description="Whether Business Manager blocks offline analytics",
    ),
    Property(
        "business_manager_created_by",
        StringType,
        description="Business Manager created by user ID",
    ),
    Property(
        "business_manager_created_time",
        StringType,
        description="Business Manager creation time",
    ),
    Property(
        "business_manager_extended_updated_time",
        StringType,
        description="Business Manager extended updated time",
    ),
    Property(
        "business_manager_is_hidden",
        BooleanType,
        description="Whether Business Manager is hidden",
    ),
    Property(
        "business_manager_link",
        StringType,
        description="Business Manager link",
    ),
    Property(
        "business_manager_name",
        StringType,
        description="Business Manager name",
    ),
    Property(
        "business_manager_payment_account_id",
        IntegerType,
        description="Business Manager payment account ID",
    ),
    Property(
        "business_manager_primary_page",
        StringType,
        description="Business Manager primary page ID",
    ),
    Property(
        "business_manager_profile_picture_uri",
        StringType,
        description="Business Manager profile picture URI",
    ),
    Property(
        "business_manager_timezone_id",
        IntegerType,
        description="Business Manager timezone ID",
    ),
    Property(
        "business_manager_two_factor_type",
        StringType,
        description="Business Manager two-factor type",
    ),
    Property(
        "business_manager_updated_by",
        StringType,
        description="Business Manager updated by user ID",
    ),
    Property(
        "business_manager_update_time",
        StringType,
        description="Business Manager update time",
    ),
    Property(
        "business_manager_verification_status",
        StringType,
        description="Business Manager verification status",
    ),
    Property(
        "business_manager_vertical",
        StringType,
        description="Business Manager vertical",
    ),
    Property(
        "business_manager_vertical_id",
        IntegerType,
        description="Business Manager vertical ID",
    ),
    Property(
        "business_manager_manager_id",
        IntegerType,
        description="Business Manager manager ID",
    ),
    Property(
        "extended_credit_invoice_group_id",
        IntegerType,
        description="Extended credit invoice group ID",
    ),
    Property(
        "extended_credit_invoice_group_auto_enroll",
        BooleanType,
        description="Extended credit invoice group auto-enroll",
    ),
    Property(
        "extended_credit_invoice_group_customer_po_number",
        StringType,
        description="Extended credit invoice group customer PO number",
    ),
    Property(
        "extended_credit_invoice_group_email",
        StringType,
        description="Extended credit invoice group email",
    ),
    Property(
        "extended_credit_invoice_group_emails",
        StringType,
        description="Extended credit invoice group emails",
    ),
    Property(
        "extended_credit_invoice_group_name",
        StringType,
        description="Extended credit invoice group name",
    ),
    Property(
        "io_number",
        IntegerType,
        description="Insertion order number",
    ),
    Property(
        "media_agency",
        StringType,
        description="Media agency",
    ),
    Property(
        "partner",
        StringType,
        description="Partner",
    ),
    Property(
        "salesforce_invoice_group_id",
        StringType,
        description="Salesforce invoice group ID",
    ),
    Property(
        "funding_source",
        StringType,
        description=(
            "ID of the payment method funding this ad account. Empty when the account has no "
            "payment method (ads can be created but get no delivery) or when the account is disabled."
        ),
    ),
    Property(
        "funding_source_details",
        ObjectType(
            Property("id", StringType, description="Payment method ID"),
            Property("type", IntegerType, description="Funding source type (e.g. CREDIT_CARD, INVOICE)"),
            Property("display_string", StringType, description="How the payment method is displayed"),
            Property(
                "coupons",
                ArrayType(
                    ObjectType(
                        Property("coupon_id", StringType, description="Facebook Ads coupon ID"),
                        Property("amount", IntegerType, description="Coupon amount"),
                        Property("currency", StringType, description="Coupon currency"),
                        Property("display_amount", StringType, description="Coupon display amount"),
                        Property("original_amount", IntegerType, description="Coupon original amount"),
                        Property("original_display_amount", StringType, description="Coupon original display amount"),
                        Property("expiration_date", DateTimeType, description="Coupon expiration date"),
                        Property("start_date", DateTimeType, description="Coupon start date"),
                    )
                ),
                description="List of active Facebook Ads coupons from the payment method",
            ),
        ),
        description="Payment method details (ID, type, display string, coupons)",
    ),
]


class AdAccountsStream(FacebookStream):
    """Ad accounts visible to the connection.

    https://developers.facebook.com/docs/graph-api/reference/user/accounts/

    Two sources are merged into one stream:

    1. The ad account configured on the source (``config.account_id``), fetched
       directly with ``GET /act_{account_id}``. This is the account every other
       stream extracts, so it must always be present in the table.
    2. ``GET /me/adaccounts`` — every ad account the token user can see. This is
       the historical behaviour; it is kept so existing tables do not lose rows.

    ``/me/adaccounts`` lists the accounts of the *user* who connected, and the
    configured account is not guaranteed to be among them (agency users often
    reach client accounts through a Business Manager). Before the direct fetch,
    such sources ended up with every other account of that user in the table
    and without the one they were configured for (NEKT-5001).
    """

    @property
    def url_base(self) -> str:
        """``/me`` — the listing endpoint is scoped to the token user."""
        return f"https://graph.facebook.com/{API_VERSION}/me"

    @property
    def fields_mode(self) -> str:
        """Get the fields mode from config (basic or extended)."""
        return self.config.get("ad_accounts_fields_mode", "extended")

    @property
    def columns(self) -> list[str]:  # noqa: RUF012
        """Get columns based on the configured fields mode."""
        if self.fields_mode == "basic":
            return BASIC_COLUMNS
        return BASIC_COLUMNS + EXTENDED_COLUMNS

    @property
    def path(self) -> str:
        """Build the path with fields based on the configured mode."""
        return f"/adaccounts?fields={self.columns}"

    name = "adaccounts"
    tap_stream_id = "adaccounts"
    primary_keys = ["account_id"]  # noqa: RUF012
    replication_key = None

    @property
    def schema(self) -> dict:
        """Build schema based on the configured fields mode."""
        if self.fields_mode == "basic":
            properties = BASIC_SCHEMA_PROPERTIES
        else:
            properties = BASIC_SCHEMA_PROPERTIES + EXTENDED_SCHEMA_PROPERTIES
        return PropertiesList(*properties).to_dict()

    def _build_configured_account_request(
        self,
        columns: list[str],
    ) -> requests.PreparedRequest:
        """Build ``GET /act_{account_id}?fields=...`` for the configured account.

        Only names the Graph API knows are requested — see GRAPH_AD_ACCOUNT_FIELDS.
        """
        account_id = self.config["account_id"]
        requested = [c for c in columns if c in GRAPH_AD_ACCOUNT_FIELDS]
        dropped = len(columns) - len(requested)
        if dropped:
            internal_logger.debug(
                f"[{self.name}] act_{account_id}: not requesting {dropped} column(s) "
                "that are not Graph API ad account fields."
            )
        return self.build_prepared_request(
            method="GET",
            url=f"https://graph.facebook.com/{API_VERSION}/act_{account_id}",
            params={"fields": ",".join(requested)},
            headers=self.http_headers,
        )

    def _is_non_retriable_client_error(self, response: requests.Response) -> bool:
        """True for a 4xx the base class would not retry (typically permissions)."""
        status = response.status_code
        if not (HTTPStatus.BAD_REQUEST <= status < HTTPStatus.INTERNAL_SERVER_ERROR):
            return False
        content = str(response.content).lower()
        if "too many calls" in content or "request limit reached" in content:
            return False
        return not self._is_transient_error(response)

    def _fetch_configured_account(self, context: dict | None) -> dict:
        """Fetch the configured ad account as a single record.

        In ``extended`` mode the request asks for finance/owner fields that need
        elevated permissions on the account. If Facebook rejects that request
        with a client error, retry once with the ``basic`` field set so the row
        still lands in the table. Any other failure goes through the regular
        ``_request`` path (backoff on retriable errors, exit on client errors).
        """
        account_id = self.config["account_id"]
        prepared = self._build_configured_account_request(self.columns)
        internal_logger.info(
            f"[{self.name}] Fetching configured account act_{account_id} directly "
            f"(fields_mode={self.fields_mode}, {len(self.columns)} fields)."
        )
        response = self.requests_session.send(prepared, timeout=self.timeout)

        if (
            response.status_code != HTTPStatus.OK
            and self.fields_mode == "extended"
            and self._is_non_retriable_client_error(response)
        ):
            user_logger.warning(
                f"[{self.name}] Facebook did not allow the extended fields (payment "
                f"method, owner, tax id) for ad account {account_id}. The account was "
                "extracted with the basic fields only, so those extended columns will "
                "be empty for it. Grant the connected user finance access on the "
                "account, or set 'Ad Accounts Fields Mode' to 'basic' to silence this "
                "warning."
            )
            internal_logger.warning(
                f"[{self.name}] act_{account_id} extended fetch failed with "
                f"{response.status_code}; retrying with BASIC_COLUMNS. "
                f"body={str(response.content)[:500]}"
            )
            prepared = self._build_configured_account_request(BASIC_COLUMNS)
            response = None

        if response is None or response.status_code != HTTPStatus.OK:
            # Standard handling: backoff on retriable errors, stop on client errors.
            response = self.request_decorator(self._request)(prepared, context)

        record = response.json()
        internal_logger.info(
            f"[{self.name}] Configured account act_{account_id} fetched "
            f"(is_prepay_account={record.get('is_prepay_account')}, "
            f"{len(record)} fields)."
        )
        return record

    def request_records(self, context: dict | None) -> t.Iterable[dict]:
        """Yield the configured account first, then the token user's other accounts."""
        account_id = str(self.config["account_id"])
        yield self._fetch_configured_account(context)

        skipped = 0
        for record in super().request_records(context):
            if str(record.get("account_id")) == account_id:
                skipped += 1
                continue
            yield record
        internal_logger.debug(
            f"[{self.name}] /me/adaccounts listing done; skipped {skipped} "
            f"duplicate(s) of the configured account act_{account_id}."
        )

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """Cast the monetary amounts Facebook returns as strings to integers (cents)."""
        row["amount_spent"] = int(row["amount_spent"]) if "amount_spent" in row else None
        row["balance"] = int(row["balance"]) if "balance" in row else None
        row["min_campaign_group_spend_cap"] = (
            int(row["min_campaign_group_spend_cap"]) if "min_campaign_group_spend_cap" in row else None
        )
        row["spend_cap"] = int(row["spend_cap"]) if "spend_cap" in row else None
        return row

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": 25}
        if next_page_token is not None:
            params["after"] = next_page_token

        return params

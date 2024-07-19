"""Stream class for AdInsights."""

from __future__ import annotations

import json
import time
import typing as t
from functools import lru_cache
from hashlib import md5
from http import HTTPStatus

import facebook_business.adobjects.user as fb_user
import pendulum
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsactionstats import AdsActionStats
from facebook_business.adobjects.adshistogramstats import AdsHistogramStats
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi, FacebookRequest
from facebook_business.exceptions import FacebookRequestError
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, Stream

EXCLUDED_FIELDS = [
    "account_currency",
    # "account_id",
    # "account_name",
    "action_values",
    # "actions",
    "ad_click_actions",
    # "ad_id",
    "ad_impression_actions",
    # "ad_name",
    "adset_end",
    # "adset_id",
    # "adset_name",
    "adset_start",
    "age_targeting",
    "attribution_setting",
    "auction_bid",
    "auction_competitiveness",
    "auction_max_competitor_bid",
    "buying_type",
    # "campaign_id",
    # "campaign_name",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "catalog_segment_actions",
    "catalog_segment_value",
    "catalog_segment_value_mobile_purchase_roas",
    "catalog_segment_value_omni_purchase_roas",
    "catalog_segment_value_website_purchase_roas",
    # "clicks",
    "conversion_lead_rate",
    # "conversion_rate_ranking",
    # "conversion_values",
    # "conversions",
    "converted_product_quantity",
    "converted_product_value",
    "cost_per_15_sec_video_view",
    "cost_per_2_sec_continuous_video_view",
    # "cost_per_action_type",
    "cost_per_ad_click",
    # "cost_per_conversion",
    "cost_per_conversion_lead",
    "cost_per_dda_countby_convs",
    "cost_per_estimated_ad_recallers",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_one_thousand_ad_impression",
    "cost_per_outbound_click",
    "cost_per_thruplay",
    "cost_per_unique_action_type",
    "cost_per_unique_click",
    "cost_per_unique_conversion",
    "cost_per_unique_inline_link_click",
    "cost_per_unique_outbound_click",
    # "cpc",
    # "cpm",
    "cpp",
    "created_time",
    "creative_media_type",
    # "ctr",
    # "date_start",
    # "date_stop",
    "dda_countby_convs",
    "dda_results",
    # "engagement_rate_ranking",
    # "estimated_ad_recall_rate",
    "estimated_ad_recall_rate_lower_bound",
    "estimated_ad_recall_rate_upper_bound",
    # "estimated_ad_recallers",
    "estimated_ad_recallers_lower_bound",
    "estimated_ad_recallers_upper_bound",
    # "frequency",
    "full_view_impressions",
    "full_view_reach",
    "gender_targeting",
    # "impressions",
    # "inline_link_click_ctr",
    # "inline_link_clicks",
    # "inline_post_engagement",
    "instagram_upcoming_event_reminders_set",
    "instant_experience_clicks_to_open",
    "instant_experience_clicks_to_start",
    "instant_experience_outbound_clicks",
    "interactive_component_tap",
    "labels",
    "location",
    "marketing_messages_cost_per_delivered",
    "marketing_messages_cost_per_link_btn_click",
    "marketing_messages_spend",
    "marketing_messages_website_purchase_values",
    "mobile_app_purchase_roas",
    "objective",
    "optimization_goal",
    # "outbound_clicks",
    # "outbound_clicks_ctr",
    "place_page_name",
    # "purchase_roas",
    "qualifying_question_qualify_answer_rate",
    # "quality_ranking",
    # "reach",
    "social_spend",
    # "spend",
    "total_postbacks",
    "total_postbacks_detailed",
    "total_postbacks_detailed_v4",
    "unique_actions",
    "unique_clicks",
    "unique_conversions",
    "unique_ctr",
    "unique_inline_link_click_ctr",
    "unique_inline_link_clicks",
    "unique_link_clicks_ctr",
    "unique_outbound_clicks",
    "unique_outbound_clicks_ctr",
    "unique_video_continuous_2_sec_watched_actions",
    "unique_video_view_15_sec",
    "updated_time",
    # "video_15_sec_watched_actions",
    # "video_30_sec_watched_actions",
    # "video_avg_time_watched_actions",
    "video_continuous_2_sec_watched_actions",
    # "video_p100_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_play_actions",
    "video_play_curve_actions",
    "video_play_retention_0_to_15s_actions",
    "video_play_retention_20_to_60s_actions",
    "video_play_retention_graph_actions",
    "video_thruplay_watched_actions",
    "video_time_watched_actions",
    "website_ctr",
    "website_purchase_roas",
    "wish_bid",
    "__module__",
    "__doc__",
    "__dict__",
]

POLL_JOB_SLEEP_TIME = 10
AD_REPORT_RETRY_TIME = 5 * 60
AD_REPORT_INCREMENT_SLEEP_TIME = 30
INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 10 * 60
JOB_STALE_ERROR_MESSAGE = (
    "This is an intermittent error and may resolve itself on "
    "subsequent queries to the Facebook API. "
    "You should deselect fields from the schema that are not necessary, "
    "as that may help improve the reliability of the Facebook API."
)


class AdsInsightStream(Stream):
    name = "adsinsights"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "date_start"

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the stream."""
        self._report_definition = kwargs.pop("report_definition")
        super().__init__(*args, **kwargs)

    @property
    def primary_keys(self) -> list[str] | None:
        return ["id"]

    @primary_keys.setter
    def primary_keys(self, new_value: list[str] | None) -> None:
        """Set primary key(s) for the stream.

        Args:
            new_value: TODO
        """
        self._primary_keys = new_value

    @staticmethod
    def _get_datatype(field: str) -> th.Type | None:
        d_type = AdsInsights._field_types[field]  # noqa: SLF001
        if d_type == "string":
            return th.StringType()
        if d_type.startswith("list"):
            if "AdsActionStats" in d_type:
                sub_props = [
                    th.Property(field.replace("field_", ""), th.StringType())
                    for field in list(AdsActionStats.Field.__dict__)
                    if field not in EXCLUDED_FIELDS
                ]
                return th.ArrayType(th.ObjectType(*sub_props))
            if "AdsHistogramStats" in d_type:
                sub_props = []
                for field in list(AdsHistogramStats.Field.__dict__):
                    if field not in EXCLUDED_FIELDS:
                        clean_field = field.replace("field_", "")
                        if AdsHistogramStats._field_types[clean_field] == "string":  # noqa: SLF001
                            sub_props.append(th.Property(clean_field, th.StringType()))
                        else:
                            sub_props.append(
                                th.Property(
                                    clean_field,
                                    th.ArrayType(th.IntegerType()),
                                ),
                            )
                return th.ArrayType(th.ObjectType(*sub_props))
            return th.ArrayType(th.ObjectType())
        msg = f"Type not found for field: {field}"
        raise RuntimeError(msg)

    @property
    @lru_cache  # noqa: B019
    def schema(self) -> dict:
        properties: th.List[th.Property] = []
        properties.append(th.Property("id", th.StringType()))
        columns = list(AdsInsights.Field.__dict__)[1:]
        for field in columns:
            if field in EXCLUDED_FIELDS:
                continue
            properties.append(th.Property(field, self._get_datatype(field)))
        for breakdown in self._report_definition["breakdowns"]:
            properties.append(th.Property(breakdown, th.StringType()))
        return th.PropertiesList(*properties).to_dict()

    def _initialize_client(self) -> None:
        self.facebook_api = FacebookAdsApi.init(
            access_token=self.config["access_token"],
            timeout=300,
            api_version=self.config["api_version"],
        )
        self.facebook_id = fb_user.User(fbid="me")

        account_id = self.config["account_id"]
        self.account = AdAccount(f"act_{account_id}").api_get()
        if not self.account:
            msg = f"Couldn't find account with id {account_id}"
            raise RuntimeError(msg)

    def _check_facebook_api_usage(self, headers: str, account_id: str) -> None:
        total_time_to_regain_access = json.loads(headers.get("x-business-use-case-usage"))[account_id][0].get(
            "estimated_time_to_regain_access"
        )

        self.logger.info(
            "Total time to regain access is %s seconds.",
            total_time_to_regain_access,
        )

        if total_time_to_regain_access > 0:
            self.logger.info(
                " ZZzzzzzzZZz - Sleeping for %s seconds until API limit is cleared.",
                total_time_to_regain_access,
            )
            time.sleep(total_time_to_regain_access)

    def _trigger_async_insight_report_creation(self, account_id: str, params: dict) -> th.Any:

        request = FacebookRequest(
            node_id=f"act_{account_id}",
            method="POST",
            endpoint="/insights",
            api_type="EDGE",
            include_summary=False,
            api=FacebookAdsApi.get_default_api(),
        )

        request.add_params(params)

        return request.execute()

    def _run_job_to_completion(self, report_instance: AdReportRun, report_date: str) -> th.Any:
        status = None
        time_start = time.time()

        while status != "Job Completed":
            duration = time.time() - time_start
            job = report_instance.api_get()
            status = job[AdReportRun.Field.async_status]
            percent_complete = job[AdReportRun.Field.async_percent_completion]

            job_id = job["id"]
            self.logger.info(
                "ID: %s - %s for %s - %s%% done. ",
                job_id,
                status,
                report_date,
                percent_complete,
            )

            if status == "Job Completed":
                return job
            if status == "Job Failed":
                self.logger.info(
                    "Insights job %s failed, trying again in a minute." + JOB_STALE_ERROR_MESSAGE,
                    job_id,
                )
                return
            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                self.logger.info(
                    "Insights job %s did not start after %s seconds." + JOB_STALE_ERROR_MESSAGE,
                    job_id,
                    duration,
                )
                return
            if duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS:
                self.logger.info(
                    "Insights job %s did not complete after %s seconds",
                    job_id,
                    INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS,
                )
                return

            self.logger.info(
                "Sleeping for %s seconds until job is done",
                POLL_JOB_SLEEP_TIME,
            )
            time.sleep(POLL_JOB_SLEEP_TIME)
        msg = "Job failed to complete for unknown reason"
        raise RuntimeError(msg)

    def _get_selected_columns(self) -> list[str]:
        columns = [keys[1] for keys, data in self.metadata.items() if data.selected and len(keys) > 0]
        if not columns:
            columns = list(self.schema["properties"])

        # pop ID, since it's auto-generated
        if "id" in columns:
            columns.remove("id")

        return columns

    def _get_start_date(
        self,
        context: dict | None,
    ) -> pendulum.Date:
        lookback_window = self._report_definition["lookback_window"]

        config_start_date = pendulum.parse(self.config["start_date"]).date()
        incremental_start_date = pendulum.parse(
            self.get_starting_replication_key_value(context),
        ).date()
        lookback_start_date = incremental_start_date.subtract(days=lookback_window)

        # Don't use lookback if this is the first sync. Just start where the user requested.
        if config_start_date >= incremental_start_date:
            report_start = config_start_date
            self.logger.info("Using configured start_date as report start filter %s.", report_start)
        else:
            self.logger.info(
                "Incremental sync, applying lookback '%s' to the "
                "bookmark start_date '%s'. Syncing "
                "reports starting on '%s'.",
                lookback_window,
                incremental_start_date,
                lookback_start_date,
            )
            report_start = lookback_start_date

        # Facebook store metrics maximum of 37 months old. Any time range that
        # older that 37 months from current date would result in 400 Bad request
        # HTTP response.
        # https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
        today = pendulum.today().date()
        oldest_allowed_start_date = today.subtract(months=37)
        if report_start < oldest_allowed_start_date:
            report_start = oldest_allowed_start_date
            self.logger.info(
                "Report start date '%s' is older than 37 months. " "Using oldest allowed start date '%s' instead.",
                report_start,
                oldest_allowed_start_date,
            )
        return report_start

    def _generate_hash_id(self, adinsight: AdsInsights):
        # Extract the relevant properties from the AdsInsights object
        date_start = adinsight.get("date_start", "")
        campaign_id = adinsight.get("campaign_id", "")
        adset_id = adinsight.get("adset_id", "")
        ad_id = adinsight.get("ad_id", "")

        # Concatenate the properties into a string
        properties_string = f"{date_start}-{campaign_id}-{adset_id}-{ad_id}"

        # Create an MD5 hash from the concatenated string
        hash_object = md5(properties_string.encode())

        # Return the hexadecimal representation of the hash
        return hash_object.hexdigest()

    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        self._initialize_client()

        time_increment = self._report_definition["time_increment_days"]

        sync_end_date = pendulum.parse(
            self.config.get("end_date", pendulum.today().to_date_string()),
        ).date()

        report_date = self._get_start_date(context)
        columns = self._get_selected_columns()

        while report_date <= sync_end_date:
            params = {
                "level": self._report_definition["level"],
                "action_breakdowns": self._report_definition["action_breakdowns"],
                "action_report_time": self._report_definition["action_report_time"],
                "breakdowns": self._report_definition["breakdowns"],
                "fields": columns,
                "time_increment": time_increment,
                "limit": 100,
                "action_attribution_windows": [
                    self._report_definition["action_attribution_windows_view"],
                    self._report_definition["action_attribution_windows_click"],
                ],
                "time_range": {
                    "since": report_date.to_date_string(),
                    "until": report_date.to_date_string(),
                },
            }

            try:
                response = self._trigger_async_insight_report_creation(
                    params=params, account_id=self.config["account_id"]
                )

                if response._http_status != 200:
                    self._check_facebook_api_usage(headers=response._headers, account_id=self.config["account_id"])
                    continue

                report_run_id = response.json()["report_run_id"]
                job = self._run_job_to_completion(
                    report_instance=AdReportRun(report_run_id),
                    report_date=report_date.to_date_string(),
                )

                if not isinstance(job, AdReportRun):
                    # retry if facebook job report generation got stuck
                    time.sleep(AD_REPORT_RETRY_TIME)
                    continue

                for obj in job.get_result():
                    if isinstance(obj, AdsInsights):
                        obj["id"] = self._generate_hash_id(adinsight=obj)
                        yield obj.export_all_data()
                    else:
                        # stop the for loop and retry the same date after a while
                        time.sleep(AD_REPORT_RETRY_TIME)
                        break
                else:
                    # bump to the next increment
                    time.sleep(AD_REPORT_INCREMENT_SLEEP_TIME)
                    report_date = report_date.add(days=time_increment)

            except FacebookRequestError as fb_err:
                if fb_err.api_error_code == HTTPStatus.BAD_REQUEST and "unsupported get request" in str(
                    fb_err.api_error_message.lower()
                ):
                    self.logger.warning(f"API Error: {fb_err.api_error_message()}. Trying again..")
                    continue

                self.logger.warning(f"An unhandled error occurred: {fb_err}. Stopping execution.")
                raise FatalAPIError(fb_err)

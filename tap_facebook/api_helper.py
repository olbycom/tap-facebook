import json
from logging import Logger
from time import sleep

CALL_THRESHOLD_PERCENTAGE = 90


def get_usage_headers(headers: dict, account_id: str, logger: Logger):
    logger.info(f"API Usage | Response headers: {headers}")

    # Facebook may return this information in two different ways
    app_usage = headers.get("X-App-Usage") or headers.get("x-app-usage") or {}
    ad_account_usage = headers.get("X-Ad-Account-Usage") or headers.get("x-ad-account-usage") or {}
    business_case_usage = headers.get("X-Business-Use-Case-Usage") or headers.get("x-business-use-case-usage") or {}

    if app_usage:
        app_usage = json.loads(app_usage)
        logger.info(f"API Usage | X-App-Usage: {app_usage}")

    if ad_account_usage:
        ad_account_usage = json.loads(ad_account_usage)
        logger.info(f"API Usage | X-Ad-Account-Usage: {ad_account_usage}")

    if business_case_usage:
        usage_list = json.loads(business_case_usage)
        if account_id not in usage_list:
            business_case_usage = {}
        for entry in usage_list.get(account_id, []):
            if entry.get("type") in ["ads_management", "ads_insights", "custom_audience"]:
                business_case_usage = entry
                logger.info(f"API Usage | X-Business-Use-Case-Usage: {business_case_usage}")

    return app_usage, ad_account_usage, business_case_usage


def has_reached_api_limit(headers: dict, account_id: str, logger: Logger) -> bool:
    app_usage, ad_account_usage, business_case_usage = get_usage_headers(
        headers=headers, account_id=account_id, logger=logger
    )
    if app_usage or ad_account_usage or business_case_usage:
        call_count = max(app_usage.get("call_count", 0), business_case_usage.get("call_count", 0))
        total_cputime = max(app_usage.get("total_cputime", 0), business_case_usage.get("total_cputime", 0))
        total_time = max(app_usage.get("total_time", 0), business_case_usage.get("total_time", 0))
        acc_id_util_pct = ad_account_usage.get("acc_id_util_pct", 0)
        estimated_time_to_regain_access = (
            int(business_case_usage.get("estimated_time_to_regain_access", 0)) * 60
        )  # This time is in minutes according to the docs
        reset_time_duration = int(ad_account_usage.get("reset_time_duration", 0))

        logger.warning(
            f"API Usage | Call Count: {call_count}%, CPU Time: {total_cputime}%, Total Time: {total_time}%, Ad Account Usage: {acc_id_util_pct}%"
        )
        logger.info(
            f"API Usage | Estimated time to regain access (BUC): {estimated_time_to_regain_access}s, Reset time duration (Ad Account): {reset_time_duration}s"
        )

        over_quota_sleep_time = max(estimated_time_to_regain_access, reset_time_duration)
        if over_quota_sleep_time > 0:
            # quota already reached, let's wait for the suggested time and then go back to making requests
            logger.warning(f"API Usage | Rate limit reached, sleeping for {over_quota_sleep_time}s.")
            sleep(over_quota_sleep_time)
            return False

        if max(call_count, acc_id_util_pct) > CALL_THRESHOLD_PERCENTAGE:
            return True
        else:
            return False
    else:
        logger.warning("API Usage | No usage data found in headers.")
        return False

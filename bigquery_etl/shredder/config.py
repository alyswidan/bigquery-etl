#!/usr/bin/env python3

"""Meta data about tables and ids for self serve deletion."""

from datetime import date, datetime, timedelta
from dataclasses import dataclass, field, replace
from typing import List, Union


# Use a constant because "submission_timestamp" is easy for relud to misspell
SUBMISSION_TIMESTAMP = "submission_timestamp"
SUBMISSION_DATE = "submission_date"
DEFAULT_DATE_CONDITION = f"DATE(request.{SUBMISSION_TIMESTAMP}) <= CURRENT_DATE"


@dataclass
class DeleteItem:
    request_table: str
    request_id: str
    request_date_condition: str
    target_table: str
    target_id: str
    target_date_conditions: List[str]


def date_range(start, end=datetime.utcnow().date(), step=timedelta(days=1)):
    return (start + step * i for i in range((end - start) // step))


ID_PRIORITY = [
    CLIENT_ID := "client_id",
    GLEAN_CLIENT_ID := "client_info.client_id",
    IMPRESSION_ID := "impression_id",
    USER_ID := "user_id",
    POCKET_ID := "pocket_id",
    SHIELD_ID := "shield_id",
    ECOSYSTEM_CLIENT_ID := "payload.ecosystem_client_id",
    PIONEER_ID := "payload.pioneer_id",
    ID := "id",
]

DELETE_ITEMS = [
    desktop := DeleteItem(
        request_table="telemetry_stable.deletion_request_v4",
        request_id=CLIENT_ID,
        request_date_condition=f"DATE(request.{SUBMISSION_TIMESTAMP}) BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE",
        target_table="telemetry_derived.attitudes_daily_v1",
        target_id=CLIENT_ID,
        target_date_conditions=[f"DATE(target.{SUBMISSION_TIMESTAMP}) <= CURRENT_DATE"],
    ),
    sub_date_desktop := replace(
        desktop,
        target_table="search_derived.mobile_search_clients_daily_v1",
        target_date_conditions=[f"target.{SUBMISSION_DATE} <= CURRENT_DATE"],
    ),
    replace(
        sub_date_desktop, target_table="search_derived.mobile_search_clients_daily_v1"
    ),
    replace(sub_date_desktop, target_table="search_derived.search_clients_daily_v8"),
    replace(
        sub_date_desktop, target_table="search_derived.search_clients_last_seen_v1"
    ),
    replace(
        sub_date_desktop,
        target_table="telemetry_derived.clients_daily_histogram_aggregates_v1",
    ),
    replace(
        sub_date_desktop,
        target_table="telemetry_derived.clients_daily_scalar_aggregates_v1",
    ),
    replace(sub_date_desktop, target_table="telemetry_derived.clients_daily_v6"),
    replace(desktop, target_table="telemetry_derived.clients_histogram_aggregates_v1"),
    replace(sub_date_desktop, target_table="telemetry_derived.clients_last_seen_v1"),
    replace(
        desktop,
        target_table="telemetry_derived.clients_profile_per_install_affected_v1",
    ),
    replace(desktop, target_table="telemetry_derived.clients_scalar_aggregates_v1"),
    replace(sub_date_desktop, target_table="telemetry_derived.core_clients_daily_v1"),
    replace(
        sub_date_desktop, target_table="telemetry_derived.core_clients_last_seen_v1"
    ),
    replace(desktop, target_table="telemetry_derived.core_live"),
    replace(desktop, target_table="telemetry_derived.event_events_v1"),
    replace(desktop, target_table="telemetry_derived.events_live"),
    replace(desktop, target_table="telemetry_derived.experiments_v1"),
    replace(desktop, target_table="telemetry_derived.main_events_v1"),
    replace(
        sub_date_desktop,
        target_table="telemetry_derived.main_summary_v4",
        target_date_conditions=[
            # use a separate delete query for each day
            f"target.{SUBMISSION_DATE} = '{day}'"
            # main summary has data back to march 2016
            for day in date_range(date(2016, 3, 12))
        ],
    ),
    replace(desktop, target_table="telemetry_stable.block_autoplay_v1"),
    replace(desktop, target_table="telemetry_stable.crash_v4"),
    replace(desktop, target_table="telemetry_stable.downgrade_v4"),
    replace(desktop, target_table="telemetry_stable.event_v4"),
    replace(desktop, target_table="telemetry_stable.first_shutdown_v4"),
    replace(desktop, target_table="telemetry_stable.focus_event_v1"),
    replace(desktop, target_table="telemetry_stable.frecency_update_v4"),
    replace(desktop, target_table="telemetry_stable.health_v4"),
    replace(desktop, target_table="telemetry_stable.heartbeat_v4"),
    replace(
        desktop,
        target_table="telemetry_stable.main_v4",
        target_date_conditions=[
            # use a separate delete query for each day
            f"DATE(target.{SUBMISSION_TIMESTAMP}) = '{day}'"
            # main_v4 has data back to november 2018
            for day in date_range(date(2018, 11, 1))
        ],
    ),
    replace(desktop, target_table="telemetry_stable.modules_v4"),
    replace(desktop, target_table="telemetry_stable.new_profile_v4"),
    replace(desktop, target_table="telemetry_stable.saved_session_v4"),
    replace(desktop, target_table="telemetry_stable.shield_icq_v1_v4"),
    replace(desktop, target_table="telemetry_stable.shield_study_addon_v3"),
    replace(desktop, target_table="telemetry_stable.shield_study_error_v3"),
    replace(desktop, target_table="telemetry_stable.shield_study_v3"),
    replace(desktop, target_table="telemetry_stable.testpilot_v4"),
    replace(desktop, target_table="telemetry_stable.third_party_modules_v4"),
    replace(desktop, target_table="telemetry_stable.untrusted_modules_v4"),
    replace(desktop, target_table="telemetry_stable.update_v4"),
    replace(desktop, target_table="telemetry_stable.voice_v4"),
    # TODO these should probably be deleted
    replace(desktop, target_table="telemetry_derived.test3"),
    replace(desktop, target_table="telemetry_derived.test4"),
    replace(desktop, target_table="telemetry_derived.test_daily_original"),
    replace(desktop, target_table="telemetry_derived.test_histogram_aggregates"),
    replace(desktop, target_table="telemetry_derived.test_histogram_daily"),
    replace(desktop, target_table="telemetry_derived.test_scalar_aggregates"),
    replace(desktop, target_table="telemetry_derived.test_scalars_daily"),
    replace(
        sub_date_desktop, target_table="search_derived.search_clients_last_seen_v101*"
    ),
    # activity stream
    activity_stream := replace(
        desktop,
        request_table=desktop.request_table,
        request_id="payload.processes.parent.scalars.deletion_request_impression_id",
        target_table="activity_stream_stable.impression_stats_v1",
        target_id=IMPRESSION_ID,
    ),
    replace(activity_stream, target_table="activity_stream_stable.spoc_fills_v1"),
    # TODO check if this table's impression_id is from activity stream
    replace(
        activity_stream, target_table="messaging_system_stable.undesired_events_v1"
    ),
]

UNSUPPORTED = [
    # pocket
    replace(
        desktop, target_table="pocket_stable.fire_tv_events_v1", target_id=POCKET_ID
    ),
    # fxa
    fxa := replace(
        desktop, target_table="fxa_users_services_daily_v1", target_id=USER_ID
    ),
    replace(fxa, target_table="fxa_users_services_first_seen_v1"),
    replace(fxa, target_table="fxa_users_services_last_seen_v1"),
    replace(fxa, target_table="telemetry_derived.devtools_events_amplitude_v1"),
    # internal
    mobile := replace(
        desktop,
        request_id="normalized_app_name != 'Firefox' AND {CLIENT_ID}",
        target_table="activity_stream_stable.events_v1",
    ),
    replace(mobile, target_table="eng_workflow_stable.build_v1"),
    replace(mobile, target_table="messaging_system_stable.cfr_v1"),
    replace(mobile, target_table="messaging_system_stable.onboarding_v1"),
    replace(mobile, target_table="messaging_system_stable.snippets_v1"),
    # mobile
    replace(mobile, target_table="mobile_stable.activation_v1"),
    replace(mobile, target_table="telemetry_stable.core_v1"),
    replace(mobile, target_table="telemetry_stable.core_v2"),
    replace(mobile, target_table="telemetry_stable.core_v3"),
    replace(mobile, target_table="telemetry_stable.core_v4"),
    replace(mobile, target_table="telemetry_stable.core_v5"),
    replace(mobile, target_table="telemetry_stable.core_v6"),
    replace(mobile, target_table="telemetry_stable.core_v7"),
    replace(mobile, target_table="telemetry_stable.core_v8"),
    replace(mobile, target_table="telemetry_stable.core_v9"),
    replace(mobile, target_table="telemetry_stable.core_v10"),
    replace(mobile, target_table="telemetry_stable.mobile_event_v1"),
    replace(mobile, target_table="telemetry_stable.mobile_metrics_v1"),
    # glean
    fenix := replace(
        desktop,
        request_table="org_mozilla_fenix_stable.deletion_request_v1",
        request_id=GLEAN_CLIENT_ID,
        target_table="org_mozilla_fenix_stable.activation_v1",
        target_id=GLEAN_CLIENT_ID,
    ),
    replace(fenix, target_table="org_mozilla_fenix_stable.baseline_v1"),
    replace(fenix, target_table="org_mozilla_fenix_stable.bookmarks_sync_v1"),
    replace(fenix, target_table="org_mozilla_fenix_stable.events_v1"),
    replace(fenix, target_table="org_mozilla_fenix_stable.history_sync_v1"),
    replace(fenix, target_table="org_mozilla_fenix_stable.logins_sync_v1"),
    replace(fenix, target_table="org_mozilla_fenix_stable.metrics_v1"),
    replace(
        fenix,
        target_id=CLIENT_ID,
        target_table="org_mozilla_fenix_derived.clients_daily_v1",
        target_date_conditions=sub_date_desktop.target_date_conditions,
    ),
    replace(
        fenix,
        target_id=CLIENT_ID,
        target_table="org_mozilla_fenix_derived.clients_last_seen_v1",
        target_date_conditions=sub_date_desktop.target_date_conditions,
    ),
    fenix_nightly := replace(
        fenix,
        request_table="org_mozilla_fenix_nightly_stable.deletion_request_v1",
        target_table="org_mozilla_fenix_nightly_stable.activation_v1",
    ),
    replace(fenix_nightly, target_table="org_mozilla_fenix_nightly_stable.baseline_v1"),
    replace(
        fenix_nightly, target_table="org_mozilla_fenix_nightly_stable.bookmarks_sync_v1"
    ),
    replace(fenix_nightly, target_table="org_mozilla_fenix_nightly_stable.events_v1"),
    replace(
        fenix_nightly, target_table="org_mozilla_fenix_nightly_stable.history_sync_v1"
    ),
    replace(
        fenix_nightly, target_table="org_mozilla_fenix_nightly_stable.logins_sync_v1"
    ),
    replace(fenix_nightly, target_table="org_mozilla_fenix_nightly_stable.metrics_v1"),
    ref_browser := replace(
        fenix,
        request_table="org_mozilla_reference_browser_stable.deletion_request_v1",
        target_table="org_mozilla_reference_browser_stable.baseline_v1",
    ),
    replace(ref_browser, target_table="org_mozilla_reference_browser_stable.events_v1"),
    replace(
        ref_browser, target_table="org_mozilla_reference_browser_stable.metrics_v1"
    ),
    tv_firefox := replace(
        fenix,
        request_table="org_mozilla_tv_firefox_stable.deletion_request_v1",
        target_table="org_mozilla_tv_firefox_stable.baseline_v1",
    ),
    replace(tv_firefox, target_table="org_mozilla_tv_firefox_stable.events_v1"),
    replace(tv_firefox, target_table="org_mozilla_tv_firefox_stable.metrics_v1"),
    vrbrowser := replace(
        fenix,
        request_table="org_mozilla_vrbrowser_stable.deletion_request_v1",
        target_table="org_mozilla_vrbrowser_stable.baseline_v1",
    ),
    replace(vrbrowser, target_table="org_mozilla_vrbrowser_stable.bookmarks_sync_v1"),
    replace(vrbrowser, target_table="org_mozilla_vrbrowser_stable.events_v1"),
    replace(vrbrowser, target_table="org_mozilla_vrbrowser_stable.history_sync_v1"),
    replace(vrbrowser, target_table="org_mozilla_vrbrowser_stable.logins_sync_v1"),
    replace(vrbrowser, target_table="org_mozilla_vrbrowser_stable.metrics_v1"),
    replace(vrbrowser, target_table="org_mozilla_vrbrowser_stable.session_end_v1"),
    # other
    replace(
        desktop, target_id=PIONEER_ID, target_table="telemetry_stable.pioneer_study_v4"
    ),
    replace(
        desktop,
        target_id=ECOSYSTEM_CLIENT_ID,
        target_table="telemetry_stable.pre_account_v4",
    ),
    # TODO evaluate whether these are actually user ids
    replace(
        desktop,
        target_id=SHIELD_ID,
        target_table="telemetry_derived.survey_gizmo_daily_attitudes",
    ),
    root_id := replace(
        desktop,
        target_id=ID,
        target_table="firefox_launcher_process_stable.launcher_process_failure_v1",
    ),
    replace(root_id, target_table="telemetry_derived.origin_content_blocking"),
    replace(root_id, target_table="telemetry_stable.anonymous_v4"),
    replace(root_id, target_table="telemetry_stable.optout_v4"),
    replace(root_id, target_table="telemetry_stable.pre_account_v4"),
    replace(root_id, target_table="telemetry_stable.prio_v4"),
    replace(root_id, target_table="telemetry_stable.sync_v4"),
    replace(root_id, target_table="telemetry_stable.sync_v5"),
]

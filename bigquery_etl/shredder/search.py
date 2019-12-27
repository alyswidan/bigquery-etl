#!/usr/bin/env python3

"""Search for tables and user ids that may be eligible for self serve deletion."""

from itertools import chain
import re

from google.cloud import bigquery

from .config import ID_PREFERENCE, DELETE_ITEMS


KNOWN_IDS = {
    known_id
    for item in chain(DELETE_ITEMS, UNSUPPORTED)
    for known_id in [
        (item.target_table, item.target_id),
        (item.request_table, item.request_id),
    ]
}

# TODO this should move to config.py
ID_PATTERN = re.compile("(?!(" + "|".join(
    "activation_id",
    "addon_id",
    "application_id",
    "batch_id",
    "bucket_id",
    "((x_)?de)?bug_id",
    "build_id",
    "campaign_id",
    "changeset_id",
    "crash_id",
    "device_id",
    "distribution_id",
    "document_id",
    "error_id",
    "experiment_id",
    "extension_id",
    "encryption_key_id",
    "insert_id",
    "message_id",
    "model_id",
    "network_id",
    "page_id",
    "partner_id",
    "product_id",
    "run_id",
    "setter_id",
    "survey_id",
    "sample_id",
    "(sub)?session_id",
    "subsys(tem)?_id",
    "thread_id",
    "vendor_id",
    "id_bucket",
    r"active_experiment\.id",
    r"theme\.id",
    r"tiles\[]\.id",
    r"spoc_fills\[]\.id",
    r"devices\[]\.id",
    # these prefixes need to be evaluated
    "enrollment_id",  # for experiments
    "flow_id",
    "intent_id",
    "requestee_id",
) + r")?)(.*_)?id(_.*)?")
DATASET_PATTERN = re.compile(".*_(stable|decoded)")


def find_ids(fields, prefix=""):
    for field in fields:
        if field.field_type == "RECORD":
            prefix += field.name + ("[]" if field.mode == "REPEATED" else "") + "."
            yield from find_ids(field.fields, prefix)
        elif ID_PATTERN.search(id_field := prefix + field.name):
            yield id_field


def id_priority(id_field):
    """Get sort priority for id_field, where lower is better."""
    try:
        return ID_PREFERENCE.index(id_field)
    except ValueError:
        return len(ID_PREFERENCE)


def find_target_tables():
    client = bigquery.Client()
    for dataset in client.list_datasets(SHARED_PROD):
        if not DATASET_PATTERN.match(dataset.dataset_id):
            continue
        for table_ref in client.list_tables(dataset.reference):
            table = client.get_table(table_ref)
            full_table_id = table.full_table_id.replace(":", ".")
            if full_table_id in REQUESTS_TO_TARGETS:
                continue
            ids = sorted(find_ids(table.schema)), key=id_sort_key)
            if not ids:
                continue
            target_table = table.full_table_id.split(":", 1)[-1]
            for target_id in ids:
                if (target_table, target_id) not in KNOWN_IDS:
                    yield target_table, target_id


def main():
    for target_table, ids in find_target_tables():
        for target_id in fields:
            print(f"target_table={target_table!r}, target_id={target_id!r}")


if __name__ == "__main__":
    main()

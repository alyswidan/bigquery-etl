#!/usr/bin/env python3

"""Delete user data from long term storage."""

from argparse import ArgumentParser
from dataclasses import asdict
from multiprocessing.pool import ThreadPool

from google.cloud import bigquery

from .config import DELETE_ITEMS


parser = ArgumentParser(description=__doc__)
parser.add_argument("--dry_run", "--dry-run", action="store_true", default=False)
parser.add_argument("--verbose", "-v", action="store_true", default=False)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument(
    "--parallelism",
    default=4,
    type=int,
    help="Maximum number of queries to execute concurrently",
)

# TODO it may not work to use DML here for main and main summary, in which case we will need to select one sample id at a time into correctly clustered temp tables and then copy-join the temp tables to overwrite the original, like we do in script/copy_deduplicate
DELETE_TEMPLATE = """
MERGE
  `{target_table}` AS target
USING
  `{request_table}` AS request
ON
  request.{request_id} = target.{target_id}
  AND {request_date_condition}
  AND {target_date_condition}
WHEN MATCHED THEN DELETE
"""


def run_query(client, query, dry_run):
    job = client.query(query, bigquery.QueryJobConfig(dry_run=dry_run))
    if not dry_run:
        job.result()
    return job.total_bytes_processed


def handle_delete_item(client, pool, item, dry_run, verbose):
    bytes_processed = bytes_deleted = 0
    if not dry_run:
        table_bytes_deleted = client.get_table(item.target_table).num_bytes
    queries = [
        DELETE_TEMPLATE.format(
            target_date_condition=target_date_condition, **asdict(item),
        )
        for target_date_condition in item.target_date_conditions
    ]
    if verbose:
        run_tense = "Would run" if dry_run else "Running"
        for query in queries:
            print(f"{run_tense} query:")
            print(query)
    # TODO test that multiple merge statements can run at once
    bytes_processed = sum(pool.starmap(
        run_query,
        [(client, query, dry_run) for query in queries],
        chunksize=1,
    ))
    if dry_run:
        print(f"Would scan {bytes_processed} bytes from {item.target_table}")
    else:
        bytes_deleted -= client.get_table(item.target_table).num_bytes
        print(
            f"Scanned {bytes_processed} bytes and "
            f"deleted {bytes_deleted} from {item.target_table}"
        )
    return bytes_processed, bytes_deleted


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)
    total_bytes_processed = table_bytes_deleted = 0
    with ThreadPool(args.parallelism) as pool:
        result = pool.starmap(
            handle_delete_item,
            [
                (client, pool, item, args.dry_run, args.verbose)
                for item in DELETE_ITEMS
                if item.target_table == "telemetry_stable.main_v4"
            ],
            chunksize=1,
        )
    bytes_processed = sum(r[0] for r in result)
    bytes_deleted = sum(r[1] for r in result)
    if args.dry_run:
        print(f"Would scan {bytes_processed} in total")
    else:
        print(
            f"Scanned {bytes_processed} and "
            f"deleted {bytes_deleted} in total"
        )


if __name__ == "__main__":
    main()

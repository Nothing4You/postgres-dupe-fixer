import argparse
import logging
import time
from datetime import UTC, datetime

import psycopg
from psycopg import sql

POSTGRES_HOST = "127.0.0.1"
POSTGRES_USER = "lemmy"
POSTGRES_PASSWORD = "lemmypass"  # noqa: S105
POSTGRES_DB = "lemmy"

BROKEN_TABLE = "community"
BROKEN_TABLE_PKEY = "id"
BROKEN_TABLE_UNIQUE_COLUMN = "actor_id"
BROKEN_TABLE_DUPLICATE_QUERY = """
with duplicate_actor as (
    select lower(actor_id) as actor_id from community group by lower(actor_id) having count(*) > 0 limit 1
)
select community.id as pkey, community.actor_id as unique_column
from community
join duplicate_actor on lower(community.actor_id) = duplicate_actor.actor_id
order by community.id asc
"""


def get_tables_and_row_counts(
    cur: psycopg.Cursor,
) -> dict[sql.Identifier, int]:
    start = time.perf_counter()

    # https://stackoverflow.com/a/75752699
    cur.execute("""
    select table_name
    from information_schema.tables
    where table_schema = 'public'
    """)

    table_names = cur.fetchall()

    row_counts = {}

    for table_name_row in table_names:
        table = table_name_row[0]
        cur.execute(
            sql.SQL("select count(*) from {}").format(sql.Identifier(table)),
        )
        row = cur.fetchone()
        if row is None:
            msg = f"Unable to retrieve row count for {table=}"
            raise Exception(msg)
        row_counts[table] = row[0]

    end = time.perf_counter()
    logging.info("counted rows of all tables in %.2f seconds", end - start)

    return row_counts


def get_foreign_key_references(
    cur: psycopg.Cursor,
    table: str,
    column: str,
) -> set[tuple[str, str]]:
    # https://stackoverflow.com/a/21125640
    query = sql.SQL("""
    select (select r.relname from pg_class r where r.oid = c.conrelid) as table,
           (select array_agg(attname) from pg_attribute
            where attrelid = c.conrelid and ARRAY[attnum] <@ c.conkey) as col
    from pg_constraint c
    where c.confrelid = (select oid from pg_class where relname = %s) and
          c.confkey @> (select array_agg(attnum) from pg_attribute
                        where attname = %s and attrelid = c.confrelid);
    """)

    cur.execute(query, (table, column))
    rows = cur.fetchall()

    foreign_keys = set()

    for row in rows:
        for referencing_column in row[1]:
            foreign_keys.add((row[0], referencing_column))

    return foreign_keys


def get_duplicate_batch(
    cur: psycopg.Cursor,
) -> list[tuple[int, str]]:
    cur.execute(BROKEN_TABLE_DUPLICATE_QUERY)
    rows = cur.fetchall()

    return [(row[0], row[1]) for row in rows] + [(250_000, "test")]


def main() -> None:
    parser = argparse.ArgumentParser()
    # parser.add_argument("--host")
    # parser.add_argument("--port", type=int)
    # parser.add_argument("--path")
    args = parser.parse_args()
    _ = args

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)8s - %(name)s:%(funcName)s - %(message)s",
    )

    logging.Formatter.formatTime = (  # type: ignore[method-assign]
        lambda self,  # type: ignore[assignment,misc] # noqa: ARG005
        record,
        datefmt: datetime.fromtimestamp(  # noqa: ARG005
            record.created,
            UTC,
        )
        .astimezone()
        .isoformat()
    )

    broken_table = sql.Identifier(BROKEN_TABLE)
    broken_table_pkey = sql.Identifier(BROKEN_TABLE_PKEY)

    with psycopg.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        cur: psycopg.Cursor = conn.cursor()

        foreign_keys = get_foreign_key_references(cur, BROKEN_TABLE, BROKEN_TABLE_PKEY)
        logging.debug("%r", foreign_keys)

        # This could trade some safety for improved speed by skipping counting rows in tables that don't have foreign key references instead of counting rows in all tables
        expected_row_counts = get_tables_and_row_counts(cur)

        while len(duplicates := get_duplicate_batch(cur)) > 0:
            logging.info(
                "Processing %s duplicates of %s",
                len(duplicates),
                duplicates[0][1],
            )
            primary = duplicates[0][0]
            duplicate_ids = [d[0] for d in duplicates[1:]]
            logging.info(
                "considering %s as primary id, duplicates are %s",
                primary,
                duplicate_ids,
            )

            updated_rows = 0

            for fk_table, fk_column in foreign_keys:
                cur.execute(
                    sql.SQL("update {} set {} = %s where {} = any(%s)").format(
                        sql.Identifier(fk_table),
                        sql.Identifier(fk_column),
                        sql.Identifier(fk_column),
                    ),
                    (primary, duplicate_ids),
                )
                logging.info(
                    'updated "%s"."%s" in %s rows',
                    fk_table,
                    fk_column,
                    cur.rowcount,
                )
                updated_rows += cur.rowcount

            cur.execute(
                sql.SQL("delete from {} where {} = any(%s)").format(
                    broken_table,
                    broken_table_pkey,
                ),
                (duplicate_ids,),
            )
            if len(duplicate_ids) != cur.rowcount:
                conn.rollback()
                msg = f"Attempted to delete {len(duplicate_ids)} rows, deleted {cur.rowcount} rows"
                raise Exception(msg)

            expected_row_counts[broken_table] -= cur.rowcount

            current_row_counts = get_tables_and_row_counts(cur)

            if current_row_counts != expected_row_counts:
                differences = current_row_counts.copy()
                for table in expected_row_counts:
                    if table not in differences:
                        differences[table] = -expected_row_counts[table]
                    elif expected_row_counts[table] == differences[table]:
                        del differences[table]
                    else:
                        differences[table] -= expected_row_counts[table]

                conn.rollback()
                msg = f"Unexpected row counts after updates, changes have been rolled back: {differences=}"
                raise Exception(msg)

            conn.commit()
            logging.info(
                "%s duplicates have been deleted, changes have been committed to the database",
            )


main()

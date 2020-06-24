from sql_queries import create_table_queries, drop_table_queries
from airflow.hooks.postgres_hook import PostgresHook

import logging

log = logging.getLogger(__name__)

def postgres_dwh_conn():
    # create connection to postgres
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh", schema="test")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    return cur, conn


def drop_tables():
    # drop all tables from the database
    cur, conn = postgres_dwh_conn()

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    conn.close()

    log.info('Done dopping tables')


def create_tables():
    # create new tables schemas
    cur, conn = postgres_dwh_conn()

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

    conn.close()

    log.info('Done creating tables')
"""
db.py

Single place that knows how to connect to the pipeline's Postgres
database. Reads PIPELINE_POSTGRES_* env vars, which docker-compose.yml
sets for every service that talks to the DB.

Never hardcode credentials here. .env.example documents the variables;
.env holds the real values (gitignored).
"""

import os
from contextlib import contextmanager

import psycopg2


def _conn_params() -> dict:
    return {
        "host": os.environ["PIPELINE_POSTGRES_HOST"],
        "port": int(os.environ.get("PIPELINE_POSTGRES_PORT", 5432)),
        "dbname": os.environ["PIPELINE_POSTGRES_DB"],
        "user": os.environ["PIPELINE_POSTGRES_USER"],
        "password": os.environ["PIPELINE_POSTGRES_PASSWORD"],
    }


@contextmanager
def get_connection():
    """Yields a psycopg2 connection, committing on clean exit and rolling
    back on exception. Always closes."""
    conn = psycopg2.connect(**_conn_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
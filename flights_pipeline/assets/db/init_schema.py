from dagster import asset
import psycopg2
import os, pandas as pd
from dotenv import load_dotenv

# load pg.env
load_dotenv("pg.env")

@asset
def create_postgres_tables():
    """Create PostgreSQL tables from schema.sql file."""
    # Connexion à PostgreSQL avec les variables de pg.env
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "flights_db"),
        user=os.getenv("POSTGRES_USER", "dag_user"),
        password=os.getenv("POSTGRES_PASSWORD", "dagster")
    )
    cur = conn.cursor()

    # Lire et exécuter les instructions SQL
    with open(os.path.join(os.path.dirname(__file__), "schema.sql"), "r") as f:
        sql = f.read()
        cur.execute(sql)
        print("PostgreSQL tables created successfully.")

    conn.commit()
    cur.close()
    conn.close()

    return "PostgreSQL tables created."
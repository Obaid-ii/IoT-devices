# db_util.py
import psycopg2

def get_postgres_connection():
    return psycopg2.connect(database="postgres", user="postgres", password="obaid", host="localhost", port="5432")

def close_postgres_connection(conn):
    if conn:
        conn.close()

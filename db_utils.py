# db_utils.py
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine

def get_database_url():
    # Prefer a single DATABASE_URL env var (recommended for deployments)
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    # Otherwise build from components
    user = os.getenv("DB_USER", "sentiment_user")
    password = os.getenv("DB_PASSWORD", "")   # empty by default
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    db = os.getenv("DB_NAME", "stock_data")
    password_quoted = quote_plus(password)
    return f"mysql+mysqlconnector://{user}:{password_quoted}@{host}:{port}/{db}"

def make_engine(**kwargs):
    """
    Returns a SQLAlchemy engine using the DATABASE_URL or DB_ environment vars.
    Passes kwargs through to create_engine (e.g. pool_size, connect_args).
    """
    defaults = {"pool_pre_ping": True}
    defaults.update(kwargs or {})
    return create_engine(get_database_url(), **defaults)
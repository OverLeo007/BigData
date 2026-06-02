import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "laba6laba6laba6")

POSTGRES_USER = os.environ.get("POSTGRES_USER", "laba6")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "laba6")
SUPERSET_DB = os.environ.get("SUPERSET_DB", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@postgres:5432/{SUPERSET_DB}"
)

WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

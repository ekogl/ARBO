import os

class Config:

    DB_HOST = os.getenv("ARBO_DB_HOST", "localhost")
    DB_PORT = os.getenv("ARBO_DB_PORT", 5433)
    DB_NAME = os.getenv("ARBO_DB_NAME", "arbo_state")
    DB_USER = os.getenv("ARBO_DB_USER", "arbo_user")
    DB_PASS = os.getenv("ARBO_DB_PASS", "arbo_pass")

    AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
    AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")

    DEFAULT_STARTUP = 6.0
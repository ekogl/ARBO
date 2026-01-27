import pytest
import psycopg2
from arbo_lib.config import Config

@pytest.fixture(autouse=True)
def patch_db_config(monkeypatch):
    """
    Forces the Config class to use 'arbo_test' during tests.
    """
    monkeypatch.setattr(Config, "DB_NAME", "arbo_test")

@pytest.fixture(scope="function")
def db_clean():
    """
    Cleans the database before each test execution
    :return:
    """

    assert Config.DB_NAME == "arbo_test"

    conn = psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database="arbo_test",
        user=Config.DB_USER,
        password=Config.DB_PASS
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE task_models CASCADE;")
    yield

    conn.close()
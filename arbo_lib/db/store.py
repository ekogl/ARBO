import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from arbo_lib.config import Config
from arbo_lib.core.exceptions import TaskAlreadyExistsError, TaskNotFoundError
from arbo_lib.utils.logger import get_logger
from typing import Optional, List, Dict

logger = get_logger("arbo.db_store")

class ArboState:
    """
    This class provides methods to interact with the ARBO state DB (PostgreSQL).

    This class handles all database operations including connection management,
    transaction handling, fetching task parameters, and storing execution history.
    """

    def __init__(self):
        """
        Initialize connection parameters from config file
        """
        self.conn_params = {
            "host": Config.DB_HOST,
            "port": Config.DB_PORT,
            "database": Config.DB_NAME,
            "user": Config.DB_USER,
            "password": Config.DB_PASS
        }

    @contextmanager
    def _get_cursor(self):
        """
        Yields a cursor, handles connection, commit/rollback and closing connection
        :return:
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.conn_params)

            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    yield cur

        except psycopg2.OperationalError as e:
            logger.error("DB connection Error")
            raise e
        except psycopg2.errors.UniqueViolation as e:
            raise
        except Exception as e:
            logger.error("Unexpected error:", e)
            raise e
        finally:
            if conn:
                conn.close()

    def initialize_task(self, task_name: str, t_base: float, p: float = 1, c_startup: float = 6, alpha: float = 0.3) -> None:
        """
        Creates new db entry for a new task
        Raises TaskAlreadyExistsError if task already exists
        :param task_name:
        :param t_base:
        :param p:
        :param c_startup:
        :param alpha:
        :return:
        """
        query = """
        INSERT INTO task_models (task_name, t_base_1, p_obs, c_startup, alpha)
            VALUES (%s, %s, %s, %s, %s)
        """

        try:
            with self._get_cursor() as cur:
                cur.execute(query, (task_name, t_base, p, c_startup, alpha,))
        except psycopg2.errors.UniqueViolation as e:
            raise TaskAlreadyExistsError(f"Task {task_name} already exists in DB")

    def get_task_model(self, task_name: str) -> Optional[dict]:
        """
        Fetch baseline parameters form DB
        :param task_name: name of task
        :return: dictionary of values
        """
        query = "SELECT * FROM task_models WHERE task_name = %s"
        with self._get_cursor() as cur:
            cur.execute(query, (task_name,))
            return cur.fetchone()


    def get_history(self, task_name: str, limit: int = 50) -> List[Dict]:
        """
        Fetches history of task executions
        :param task_name: name of task
        :param limit: how many past executions to fetch
        :return:
        """
        query = "SELECT * FROM execution_history WHERE task_name = %s ORDER BY recorded_at DESC LIMIT %s"

        with self._get_cursor() as cur:
            cur.execute(query, (task_name, limit))
            return cur.fetchall()

    def update_model(self, task_name: str, new_p: float, run_data) -> None:
        """
        Updates the model (p_obs) and inserts the execution to history

        raises TaskNotFoundError if task not found

        :param task_name:
        :param new_p:
        :param run_data:
        :return:
        """

        # query for task model update
        sql_model_update = """
        UPDATE task_models 
            SET p_obs = %s,
                last_updated = CURRENT_TIMESTAMP,
                sample_count = sample_count + 1
            WHERE task_name = %s
        """

        # query for inserting execution
        sql_insert_history = """
        INSERT INTO execution_history (task_name, parallelism, input_scale_factor, cluster_load, total_duration, residual, cost_metric, p_snapshot)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self._get_cursor() as cur:
            cur.execute(sql_model_update, (new_p, task_name,))

            if cur.rowcount == 0:
                raise TaskNotFoundError(f"Task {task_name} not found in DB")

            cur.execute(sql_insert_history, (
                run_data["task_name"],
                run_data["s"],
                run_data["gamma"],
                run_data["cluster_load"],
                run_data["total_duration"],
                run_data["residual"],
                run_data["cost_metric"],
                run_data.get("p_snapshot", new_p)
            ))
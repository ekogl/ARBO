import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from arbo_lib.config import Config
from arbo_lib.core.exceptions import TaskAlreadyExistsError, TaskNotFoundError, StaleDataError
from arbo_lib.utils.logger import get_logger
from typing import Optional, List, Dict, Generator

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
    def _get_cursor(self) -> Generator[RealDictCursor, None, None]:
        """
        Yields a cursor, handles connection, commit/rollback and closing connection

        :yield: A psycopg2 RealDictCursor object
        :raises psycopg2.OperationalError: If connection to DB fails
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
        except TaskNotFoundError as e:
            raise
        except Exception as e:
            logger.error("Unexpected error:", e)
            raise e
        finally:
            if conn:
                conn.close()

    def initialize_task(
            self, task_name: str, t_base: float, base_input_quantity: float, p: float = 1,
            c_startup: float = 6, alpha_p: float = 0.7, alpha_k: float = 0.8
    ) -> None:
        """
        Creates a new db entry for a new task
        Raises TaskAlreadyExistsError if the task already exists
        :param task_name: Unique identifier for task
        :param t_base: baseline execution time
        :param base_input_quantity: baseline input size
        :param p: baseline parallelism
        :param c_startup: startup time constant
        :param alpha_p: learning rate for updating 'p'
        :param alpha_k: learning rate for updating 'k'
        :return: None
        """
        query = """
        INSERT INTO task_models (task_name, t_base_1, base_input_quantity, p_obs, c_startup, alpha_p, alpha_k, k_exponent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1.0)
        """

        try:
            with self._get_cursor() as cur:
                cur.execute(query, (task_name, t_base, base_input_quantity, p, c_startup, alpha_p, alpha_k,))
        except psycopg2.errors.UniqueViolation as e:
            raise TaskAlreadyExistsError(f"Task {task_name} already exists in DB")

    def update_baseline(self, task_name: str, new_t_base: float) -> None:
        """
        Update baseline for a specific task
        :param task_name: Unique identifier for task
        :param new_t_base: actual execution time of task
        :return: None
        """
        query = "UPDATE task_models SET t_base_1 = %s WHERE task_name = %s"
        with self._get_cursor() as cur:
            cur.execute(query, (new_t_base, task_name,))

    def get_task_model(self, task_name: str) -> Optional[Dict]:
        """
        Fetch baseline parameters from DB
        :param task_name: name of task
        :return: dictionary containing task model parameters (p, k, ...)
        """
        query = "SELECT * FROM task_models WHERE task_name = %s"
        with self._get_cursor() as cur:
            cur.execute(query, (task_name,))
            return cur.fetchone()


    def get_history(self, task_name: str, limit: int = 50) -> List[Dict]:
        """
        Fetches history of task executions
        :param task_name: name of the task
        :param limit: how many past executions to fetch
        :return: list of dictionaries representing execution rows
        """
        query = "SELECT * FROM execution_history WHERE task_name = %s ORDER BY recorded_at DESC LIMIT %s"

        with self._get_cursor() as cur:
            cur.execute(query, (task_name, limit))
            return cur.fetchall()

    def update_model(self, task_name: str, new_p: float, new_k: float, run_data: dict, expected_version: int) -> None:
        """
        Updates the model (p_obs) and inserts the execution to history
        Only updates the model if sample_count matches expected_version
        raises TaskNotFoundError if task not found

        :param task_name: Unique identifier for task
        :param new_p: new value for p
        :param new_k: new value for k
        :param run_data: dictionary containing execution metadata
        :param expected_version: expected sample_count value
        :return: None
        """

        new_p = float(new_p)
        new_k = float(new_k)

        # query for task model update
        sql_model_update = """
        UPDATE task_models 
            SET p_obs = %s,
                k_exponent = %s,
                last_updated = CURRENT_TIMESTAMP,
                sample_count = sample_count + 1
            WHERE task_name = %s
                AND sample_count = %s
        """

        # query for inserting execution
        sql_insert_history = """
        INSERT INTO execution_history (task_name, parallelism, input_scale_factor, cluster_load, total_duration, residual, cost_metric, p_snapshot, time_amdahl, pred_residual)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self._get_cursor() as cur:
            cur.execute(sql_model_update, (new_p, new_k, task_name, expected_version,))

            if cur.rowcount == 0:
                # if 0 rows affected task not found or someone else updated it in the meantime (version does not match)
                cur.execute("SELECT 1 FROM task_models WHERE task_name = %s", (task_name,))
                if cur.fetchone():
                    raise StaleDataError(f"Concurrency conflict: Task {task_name} was updated by another worker.")
                else:
                    raise TaskNotFoundError(f"Task {task_name} not found in DB")

            cur.execute(sql_insert_history, (
                str(run_data["task_name"]),
                int(run_data["s"]),
                float(run_data["gamma"]),
                float(run_data["cluster_load"]),
                float(run_data["total_duration"]),
                float(run_data["residual"]),
                float(run_data["cost_metric"]),
                float(run_data.get("p_snapshot", new_p)),
                float(run_data["time_amdahl"]),
                float(run_data["pred_residual"])
            ))
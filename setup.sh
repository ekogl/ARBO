source ~/venv/bin/activate

export ARBO_DB_HOST="localhost"
export ARBO_DB_PORT="5433"
export ARBO_DB_USER="arbo_user"
export ARBO_DB_PASS="arbo_pass"
export ARBO_DB_NAME="arbo_state"

# test connection
python3 -c "from arbo_lib.db.store import ArboState; print('DB Connected:', ArboState().get_history('test_connection'))"

airflow standalone
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Airflow Variables", "TaskFlow"],
)
def airflow_variables_example():
    @task
    def set_airflow_variable():
        """Устанавливает переменную MY_DINNER в Airflow, если она ещё не задана."""
        if Variable.get("MAGIC_VAR", default_var=None) is None:
            Variable.set("MAGIC_VAR", "Hello Moto")
        else:
            Variable.update("MAGIC_VAR", "Hurricane")

    set_airflow_variable()

airflow_variables_example()


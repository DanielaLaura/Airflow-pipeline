from __future__ import print_function
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operator.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 29),
    'end_date': datetime(2020, 1, 29)
}

dag = DAG('vars_dags',
          schedule_interval="@once",
          default_args=default_args
)

# Configure variables
dag_config = Variable.get("variables_config", deserialize_json=True)
var1 = dag_config["var1"]
var2 = dag_config["var2"]
var3 = dag_config["var3"]

start = DummyOperator(
    task_id="start",
    dag=dag
)

# to test this task, run this command:
# docker-compose run --rm webserver airflow test vars_dags get_dag_config 2020-01-29
t1 = BashOperator(
    task_id="get_dag_config",
    bash_command='echo "{0}"'.format(dag_config),
    dag=dag,
)

# You can directly use a variable from a jinja template
## {{ var.value.<variable_name> }}

t2 = BashOperator(
    task_id="get_variable_value",
    bash_command='echo {{car.value.val3 }} ',
    dag=dag,
)

## {{ var.json.<variable_name> }}
t3 = BashOperator(
    task_id="get_variable_json",
    bash_command='echo {{ var.json.variable_config.var3 }}',
    dag=dag,
)


start >> [t1, t2, t3]
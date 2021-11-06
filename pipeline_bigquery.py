from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


project_name = "etldatawarehouse"
path = "/Users/nanayaw/Desktop/blossom/dvd"

default_args = {
    "owner": "nana",
    "depends_on_past": True,
    "start_date": datetime(2021, 11, 6),
    "email": ["nanaitconsult@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}


# dates will be loaded just one time. Look at the schedule interval
with DAG(
    "warehouse", schedule_interval=timedelta(days=1), default_args=default_args
) as dag:
    start = DummyOperator(task_id="start_onetime")
    end = DummyOperator(task_id="end_onetime")

    customer_tb = BashOperator(
        task_id=f"load_customer",
        bash_command=f"bq load --autodetect dvdrentals.customer {path}/customer.csv",
    )

    actor_tb = BashOperator(
        task_id=f"load_actor",
        bash_command=f"bq load --autodetect dvdrentals.actor {path}/actor.csv",
    )

    film_tb = BashOperator(
        task_id=f"load_film",
        bash_command=f"bq load --autodetect dvdrentals.film {path}/film.csv",
    )

    rental_tb = BashOperator(
        task_id=f"load_rental",
        bash_command=f"bq load --autodetect dvdrentals.rental {path}/rental.csv",
    )

    start >> [customer_tb, actor_tb, film_tb, rental_tb] >> end

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

# /you can create default arguments
default_args = {
    "owner": "nana",
    "depends_on_past": False,
    # /in case it fails, try 2 times
    "retries": 2,
    # /delay 30 secs
    "retry_delay": timedelta(seconds=10),
}
# Create a DAG
with DAG(
    "BasicETL",  # name of the file
    description="Basic ETL Dag",
    default_args=default_args,
    # /timedelta helps specify if the dag should in days(days=1(start date)), seconds, hours
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 11, 2)
    # end_date=datetime(2020, 11, 2),
) as dag:

    # /create the task using the bashOperator
    taskA = BashOperator(
        task_id="taskA",
        # /ls /Users/nanayaw/Downloads | grep 'zip' > /Users/nanayaw/DWH/results.txt -- go to the downloads folder and match the pattern of a zip file and writes the file names to the text file
        bash_command="ls /Users/nanayaw/Downloads | grep 'zip' > /Users/nanayaw/DWH/results.txt",
    )

    taskB = BashOperator(
        task_id="taskB",
        bash_command="cat /Users/nanayaw/DWH/results.txt > /Users/nanayaw/DWH/Newresults.txt",
    )

    taskC = BashOperator(
        task_id="taskC", bash_command="mkdir /Users/nanayaw/PipelineFolder"
    )

    taskA >> taskB >> taskC

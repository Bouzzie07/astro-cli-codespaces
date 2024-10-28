import airflow
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from rockets.includes.get_pictures import _get_pictures
from rockets.includes.get_email import task_fail_alert

from airflow.models import Variable

arg = {
"on_failure_callback": task_fail_alert,
"params": {
    "environment": Variable.get("environment"),
    "dag_owner": "Bussie"
}
}

#Instantiate a DAG object using With clause context manager to efficiently close processes/resource efficiently.
with DAG(
    dag_id="rocket",
    start_date=datetime(2024, 10, 28),
    schedule_interval=None,
    catchup=False, #catchup tries to backfill any missed date (historical data)
    default_args=arg,
    tags=["CoreDataEngineerBootcamp"]
) as dag:
    
    #When you instantiate an object with an operator, it becomes a task
    #To download the images based on the task in class, instantiate another object, go to github and import the needed module from the operator
    download_launches = BashOperator(
        task_id="download_launches", #Task_id is very important when you want to instantiate an operator as it enables airflow to identify each task
        #you can use different bash commandline utilities to get the data from the internet. Using curl (client url) is a commandline utility
        #that helps to transfer data over the internet so, it can be used to download or upload data across all operating systems.
        bash_command="curl -o /usr/local/airflow/dags/rockets/launches/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming"
    )

#Transform
    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures
    )

#notify
    notify = BashOperator (
        task_id="notify",
        bash_command= 'echo "There are now $(ls /usr/local/airflow/dags/rockets/images | wc -l) images"'
    )

#Set the order of execution (dependency/relationships) of tasks
    download_launches >> get_pictures >> notify
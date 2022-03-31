#hello_world.py
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_world() -> None:
    print("world")

#with 구문으로 DAG 정의 시작
with DAG(
    dag_id="hello_world", # DAG 식별자용 아이디
    description = "JODONG2 first DAG",
    start_date=days_ago(2), # DAG 정의 기준 2일전부터 실행합니다.
    schedule_interval= "0 6 * * *", # 매일 06:00에 실행합니다
    tags=["my_dags"],
)as dag:

    #테스크를 정의합니다.
    #Python Operaotr로 world를 출력합니다.
    t2 = PythonOperator(
        task_id = "print_world",
        python_callable=print_world,
        depends_on_past= True,
        owner="DONG2",
        retries = 3,
        retry_delay = timedelta(minutes=5),
    )

    #테스트 순서를 정합니다.
    #t1실행후 t2실행

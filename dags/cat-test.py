from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

with DAG(
        dag_id='echo_test1', description='echo_test1',
        schedule_interval=None,
        default_args=default_args) as dag:
    text_file_path = '/Users/louis/Documents/Workspace/02.Study/airflow/dags'

    #### create txt file  --> 텍스트 파일을 생성합니다
    create_text_file_command = f'cd {text_file_path} && echo hello airflow > test.txt'
    create_text_file = BashOperator(
        task_id='run_after_loop',
        bash_command='echo 1',
    )

    # #### cat txt file  --> 텍스트 파일을 읽습니다
    # read_text_file_command = f'cd {text_file_path} && cat test.txt'
    # read_text_file = BashOperator(
    #     task_id='cat_text_file',
    #     bash_command=read_text_file_command,
    #     dag=dag)
    #
    # #### remove txt file  --> 텍스트 파일을 삭제합니다.
    # remove_text_file_command = f'cd {text_file_path} && rm test.txt'
    # remove_text_file = BashOperator(
    #     task_id='remove_text_file',
    #     bash_command=remove_text_file_command,
    #     dag=dag)
    #
    # create_text_file >> read_text_file >> remove_text_file  # 이것은 위의 task를 이어주는 줄입니다.
    # create_text_file
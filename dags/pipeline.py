import airflow
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import os
from datetime import date, timedelta

from helpers.create_dir import create_dir
from helpers.constants import UNZIPPED_FILES_FOLDER
from tasks_functions.functions import start,\
    complete,\
    get_ftp_host_and_files_names_to_download,\
    download_file, upload_files_to_s3,\
    download_files_from_s3,\
    parse_files,\
    unzip


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG("my_dag",  # Dag id
         default_args=default_args,
         # Cron expression, here it is a preset of Airflow, @daily means once every day.
         schedule_interval='@daily',
         render_template_as_native_obj=True
         ) as dag:

    groups = []
    create_dir(os.getcwd(), UNZIPPED_FILES_FOLDER)
    ftp_and_names = get_ftp_host_and_files_names_to_download()
    start_t = PythonOperator(
        task_id=f'start',
        python_callable=start)

    for index, file_name in enumerate(ftp_and_names['file_names']):
        t1 = PythonOperator(
            task_id=f'download_file{index}',
            python_callable=download_file,
            op_kwargs={'file_name': file_name,
                       'ftp_host': ftp_and_names["ftp_host"],
                       'task_id': f'download_file{index}'},
            provide_context=True)

        t2 = PythonOperator(
            task_id=f'unzip_files{index}',
            python_callable=unzip,
            op_kwargs={'id': f'download_file{index}',
                       'unzip_id': f'unzip_files{index}',
                       'file_name': file_name},
            provide_context=True)

        start_t >> t1 >> t2

        def get_unzipped_files_string():
            try:
                unzipped_files_string = Variable.get("unzipped_files")
                return unzipped_files_string
            except Exception as e:
                return ''
        unzipped_files_string = get_unzipped_files_string()

        unzipped_files = (unzipped_files_string.replace(
            '[', '').replace(']', '')).split(',')

        if len(unzipped_files) > 0:
            for index_parser, unzipped_file in enumerate(unzipped_files):

                t3 = PythonOperator(
                    task_id=f'upload_files_to_s3{index}{index_parser}',
                    python_callable=upload_files_to_s3,
                    op_kwargs={
                        'id_downloaded': f'download_file{index}',
                        'file_name_count': index_parser,
                        'task_id': f'upload_files_to_s3{index}{index_parser}'},
                    provide_context=True)

                t4 = PythonOperator(
                    task_id=f'download_file_from_s3_{index}{index_parser}',
                    python_callable=download_files_from_s3,
                    op_kwargs={
                        'id_uploaded_to_s3': f'upload_files_to_s3{index}{index_parser}'},
                    provide_context=True)

                t5 = PythonOperator(
                    task_id=f'parse_files{index}{index_parser}',
                    python_callable=parse_files,
                    op_kwargs={
                        'task_id': f'parse_files{index}{index_parser}',
                        'id_uploaded_to_s3': f'upload_files_to_s3{index}{index_parser}'},
                    provide_context=True)
                # taks do not start; need to change the state of them

                t6 = PythonOperator(
                    task_id=f'complete_{index}{index_parser}',
                    python_callable=complete,
                    op_kwargs={'task_id': f'complete_{index}{index_parser}'},
                    provide_context=True)

                t2 >> t3 >> t4 >> t5 >> t6

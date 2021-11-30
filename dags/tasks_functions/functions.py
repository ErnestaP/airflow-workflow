from airflow.models import Variable, DagBag, TaskInstance
from airflow import DAG, settings, configuration as conf

import os

from helpers.connect_to_ftp import connect_to_ftp
from helpers.collect_files import collect_files
from helpers.download_file_from_ftp import download_file_from_ftp
from helpers.unzip_files import unzip_files
from helpers.upload_to_s3 import upload_to_s3
from helpers.download_from_s3 import download_file_from_s3
from  helpers.parse_files import crawler_parser

def start():
    return


def complete(task_id, **kwargs):
    resetTasksStatus(task_id, kwargs['execution_date'])
    return


def get_name_template_for_zipped_files_count(zip_id):
    return f'count_{zip_id}'


def get_ftp_host_and_files_names_to_download():
    ftp_host = connect_to_ftp()
    file_names = collect_files(ftp_host)
    return({'ftp_host': ftp_host, 'file_names': file_names})


def download_file(file_name, ftp_host, task_id,  **context):
    downloaded_files_names = download_file_from_ftp(ftp_host, file_name)
    main_file_name_with_prefix = os.path.basename(file_name)
    context['ti'].xcom_push(key=task_id, value=downloaded_files_names)


def unzip(id, unzip_id, file_name, **kwargs):
    # mna reikia grouping folder grazint ir name
    ti = kwargs['ti']
    value = ti.xcom_pull(key=id)
    files = unzip_files(value, file_name)
    file_name = os.path.basename(value)
    grouping_folder = file_name.split(".zip")[0]
    for index, unzipped_file_name in enumerate(files):
        ti.xcom_push(key=f'{id}_{index}',
                     value=f'{grouping_folder}/{unzipped_file_name}')
    # we be passed the last files. becauset he ast one was pdf...
    Variable.set("unzipped_files", files)
    return files


def upload_files_to_s3(id_downloaded, file_name_count, task_id,  **kwargs):
    ti = kwargs['ti']
    xcom_id = f'{id_downloaded}_{file_name_count}'
    value = ti.xcom_pull(key=xcom_id).split('/')
    grouping_folder = value[0]
    file_name = value[1]
    s3_key = upload_to_s3(grouping_folder, file_name)
    kwargs['ti'].xcom_push(key=task_id, value=s3_key)


def download_files_from_s3(id_uploaded_to_s3, **kwargs):
    ti = kwargs['ti']
    s3_key = ti.xcom_pull(key=id_uploaded_to_s3)
    download_file_from_s3(s3_key)

def parse_files(task_id, id_uploaded_to_s3,  **kwargs):
    resetTasksStatus(task_id, kwargs['execution_date'])
    ti = kwargs['ti']
    s3_key = ti.xcom_pull(key=id_uploaded_to_s3)
    print(s3_key, 'keyy')
    crawler_parser(s3_key)

    # return f'{s3_key}_parsed'



def resetTasksStatus(task_id, execution_date):
    '''
    I need reset status of the task, which is branches out of zip func.
    It doesn't start because it marked as removed
    https://www.linkedin.com/pulse/dynamic-workflows-airflow-kyle-bridenstine/
    '''
    dag_folder = conf.get('core', 'DAGS_FOLDER')
    dagbag = DagBag(dag_folder)
    check_dag = dagbag.dags['my_dag']
    session = settings.Session()

    my_task = check_dag.get_task(task_id)
    ti = TaskInstance(my_task, execution_date)
    ti.set_state(None, session)

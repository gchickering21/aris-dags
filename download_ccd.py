from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(dag_id='download_ccd',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=120))


def download_ccd_links():

    # ssh = SSHHook(remote_host = '172.29.6.4', username='kthomson', password='***', port = 22)
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    print(ssh)
    
    ssh_client = None
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        ssh_client.exec_command(r"python C:\Users\ebuehler\Documents\GitHub\ccdSAS\IO\ccd_data_list_downloader.py")
    finally:
        if ssh_client:
            ssh_client.close()


def download_ccd_dat():

    # ssh = SSHHook(remote_host = '172.29.6.4', username='kthomson', password='***', port = 22)
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    print(ssh)
    
    ssh_client = None
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        ssh_client.exec_command(r"python C:\Users\ebuehler\Documents\GitHub\ccdSAS\IO\ccd_data_downloader.py")
    finally:
        if ssh_client:
            ssh_client.close()


download_links = PythonOperator(
    task_id='download_links',
    python_callable=download_ccd_links,
    dag=dag
)

download_dat = PythonOperator(
    task_id='download_dat',
    python_callable=download_ccd_dat,
    dag=dag
)


download_dat >> download_links

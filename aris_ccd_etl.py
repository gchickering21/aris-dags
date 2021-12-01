from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
SERVICE_GIT_DIR = 'C:\\ARIS'

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


dag = DAG(dag_id='aris_ccd_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=36000))


def download_ccd_links():
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ##'cd' +  SERVICE_GIT_DIR + ' && python ' + ' \\ccdSAS\\ccd_data_list_downloader.py' 
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' + 'ccdSAS\\IO\\ccd_data_list_downloader.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()


def download_ccd_dat():
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' +  'ccdSAS\\IO\\ccd_data_downloader.py'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def ccd_nonfiscal():
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\SAS' + ' && sas ccd_nonfiscal_state_RE2'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def ccd_fiscal():
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\SAS' + ' && sas ccd_fiscal_state'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def db_load_mrt():

    
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\DB-Generation' + ' && python write_mrt.py' 
        # command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' +  'ccdSAS\\DB-Generation\\write_mrt.py'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def gen_hrt_xl():

    
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\ccdSAS\\HRT' + ' && .\\venv\\Scripts\\activate && python gen_hrt.py -t 203.10 --xlsx_dir C:\\ARIS\\ccdSAS\\HRT\\HRT --xlsx-only' 
        ssh_client.exec_command(command)
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

gen_nonfiscal = PythonOperator(
    task_id='ccd_fiscal',
    python_callable=ccd_nonfiscal,
    dag=dag
)

gen_fiscal = PythonOperator(
    task_id='ccd_fiscal',
    python_callable=ccd_fiscal,
    dag=dag
)

db_load = PythonOperator(
    task_id='db_load',
    python_callable=db_load_mrt,
    dag=dag
)

# gen_hrt = PythonOperator(
#     task_id='gen_hrt',
#     python_callable=gen_hrt_xl,
#     dag=dag
# )

#TODO: add checks 

download_links >> download_dat >> gen_nonfiscal >> gen_fiscal >> db_load #>> gen_hrt

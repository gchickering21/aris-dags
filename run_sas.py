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


dag = DAG(dag_id='eb_ssh_test',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=120))


def run_sas():

    # ssh = SSHHook(remote_host = '172.29.6.4', username='kthomson', password='***', port = 22)
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    print(ssh)
    
    ssh_client = None
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        stdin, stdout, stderr = ssh_client.exec_command(r"cd ..\..\users\ebuehler\Documents\GitHub\ccdSAS\SAS && sas ccd_nonfiscal_state_RE2")
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def error_lookup():
    string1 = 'ERROR:'
  
    # opening a text file
    file1 = open("..\..\users\ebuehler\Documents\GitHub\ccdSAS\Output\ccd_nonfiscal_state_2018_log.txt", "r")
    
    # read file content
    readfile = file1.read()
    
    # checking condition for string found or not
    if string1 in readfile: 
        file1.close() 
        return(True)
    else: 
        file1.close() 
        return(False)

def check_sas():

    # ssh = SSHHook(remote_host = '172.29.6.4', username='kthomson', password='***', port = 22)
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    print(ssh)
    
    ssh_client = None
    try:
        if error_lookup(): 
            raise ValueError('Error found')
        else: 
            pass 
            
    finally:
        if ssh_client:
            ssh_client.close()

def run_ccd_download():

    # ssh = SSHHook(remote_host = '172.29.6.4', username='kthomson', password='***', port = 22)
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    print(ssh)
    
    ssh_client = None
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        stdin, stdout, stderr = ssh_client.exec_command(r"cd ..\..\users\ebuehler\Documents\GitHub\ccdSAS\SAS && sas ccd_nonfiscal_state_RE2")
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()    

call_sas = PythonOperator(
    task_id='call_sas',
    python_callable=run_sas,
    dag=dag
)

check_runs = PythonOperator(
    task_id='check_runs',
    python_callable=check_sas,
    dag=dag
)

call_sas 
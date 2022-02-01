from datetime import timedelta, datetime
from pickle import TRUE
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ipeds' # File housing ARIS repos on SAS server's C drive

# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ebuehler@air.org', 'mtrihn@air.org'],
    'email_on_failure': TRUE,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define Main DAG for CCD pipeline 
dag = DAG(dag_id='aris_ipeds_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=600))


class code_executer:
    def __init__(self, service_dir, command):
        self.dir = service_dir
        self.command = command

    @property
    def command(self):
        return self.__command

    @command.setter
    def command(self):
        set_dir = 'cd ' +  self.dir
        action = ' && ' + self.command
        self.__command = set_dir + action


    def execute_command(self): 
        ssh = SSHHook(ssh_conn_id="sas1buehlere")
        ssh_client = None
        print(ssh)
        try:
            ssh_client = ssh.get_conn()
            ssh_client.load_system_host_keys()
            stdin, stdout, stderr = ssh_client.exec_command(self.command)
            out = stdout.read().decode().strip()
            error = stderr.read().decode().strip()
            print(out)
            print(error)
        finally:
            if ssh_client:
                ssh_client.close()


def t318():
    '''
    Purpose: execute t318 SAS code 
    '''
    exe = code_executer(SERVICE_GIT_DIR, 'sas t318-40-IPEDS-C2019-C2020-D21-MRT_2021_09_14')
    exe.execute_command() 


# Execute t318
execute_table = PythonOperator(
    task_id='execute_table',
    python_callable=t318,
    dag=dag
)

execute_table

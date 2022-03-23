from datetime import timedelta, datetime
from pickle import TRUE
import airflow
import code_executer
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
SERVICE_GIT_DIR = 'C:\\ARIS\\autoDigest\\ccd' # File housing ARIS repos on SAS server's C drive

# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ebuehler@air.org', 'mtrihn@air.org', "gchickering@air.org"],
    'email_on_failure': TRUE,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define Main DAG for CCD pipeline 
dag = DAG(dag_id='aris_ccd_etl',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=600))


def links():
    '''
    Purpose: execute ccd_data_list_downloader.py  on command line to generate list of CCD links
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' + '\\IO\\ccd_data_list_downloader.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()


def dat():
    '''
    Purpose: execute ccd_data_downloader.py on command line to download CCD data 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + ' && python ' +  'IO\\ccd_data_downloader.py'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def nonfiscal():
    '''
    Purpose: execute ccd_nonfiscal_state_RE2.sas on command line to generate nonfiscal long data from ccd data 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ccd_nonfiscal_state_RE2'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def nonfiscal_wide():
    '''
    Purpose: execute ccd_school_convert.sas on command line to generate nonfiscal wide data from nonfiscal long data. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ccd_school_convert'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()


def nonfiscal_school(): 
    '''
    Purpose: execute ccd_school_convert.sas on command line to generate nonfiscal wide data from nonfiscal long data. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ccd_nonfiscal_school'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def fiscal():
    '''
    Purpose: execute ccd_fiscal_state.sas on command line to generate fiscal data from ccd data. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\SAS' + ' && sas ccd_fiscal_state'
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()


def district_convert():
    '''
    Purpose: execute t318 SAS code 
    '''
    exe = code_executer(SERVICE_GIT_DIR , 'sas ccd_district_convert', 'sas')
    exe.execute_command() 

def nonfiscal_district():
    '''
    Purpose: execute t318 SAS code 
    '''
    exe = code_executer(SERVICE_GIT_DIR , 'sas ccd_nonfiscal_district', 'sas')
    exe.execute_command() 


def mrt():
    '''
    Purpose: execute write_mrt.py on command line to generate mrt from nonfiscal long and write to database. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\DB-Generation' + ' && python write_mrt.py' 
        stdin, stdout, stderr = ssh_client.exec_command(command)
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

def hrt():
    '''
    Purpose: execute gen_hrt.py on command line to generate hrt files from mrt loaded in the database. 
    '''
    ssh = SSHHook(ssh_conn_id="sas1buehlere")
    ssh_client = None
    print(ssh)
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        command = 'cd ' +  SERVICE_GIT_DIR + '\\HRT' + ' && python gen_hrt.py -t 203.10 --xlsx_dir HRT' 
        ssh_client.exec_command(command)
    finally:
        if ssh_client:
            ssh_client.close()






# Download CCD Links 
download_links = PythonOperator(
    task_id='download_links',
    python_callable=links,
    dag=dag
)

# Download CCD Data 
download_dat = PythonOperator(
    task_id='download_dat',
    python_callable=dat,
    dag=dag
)


# Generate Nonfiscal state from CCD Data with SAS
gen_nonfiscal = PythonOperator(
    task_id='gen_nonfiscal',
    python_callable=nonfiscal,
    dag=dag
)

# Generate Nonfiscal wide file with SAS 
gen_nonfiscal_wide = PythonOperator(
    task_id='gen_nonfiscal_wide',
    python_callable=nonfiscal_wide,
    dag=dag
)

# Generate Nonfiscal school from CCD Data with SAS
gen_nonfiscal_school = PythonOperator(
    task_id='gen_nonfiscal_school',
    python_callable=nonfiscal_school,
    dag=dag
)

# Generate Nonfiscal district from CCD Data with SAS
gen_nonfiscal_district = PythonOperator(
    task_id='gen_nonfiscal_district',
    python_callable=nonfiscal_district,
    dag=dag
)

# Generate Nonfiscal district from CCD Data with SAS
gen_district_wide = PythonOperator(
    task_id='gen_district_wide',
    python_callable=district_convert,
    dag=dag
)

# Generate Fiscal Data from CCD Data with SAS
gen_fiscal = PythonOperator(
    task_id='gen_fiscal',
    python_callable=fiscal,
    dag=dag
)

# Create MRT and load to Database with Python 
load_mrt = PythonOperator(
    task_id='load_mrt',
    python_callable=mrt,
    dag=dag
)

# Generate HRT file 
gen_hrt = PythonOperator(
    task_id='gen_hrt',
    python_callable=hrt,
    dag=dag
)

# DAG Dependancy
download_links >> download_dat 
download_dat >> gen_nonfiscal >> gen_nonfiscal_wide >> gen_nonfiscal_school >> load_mrt >> gen_hrt 
download_dat >> gen_nonfiscal_district >> gen_district_wide
download_dat >> gen_fiscal
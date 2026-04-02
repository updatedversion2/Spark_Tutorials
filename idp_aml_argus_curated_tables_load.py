from datetime import timedelta, datetime, date
import airflow
import yaml
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
import time
import subprocess

sys.path.insert(1, '/root/airflow/scripts/idp_ingestion/src/')
sys.path.insert(2, '/data1/airflow-data/scripts/idp_ingestion/src/')

import utility.ingestion_utility as iu

################# Config Details #############

sshcon = 'ssh_edge1'

try:
    path = "/root/airflow/scripts/idp_extract/"
    mapping = iu.load_yml_config(
        os.path.join(path, "config_files", "aml-argus", "aml_argus_curated_tables_load.yaml"))
except:
    path = "/data1/airflow-data/scripts/idp_extract/"
    mapping = iu.load_yml_config(
        os.path.join(path, "config_files", "aml-argus", "aml_argus_curated_tables_load.yaml"))

################
source = "AML"
schedule = mapping.get('schedule')
mail_notification = mapping.get('mail_notification', False)
default_args = mapping.get('default_args')[0]
default_args.update({'start_date': days_ago(1)})
dag_name = mapping.get('dag_name')
pool_name = 'aml-argus'
default_job = "aml_argus"
trig_rule = "all_success"
log_cmd = "echo `date +%y%m%d%H%M%S_$$`"
log_id = subprocess.getoutput(log_cmd)
timestr = time.strftime("%Y-%m-%d")
prevtimestr = (datetime.now() - timedelta(1)).strftime("%d%m%Y")
date1 = date.today()
RUN_DATE = str(date1)

dev_frame_path = mapping.get('dev_frame_path')
user_file_path = mapping.get('user_file_path')

print("log_id for DAG : ==> {}".format(log_id))


def job_statement(job: str, report_name: str, source: str, script: str, r_name: str):
    if job == 'execute':
        try:
            return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -l {str(log_id)} -m 8 -d 20 -c 5 -n 10 -z "--conf spark.kryoserializer.buffer.max=1gb --conf spark.ui.port=4050 --conf spark.dynamicAllocation.enabled=false --conf spark.sql.parquet.int96RebaseModeInWrite=LEGACY --conf spark.executor.memoryOverhead=1500" """
        except Exception as p:
            print("Provide the parameter to run the job")
    if job == 'idp_aml_argus_account':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_argus_data_processing.yml --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_customer':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_argus_data_processing.yml --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    #INCT_DERIVED STAGING
    elif job == 'idp_aml_argus_inct_derived_staging':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    #INCT_DERIVED CONSUMPTION
    elif job == 'execute_aml_trn_daily_inct_derived_upi':
        application_name = f"IDP_AML_ARGUS_{script}_upi"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py UPI daily' """

    elif job == 'execute_aml_trn_daily_inct_derived_rtgs_neft_imps':
        application_name = f"IDP_AML_ARGUS_{script}_rtgs_neft_imps"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py RTGS_NEFT_IMPS daily' """

    elif job == 'execute_aml_trn_daily_inct_derived_rest':
        application_name = f"IDP_AML_ARGUS_{script}_rest"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py REST daily' """

    elif job == 'idp_aml_argus_txn_daily_upi':
        application_name = f"IDP_AML_ARGUS_{script}_upi"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py UPI daily' """

    elif job == 'idp_aml_argus_txn_daily_rtgs_neft_imps':
        application_name = f"IDP_AML_ARGUS_{script}_rtgs_neft_imps"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py RTGS_NEFT_IMPS daily' """

    elif job == 'idp_aml_argus_txn_daily_rest':
        application_name = f"IDP_AML_ARGUS_{script}_rest"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py REST daily' """

    elif job == 'idp_aml_argus_txn_aggr_daily':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_cust_acct_link':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_party_details':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_branch_master':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_customer_contact':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_argus_data_processing.yml --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_customer_address':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_argus_data_processing.yml --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_asset_cards':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 48 -d 48 -c 4 -n 10 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/aml_argus/adl/{script}.py' """


    elif job == 'idp_aml_argus_country_risk':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_aml_txn_daily_prime':
        application_name = f"IDP_AML_ARGUS_{script}"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_asset_loan_transaction':
        application_name = f"IDP_AML_ARGUS_{script}"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job == 'idp_aml_argus_asset_loan_account':
        application_name = f"IDP_AML_ARGUS_{script}"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """


    ############### success file task ####################

    elif job == 'idp_aml_argus_check_ingest_success_file_asset_loan_transaction':
        application_name = f"IDP_AML_ARGUS_{script}_asset_loan_transaction"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py LOANTXN' """

    elif job == 'idp_aml_argus_check_ingest_success_file_branch_master':
        application_name = f"IDP_AML_ARGUS_{script}_branch_master"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py BRNCHMSTR' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_customer':
        application_name = f"IDP_AML_ARGUS_{script}_aml_customer"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py AMLCUSTOMER' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_loan_account':
        application_name = f"IDP_AML_ARGUS_{script}_aml_loan_account"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py AMLLOANACCOUNT' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_txn_daily_prime':
        application_name = f"IDP_AML_ARGUS_{script}_txn_daily_prime"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py TXNDAILYPRIME' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_txn_daily_cbs':
        application_name = f"IDP_AML_ARGUS_{script}_txn_daily_cbs"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py TXNDAILYCBS' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_account':
        application_name = f"IDP_AML_ARGUS_{script}_aml_account"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py AMLACCOUNT' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_asset_cards':
        application_name = f"IDP_AML_ARGUS_{script}_aml_asset_cards"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py ASSETCARDS' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_customer_contact':
        application_name = f"IDP_AML_ARGUS_{script}_aml_customer_contact"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py CUSTCONTACT' """

    elif job == 'idp_aml_argus_check_ingest_success_file_aml_customer_address':
        application_name = f"IDP_AML_ARGUS_{script}_aml_aml_customer_address"
        return  f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/{script}.py CUSTADDRESS' """


    ###################################################################

    elif job == 'idp_aml_argus_send_mail_notification':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 32 -d 32 -c 4 -n 6 -z " --name {application_name} --deploy-mode client --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_data_processing.ini --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    elif job.startswith('idp_aml_argus_add_success_file'):
        return f""" sh -vx {user_file_path}/add_success_file.sh aml aml_argus {r_name} {log_id} """

    elif job == "refresh_table_all":
        table_names = "aml_account aml_customer aml_txn_daily aml_txn_aggr_daily aml_cust_acct_link aml_party_details aml_branch_master aml_customer_contact aml_customer_address"
        return f"""kinit -kt /data1/idp_framework/config/keytab/idp-service.keytab -c /data1/idp_framework/config/keytab/cache idp-service; sh {user_file_path}/refresh_table.sh {table_names}"""

    elif job.startswith('idp_aml_argus_refresh_table'):
        return f"""kinit -kt /data1/idp_framework/config/keytab/idp-service.keytab -c /data1/idp_framework/config/keytab/cache idp-service; sh {user_file_path}/refresh_table.sh {r_name}"""

    elif job == 'idp_aml_argus_commonlookup_refresh':
        application_name = f"IDP_AML_ARGUS_{script}"
        return f"""sh -vx {dev_frame_path}/idp_generic_wrapper.sh -r {job}_{report_name} -s {source.lower()} -m 20 -d 20 -c 4 -n 6 -z " --name {application_name} --deploy-mode cluster --conf spark.dynamicAllocation.enabled=false --files /data1/idp_framework/user_files/aml-argus/config/aml_argus_data_processing.yml --py-files /data1/idp_framework/code/idp_util.py,/data1/idp_framework/code/idp_pyspark_wrapper.py,/data1/idp_framework/user_files/aml-argus/aml_argus_utils.py " -p 'aml-argus/aml_argus/adl/{script}.py' """

    if job == 'finished_all_extract':
        return "echo 'completed all extract'"

    return ""


dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval=schedule,
    description=f'IDP AML Argus Transformed data {source}',
    max_active_runs=1,
    concurrency=4
)

preprocess_task = {}

preprocess = {default_job: BashOperator(
    task_id=default_job,
    bash_command="echo 'idp reports' ",
    pool=pool_name,
    dag=dag,
)}
preprocess_task.update(preprocess)
print(source)

### Before Starting report transformation

for rep_source in mapping.get("dag_config"):
    report_type = rep_source.get("report_type")
    preprocess = {report_type: BashOperator(
        task_id=report_type,
        bash_command="echo report_type",
        pool=pool_name,
        dag=dag,
    )}
    preprocess_task.update(preprocess)
    preprocess_task.get(report_type).set_upstream(preprocess_task.get(default_job))
    for task_config in rep_source.get("task_type"):
        tasks = task_config.get("tasks")
        report_names = task_config.get("report_names").split(",")
        for report_name in report_names:
            for task in tasks:
                job = task['task_name'].strip().lower()
                # upstream = task['upstream']
                upstream = task.get('upstream', '')
                upstream_tasks = upstream.split(',')
                script = task['script']
                r_name = task['r_name']
                job = job.strip().lower()

                if job.strip() == 'idp_aml_argus_refresh_table':
                    for upstream_task in upstream_tasks:
                        # upstream_string = task['upstream']
                        upstream, r_name = upstream_task.split("-", 1)
                        cm = job_statement(job=job, report_name=report_name, source=source, script=script,
                                           r_name=r_name)
                else:
                    cm = job_statement(job=job, report_name=report_name, source=source, script=script, r_name=r_name)
                # cm = job_statement(job=job, report_name=report_name, source=source, script=script, r_name=r_name)
                if job.strip() == 'send_mail_notification':
                    job_id = f"{job}"
                    if job_id not in preprocess_task:
                        operator = SSHOperator(task_id=job_id, command=cm, pool=pool_name, ssh_conn_id=sshcon, dag=dag,
                                               retries=4, retry_delay=timedelta(minutes=30), trigger_rule=trig_rule)
                        preprocess = {job: operator}
                        preprocess_task.update(preprocess)
                else:
                    job_id = f"{job}"
                    operator = SSHOperator(task_id=job_id, command=cm, pool=pool_name, ssh_conn_id=sshcon, dag=dag,
                                           retries=4, retry_delay=timedelta(minutes=30), trigger_rule=trig_rule)
                    preprocess = {job: operator}
                    preprocess_task.update(preprocess)
                # print("#######################################################", preprocess_task)
                # print(preprocess_task)
                for upstream_task in upstream_tasks:
                    if upstream_task:
                        parts = upstream_task.split("-", 1)
                        # upstream_task, r_name = upstream_task.split("-", 1)
                        if len(parts) == 2:
                            upstream_task, r_name = parts
                            preprocess_task.get(job).set_upstream(preprocess_task.get(upstream_task.strip()))
                        else:
                            preprocess_task.get(job).set_upstream(preprocess_task.get(parts[0].strip()))
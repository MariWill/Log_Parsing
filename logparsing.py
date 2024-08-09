from pathlib import Path

def main():
    file_dir = Path("/opt/airflow/logs/dag_id=salesforce_api_testing/")
    file_type = '*.log'

    error = ' ERROR - '
    failed_task = ' Task failed with exception'
    failed_job = 'Failed to execute job'
    failed_bulk_insert = ' bulk_insert '
    failed_bulk_upsert_finacct = ' bulk_upsert_finacct '
    failed_bulk_upsert_acct = ' bulk_upsert_acct '
    failed_bulk_upsert_finalert = ' bulk_upsert_finalert '
    failed_xcom_pull_task = ' xcom_pull_task'
    failed_check_lodestar_processing_complete = ' check_lodestar_processing_complete'

    countErrors = 0
    countFailedTasks = 0
    countFailedJobs = 0
    count_failed_bulk_insert = 0
    count_failed_bulk_upsert_finacct = 0
    count_failed_bulk_upsert_acct = 0
    count_failed_bulk_upsert_finalert = 0
    count_failed_xcom_pull_task = 0
    count_failed_check_lodestar_processing_complete = 0
    
    log_files = file_dir.rglob(file_type)
    
    log_parse
    
def log_parse():
    for log_file in log_files:
        content = log_file.open()
        for line in content:
            if error in line:
                countErrors += 1
                if failed_task in line:
                    countFailedTasks +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')
                if failed_job in line:
                    countFailedJobs +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')                    
                if failed_bulk_insert in line:
                    count_failed_bulk_insert +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')
                if failed_bulk_upsert_finacct in line:
                    count_failed_bulk_upsert_finacct +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')
                if failed_bulk_upsert_acct in line:
                    count_failed_bulk_upsert_acct +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')  
                if failed_bulk_upsert_finalert in line:
                    count_failed_bulk_upsert_finalert +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')
                if failed_xcom_pull_task in line:
                    count_failed_xcom_pull_task +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')
                if failed_check_lodestar_processing_complete in line:
                    count_failed_check_lodestar_processing_complete +=1
                    print(log_file)
                    print('-------------')
                    print('-------------')

    print("Number of errors: " + str(countErrors))
    print("Number of failed tasks: " + str(countFailedTasks))
    print("Number of failed jobs: " + str(countFailedJobs))
    
    print("end of dag run")

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

def extract_data(ti):
    ratio = 0.7
    ti.xcom_push(key="ratio", value=ratio)
    
def check_ratio(ti):
    ratio = float(ti.xcom_pull(task_ids="extract_data", key="ratio"))
    
    if ratio > 0.5:
        return "print_case_greater_half"
    return "print_case_less_half"

def print_case_greater_half(ti):
    ratio = ti.xcom_pull(task_ids="extract_data", key="ratio")
    print("The ratio is greater than half: " + str(ratio))

def print_case_less_half(ti):
    ratio = ti.xcom_pull(task_ids="extract_data", key="ratio")
    print("The ratio is less than half: " + str(ratio))
    
with DAG(
    dag_id="branch_operator_traditional_dag",
    start_date=datetime(2026, 5, 24),
    schedule="@daily",
    catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )
    
    branch_task = BranchPythonOperator(
        task_id="check_ratio",
        python_callable=check_ratio
    )
    
    task_greater = PythonOperator(
        task_id="print_case_greater_half",
        python_callable=print_case_greater_half,
    )
    
    task_less_than = PythonOperator(
        task_id="print_case_less_half",
        python_callable=print_case_less_half,
    )
    
    join_task = EmptyOperator(
        task_id="empty_task",
        trigger_rule="none_failed_min_one_success"
    )
    
    extract_task >> branch_task >> [task_greater, task_less_than] >> join_task
from airflow.sdk import dag, task

from datetime import datetime

@dag(
    dag_id="branch_operator_dag",
    start_date=datetime(2026, 5, 24),
    schedule="@daily",
    catchup=False
)
def branch_operator_dag():
    @task
    def extract_data() -> float:
        return 0.7
    
    @task.branch
    def check_ratio(ratio: float):
        if ratio > 0.5:
            return "print_case_greater_half"
        return "print_case_less_half"
    
    @task
    def print_case_greater_half(ti):
        ratio = ti.xcom_pull(task_ids="check_ratio")
        print("The ratio is greater than half: " + str(ratio))
        
    @task
    def print_case_less_half(ti):
        ratio = ti.xcom_pull(task_ids="check_ratio")
        print("The ratio is less than half: " + str(ratio))
        
    @task(trigger_rule="none_failed_min_one_success")
    def empty_task():
        pass
    
    ratio = extract_data()
    branch = check_ratio(ratio)
    greater = print_case_greater_half()
    less = print_case_less_half()
    
    branch >> [greater, less] >> empty_task()

branch_operator_dag()
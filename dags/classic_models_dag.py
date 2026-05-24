from airflow.sdk import dag, task, TaskGroup, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

from datetime import datetime

from src.pipeline.classic_models_pipeline import transform_data, notify_valid_records

TABLES = ["customers", "orders", "products"]

@dag(
    dag_id="classic_models_dag",
    start_date=datetime(2026, 5, 23),
    schedule="@daily",
    catchup=False
)
def classic_models_dag():
    task_groups = []
    s3_bucket = Variable.get("s3_raw_data_bucket")

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")
        
    for table in TABLES:
        with TaskGroup(group_id=f"{table}_tg") as tg:
            export_table_to_s3_task = SqlToS3Operator(
                task_id=f"extract_{table}",
                sql_conn_id="classicmodels_mysql_conn",
                query=f"SELECT * FROM classicmodels.{table.lower()}",
                s3_bucket=s3_bucket,
                s3_key=f"bronze/{table}/{{{{ ds.replace('-', '/') }}}}/{table}_data.csv",
                aws_conn_id="aws_default_conn",
                file_format="csv",
                replace=True
            )

            transform_task = transform_data.override(
                task_id=f"transform_{table}"
            )(table)
            
            export_table_to_s3_task >> transform_task

            task_groups.append(tg)
    
    notify_task = notify_valid_records.override(task_id="notify_valid_records")(TABLES)
    
    start_task >> task_groups >> notify_task >> end_task

classic_models_dag()
    
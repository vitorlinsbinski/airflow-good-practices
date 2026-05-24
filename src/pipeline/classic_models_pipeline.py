from airflow.sdk import task, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from io import StringIO

from src.etl.transform import drop_null_and_remove_duplicates

import pandas as pd

@task
def transform_data(table: str, **context) -> None:
    s3_bucket = Variable.get("s3_raw_data_bucket")
    
    partition = context["ds"].replace("-", "/")
    
    raw_key = f"bronze/{table}/{partition}/{table}_data.csv"
    trusted_key = f"silver/{table}/{partition}/{table}_data.csv"
    s3_hook = S3Hook(aws_conn_id="aws_default_conn")
    obj = s3_hook.get_key(
        key=raw_key, 
        bucket_name=s3_bucket
    )
    assert obj is not None
    df_raw = pd.read_csv(obj.get()["Body"]) 
    assert not df_raw.empty
    
    df_transformed = drop_null_and_remove_duplicates(df_raw)
    num_valid_records = len(df_transformed)
    
    csv_buffer = StringIO()
    df_transformed.to_csv(csv_buffer, index=False)
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=trusted_key,
        bucket_name=s3_bucket,
        replace=True
    )
    
    context["ti"].xcom_push(key="valid_records", value=num_valid_records)

@task
def notify_valid_records(tables: list[str], **context) -> None:
    for table in tables:
        group_id = f"{table}_tg"
        task_id = f"transform_{table}"
        task_id = f"{group_id}.{task_id}" if len(tables) > 1 else task_id
        
        valid_records = context["ti"].xcom_pull(
            task_ids=task_id, key="valid_records"
        )
        
        print(f"Number of valid records in table {table}: {valid_records}")
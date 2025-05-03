from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator 
from datetime import datetime
from datetime import timedelta 
import gdown
import pandas as pd 
from io import BytesIO, StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup 

gdrive_urls = {
    "data_movie_lens": "https://drive.google.com/uc?id=1-3S-XOgZyo9D3sVoXtjPvmFdsihjfQhN", 
    "item_movie_lens": "https://drive.google.com/uc?id=188tIKLJKek62rGmzj1Ylc03fe4Pgb5co",  
    "user_movie_lens": "https://drive.google.com/uc?id=1_wAww5beF2K7dpx-SU_gUUddNWeaeZqv"
}
s3_bucket = "dec-kate--lightat2003"
raw_bucket_prefix = "raw/"
transformed_bucket_prefix = "transformed/"
SNOWFLAKE_STAGE = "@MDP_STAGE"


def gdrive_to_s3():
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    for file_name, url in gdrive_urls.items():
        buffer = BytesIO()
        gdown.download(url, buffer, quiet=False)  
        buffer.seek(0)
        s3_key = f"{raw_bucket_prefix}{file_name}.csv"
        s3_hook.upload_fileobj(
            file_obj=buffer,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )

        
def transform_raw_data_in_s3():
    s3_hook = S3Hook(aws_conn_id="s3_conn")

    year_batches = [
        (1922, 1930), (1931, 1940), (1941, 1950),
        (1951, 1960), (1961, 1970), (1971, 1980),
        (1981, 1990), (1991, 1998)
    ]

    for file_name in gdrive_urls.keys():  
        csv_data = s3_hook.read_key(f"{raw_bucket_prefix}{file_name}.csv", bucket_name=s3_bucket)  
        df = pd.read_csv(StringIO(csv_data))  
        
        if 'release_date' in df.columns:
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
            df = df.dropna(subset=["release_date"]) 
            df["year"] = df["release_date"].dt.year  
            
        if file_name in ["data_movie_lens", "user_movie_lens"]:
            buffer = StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)  
            s3_hook.upload_fileobj(
                file_obj=buffer,
                key=f"{transformed_bucket_prefix}{file_name}.csv",  
                bucket_name=s3_bucket,
                replace=True
            )
        elif file_name == "item_movie_lens":
            # Process item_movie_lens by year batches
            for start_year, end_year in year_batches:
                batch_df = df[
                    (df["year"] >= start_year) & 
                    (df["year"] <= end_year)
                ]
                
                if not batch_df.empty:
                    buffer = StringIO()
                    batch_df.to_csv(buffer, index=False)
                    buffer.seek(0)  
                    s3_hook.upload_fileobj(
                        file_obj=buffer,
                        key=f"{transformed_bucket_prefix}sales_data_{start_year}_{end_year}.csv", 
                        bucket_name=s3_bucket,
                        replace=True
                    )
            
            # upload the full item_movie_lens file
            buffer = StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            s3_hook.upload_fileobj(
                file_obj=buffer,
                key=f"{transformed_bucket_prefix}{file_name}.csv",
                bucket_name=s3_bucket,
                replace=True
            )
        
with DAG(
    dag_id="movie_data_pipeline",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_from_gdrive",
        python_callable=gdrive_to_s3
    )
    
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_raw_data_in_s3
    )

    load_task = SnowflakeOperator(
        task_id="load_to_snowflake",
        sql=f"""
            COPY INTO MOVIE_DATA 
            FROM {SNOWFLAKE_STAGE}/transformed/item_movie_lens.csv
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
            
            COPY INTO USERS_DATA 
            FROM {SNOWFLAKE_STAGE}/transformed/user_movie_lens.csv
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
            
            COPY INTO USER_RATINGS
            FROM {SNOWFLAKE_STAGE}/transformed/data_movie_lens.csv
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
        """,
        snowflake_conn_id="snowflake_conn"
    )

    
    
    extract_task >> transform_task >> load_task
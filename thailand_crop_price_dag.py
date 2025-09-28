from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import pendulum
with DAG(
    dag_id='thailand_crop_price',
    start_date=pendulum.parse('2025-09-28'),
    schedule=None,
    catchup=False
) as dag:

    @task()
    def save_data_from_kaggle_to_storage():
        import kagglehub
        from kagglehub import KaggleDatasetAdapter
        import os
        
        os.environ['KAGGLE_USERNAME'] = Variable.get("kaggle_user")
        os.environ['KAGGLE_KEY'] = Variable.get("kaggle_key")
        
        df = kagglehub.dataset_load(
            KaggleDatasetAdapter.PANDAS,
            "unitednations/global-food-agriculture-statistics",
            "current_FAO/raw_files/Prices_E_All_Data_(Normalized).csv",
            pandas_kwargs={"encoding": "latin-1"},
        )

        df.to_parquet('/home/airflow/gcs/data/thailand_crop_price.parquet',index = False)

    t1 = save_data_from_kaggle_to_storage()
    t1
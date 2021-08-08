from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from cryptowatch_to_s3 import CryptowatchToS3


default_args = {
    "owner": "udacity",
    #     "start_date": days_ago(1),
    "start_date": timezone.datetime(2021, 6, 1),
}

dag = DAG(
    "etl_pipeline",
    default_args=default_args,
    description="ETL Pipeline for Automating Monthly Cryptoasset Reports.",
    schedule_interval="@monthly",
)

start_operator = DummyOperator(
    task_id="Begin_execution",
    dag=dag,
)

crypto_task = PythonOperator(
    task_id="Getting_crypto_data",
    dag=dag,
    python_callable=CryptowatchToS3,
    provide_context=True,
    op_kwargs={
        "pair_base": ["btc", "eth", "ada", "doge", "dot", "uni"],
        "pair_curr": ["usd", "gbp", "eur"],
    },
)

end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=dag,
)

start_operator >> crypto_task >> end_operator
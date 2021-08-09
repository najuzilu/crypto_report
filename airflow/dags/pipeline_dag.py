# python3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from cryptowatch_to_s3 import CryptowatchToS3
from newsapi_to_s3 import NewsApiToS3
from sentiment import DetectNewsSentiment


default_args = {
    "owner": "udacity",
    "start_time": timezone.utcnow(),
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
    task_id="Get_crypto_to_s3",
    dag=dag,
    python_callable=CryptowatchToS3,
    provide_context=True,
    op_kwargs={
        "pair_base": ["btc", "eth", "ada", "doge", "dot", "uni"],
        "pair_curr": ["usd", "gbp", "eur"],
    },
)

newsapi_task = PythonOperator(
    task_id="Get_newsapi_to_s3",
    dag=dag,
    python_callable=NewsApiToS3,
    provide_context=True,
    op_kwargs={
        "categories": [
            "bitcoin",
            "ethereum",
            "cardano",
            "dogecoin",
            "polkadot",
            "uniswap",
        ],
        "language": "en",
        "sentiment_column": "title",
    },
)

sentiment_task = PythonOperator(
    task_id="Detect_news_sentiment",
    dag=dag,
    python_callable=DetectNewsSentiment,
    op_kwargs={"column_name": "title"},
)

end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=dag,
)

start_operator >> [crypto_task, newsapi_task]
newsapi_task >> sentiment_task
sentiment_task >> end_operator
crypto_task >> end_operator

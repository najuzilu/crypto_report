# python3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from cryptowatch_to_s3 import CryptowatchToS3
from newsapi_to_s3 import NewsApiToS3
from sentiment import DetectNewsSentiment
from operators import SentimentQualityOperator
from utils import MyConfigParser


my_config = MyConfigParser()
AWS_REGION = my_config.aws_region()
S3_BUCKET = my_config.s3_bucket()


default_args = {
    "owner": "udacity",
    "start_date": timezone.utcnow(),
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

sentiment_check = SentimentQualityOperator(
    task_id="Check_sentiment_quality",
    dag=dag,
    aws_credentials_id="aws_credentials",
    region=AWS_REGION,
    s3_bucket=S3_BUCKET,
    s3_prefix="news-articles-sent",
)

end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=dag,
)

start_operator >> [crypto_task, newsapi_task]
newsapi_task >> sentiment_task
sentiment_task >> sentiment_check
sentiment_check >> end_operator
crypto_task >> end_operator

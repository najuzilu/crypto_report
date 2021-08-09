# python3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from cryptowatch_to_s3 import CryptowatchToS3
from newsapi_to_s3 import NewsApiToS3
from sentiment import DetectNewsSentiment
from operators import S3BucketOperator
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

crypto_bucket_task = S3BucketOperator(
    task_id="Check_candlestick_bucket",
    dag=dag,
    aws_credentials_id="aws-credentials",
    region=AWS_REGION,
    s3_bucket=S3_BUCKET,
    s3_prefix="ohlc-candlestick",
)

news_bucket_task = S3BucketOperator(
    task_id="Check_newsapi_bucket",
    dag=dag,
    aws_credentials_id="aws-credentials",
    region=AWS_REGION,
    s3_bucket=S3_BUCKET,
    s3_prefix="news-articles",
)


sentiment_task = PythonOperator(
    task_id="Detect_news_sentiment",
    dag=dag,
    python_callable=DetectNewsSentiment,
    op_kwargs={"column_name": "title"},
)

sent_bucket_task = S3BucketOperator(
    task_id="Check_sentiment_bucket",
    dag=dag,
    aws_credentials_id="aws-credentials",
    region=AWS_REGION,
    s3_bucket=S3_BUCKET,
    s3_prefix="news-articles-sent",
)

end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=dag,
)

start_operator >> [crypto_task, newsapi_task]
crypto_task >> crypto_bucket_task >> end_operator
newsapi_task >> news_bucket_task >> sentiment_task >> sent_bucket_task >> end_operator

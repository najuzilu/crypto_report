# python3
from operators import StageToRedshiftOperator
from operators import LoadDimensionOperator
from operators import HasRowsOperator
from airflow import DAG


def get_s3_to_redshift_dag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    aws_credentials_id,
    region,
    table,
    s3_bucket,
    s3_key,
    *args,
    **kwargs,
):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs,
    )

    # move data to staging table
    stage_task = StageToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        region=region,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
    )

    # implement a data quality check
    check_task = HasRowsOperator(
        task_id=f"check_{table}_data",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
    )

    stage_task >> check_task

    return dag


def load_dimension_tables_dag(
    parent_dag,
    task_id,
    redshift_conn_id,
    aws_credentials_id,
    table,
    query,
    *args,
    **kwargs,
):

    dag = DAG(
        f"{parent_dag}.{task_id}",
        **kwargs,
    )

    load_dimension_table = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        sql_query=query,
    )

    # implement a data quality check
    check_task = HasRowsOperator(
        task_id=f"check_{table}_data",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
    )

    # load task
    load_dimension_table >> check_task

    return dag

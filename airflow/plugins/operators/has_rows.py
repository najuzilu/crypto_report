# python3
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
import logging


class HasRowsOperator(BaseOperator):
    """
    Operator that performs checks on tables. It is used to
    check dimension and staging tables.

    Attributes
    ----------
    redshift_conn_id : str
        Airflow connection id to Amazon Redshift
    table : str
        Name of table where data check must be performed

    Methods
    -------
    execute(context):
        Performs multiple checks on Redshift table
    """

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", *args, **kwargs):
        """
        Constructs all attributes for the create operator
        :param redshift_conn_id:      Airflow connection id to Amazon Redshift
        :param table:                 Name of table where checks must be performed
        """
        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Connect to Redshit through PostgresHook, check that table has observations
        and is not empty
        :param context:                   Context passed through Airflow
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM public.{self.table}")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(
                f"Data quality check failed. {self.table} returned no results"
            )

        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(
                f"Data quality check failed. {self.table} contained 0 rows"
            )

        logging.info(
            f"""Data quality on table {self.table}
            check passed with {records[0][0]} recodrs"""
        )

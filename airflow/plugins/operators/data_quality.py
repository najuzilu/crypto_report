from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Operator that performs checks on Redshift fact table

    Attributes
    ----------
    redshift_conn_id : str
        Airflow connection id to Amazon Redshift
    tables : List[str]
        List containing all table names

    Methods
    -------
    execute(context):
        Performs multiple checks on Redshift tables
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", tables=[], *args, **kwargs):
        """
        Constructs all attributes for the create operator
        :param redshift_conn_id:      Airflow connection id to Amazon Redshift
        :param tables:                List containing all table names
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Connect to Redshit through PostgresHook, for each table name check that
        the table has observations and is not empty
        :param context:                   Context passed through Airflow
        """
        redshift = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM public.{table};")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results."
                )

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows."
                )

            self.log.info(
                f"""Data quality on table {table} check
                passed with {num_records} records."""
            )

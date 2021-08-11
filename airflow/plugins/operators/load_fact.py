from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class LoadFactOperator(BaseOperator):
    """
    Operator that loads Redshift fact tables

    Attributes
    ----------
    redshift_conn_id : str
        Airflow connection id to Amazon Redshift
    query : str
        SQL query to insert data

    Methods
    -------
    execute(context):
        Loads data from Redshift staging tables to fact table
    """

    ui_color = "#F98866"

    @apply_defaults
    def __init__(self, redshift_conn_id="", query="", *args, **kwargs):
        """
        Constructs all attributes for the load fact table operator
        :param redshift_conn_id:      Airflow connection id to Amazon Redshift
        :param query:                 SQL query to insert data
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        """
        Loads data from Redshift staging tables to fact table
        :param context:                   Context passed through Airflow
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Loading data in fact table")
        redshift_hook.run(f"{self.query}")

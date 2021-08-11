from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Operator that loads Redshift dimension tables

    Attributes
    ----------
    redshift_conn_id : str
        Airflow connection id to Amazon Redshift
    table : str
        Name of table where data will be ingested
    query : str
        SQL query to insert data

    Methods
    -------
    execute(context):
        Loads data from Redshift staging tables to dimension table
    """

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", query="", *args, **kwargs):
        """
        Constructs all attributes for the load dimension table operator
        :param redshift_conn_id:      Airflow connection id to Amazon Redshift
        :param table:                 Name of table where data will be ingested
        :param query:                 SQL query to insert data
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        """
        Loads data from Redshift staging tables to dimension table
        :param context:                   Context passed through Airflow
        """
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Inserting data to dimension table `{self.table}`...")
        redshift.run(self.query.format(self.table))

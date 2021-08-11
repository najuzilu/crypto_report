from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
import pathlib


class DropTablesOperator(BaseOperator):
    """
    Operator that drops tables on Amazon Redshift

    Attributes
    ----------
    redshift_conn_id : str
        Airflow connection id to Redshift
    sql_file : str
        Location of SQL file with queries to create Redshit tables

    Methods
    -------
    execute(context):
        Connect to Redshift and drop tables
    """

    ui_color = "#32a852"
    sql_file = str(
        pathlib.Path(
            pathlib.Path(__file__).parent.resolve().parents[1], "drop_tables.sql"
        )
    )

    @apply_defaults
    def __init__(self, redshift_conn_id="", *args, **kwargs):
        """
        Constructs all attributes for the drop operator
        :param  redshift_conn_id:      Airflow connection id to Amazon Redshift
        """
        super(DropTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Connect to Redshit through PostgresHook, read SQL queries from SQL file and
        execute each query command to drop tables
        :param context:                   Context passed through Airflow
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Dropping Redshift tables")

        sql_file = open(DropTablesOperator.sql_file, "r").read()
        commands = sql_file.split(";")

        for query in commands:
            if query.rstrip() != "":
                self.log.info(f"Executing SQL command: {query};")
                redshift.run(f"{query}")

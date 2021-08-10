from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
import pathlib


class DropTablesOperator(BaseOperator):
    ui_color = "#32a852"
    sql_file = str(
        pathlib.Path(
            pathlib.Path(__file__).parent.resolve().parents[1], "drop_tables.sql"
        )
    )

    @apply_defaults
    def __init__(self, redshift_conn_id="", *args, **kwargs):

        super(DropTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Dropping Redshift tables")

        sql_file = open(DropTablesOperator.sql_file, "r").read()
        commands = sql_file.split(";")

        for query in commands:
            if query.rstrip() != "":
                self.log.info(f"Executing SQL command: {query};")
                redshift.run(f"{query}")

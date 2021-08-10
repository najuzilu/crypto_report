from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
import pathlib


class CreateTablesOperator(BaseOperator):
    ui_color = "#32a852"
    sql_file = str(
        pathlib.Path(
            pathlib.Path(__file__).parent.resolve().parents[1], "create_tables.sql"
        )
    )

    @apply_defaults
    def __init__(self, redshift_conn_id="", *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating Redshift tables")

        sql_file = open(CreateTablesOperator.sql_file, "r").read()
        commands = sql_file.split(";")

        for query in commands:
            if query.rstrip() != "":
                self.log.info(f"Executing SQL command: {query};")
                redshift.run(f"{query}")

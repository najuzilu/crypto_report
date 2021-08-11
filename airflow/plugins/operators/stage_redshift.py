# python3
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    """
    Operator that performs checks on a Amazon S3 bucket and prefix

    Attributes
    ----------
    redshift_conn_id : str
        Airflow connection id to Amazon Redshift
    aws_credentials_id : str
        Airflow connection id to Amazon Web Services
    region : str
        Amazon S3 bucket region
    table : str
        Name of table where data will be ingested
    s3_bucket : str
        Amazon S3 bucket name
    s3_key : str
        Amazon S3 bucket prefix
    delimiter : str
        [Optional] Delimiter to use
    ignore_headers : int
        [Optional] Skip headers when reading csv file

    Methods
    -------
    execute(context):
        Loads data from S3 to Redshift staging tables
    """

    ui_color = "#358140"
    template_fields = ("s3_key",)
    copy_sql = """
        COPY public.{}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        DELIMITER '{}'
        CSV
        IGNOREHEADER {};
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        region="",
        table="",
        s3_bucket="",
        s3_key="",
        delimiter=",",
        ignore_headers=1,
        *args,
        **kwargs,
    ):
        """
        Constructs all attributes for the stage table operator
        :param redshift_conn_id:            Airflow connection id to Amazon Redshift
        :param aws_credentials_id :         Airflow connection id to Amazon Web Services
        :param region :                     Amazon S3 bucket region
        :param table :                      Name of table where data will be ingested
        :param s3_bucket :                  Amazon S3 bucket name
        :param s3_key :                     Amazon S3 bucket prefix
        :param delimiter :                  [Optional] Delimiter to use
        :param ignore_headers :             [Optional] Skip headers when reading CSV
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        """
        Loads data from S3 to Redshift dimension table
        :param context:                   Context passed through Airflow
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.delimiter,
            self.ignore_headers,
        )
        redshift.run(formatted_sql)

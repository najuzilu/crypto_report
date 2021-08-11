from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator


class S3BucketOperator(BaseOperator):
    """
    Operator that performs checks on a Amazon S3 bucket and prefix

    Attributes
    ----------
    aws_credentials_id : str
        Airflow connection id to Amazon Web Services
    region : str
        Amazon S3 bucket region
    s3_bucket : str
        Amazon S3 bucket name
    s3_prefix : str
        Amazon S3 bucket prefix

    Methods
    -------
    execute(context):
        Performs multiple checks on Amazon S3 bucket
    """

    ui_color = "#9dc949"
    template_fields = ("s3_prefix",)

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id="",
        region="",
        s3_bucket="",
        s3_prefix="",
        *args,
        **kwargs,
    ):
        """
        Constructs all attributes for the bucket object
        :param  aws_credentials_id:      Airflow connection id to Amazon Web Services
        :param region:                   Amazon S3 bucket region
        :param s3_bucket:                Amazon S3 bucket name
        :param s3_prefix:                Amazon S3 bucket prefix
        """
        super(S3BucketOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def execute(self, context):
        """
        Performs multiple checks on Amazon S3 bucket: does bucket with prefix exist and
        does the bucket and prefix have less than 1 rendered key
        :param context:                   Context passed through Airflow
        """

        s3_hook = S3Hook(self.aws_credentials_id)

        if not s3_hook.check_for_prefix(self.s3_bucket, self.s3_prefix, delimiter="/"):
            raise ValueError(
                f"Prefix {self.s3_prefix} does not exist in {self.s3_bucket}."
            )

        num_keys = len(s3_hook.list_keys(self.s3_bucket, self.s3_prefix))
        if num_keys < 1:
            raise ValueError(
                f"No keys found in bucket under s3://{self.s3_bucket}/{self.s3_prefix}."
            )

        self.log.info(
            f"""{__file__} on {self.s3_bucket}/{self.s3_prefix}
            passed with {num_keys} keys."""
        )

from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.CreateTablesOperator,
        operators.DropTablesOperator,
        operators.S3BucketOperator,
        operators.StageToRedshiftOperator,
        operators.HasRowsOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
    ]
    helpers = [
        helpers.SqlQueries,
    ]

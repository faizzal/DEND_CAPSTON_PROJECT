from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
import helpers
import operators


class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    helpers = [
        helpers.sqlCreateAndQuery,
        helpers.sqlCreateTables,
        helpers.sqlLoadingQuery 
    ],
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    
# Defining the plugin class


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


'''
This class contain main method that help for
check qulity of data by check avlibality of record 
have free to change the way of check column
'''
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,redshift="", 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift= redshift
        self.tables = ["dim_airports", "dim_carriers", "dim_weather", "fact_flights"]
        self.columns=["iata", "code", "date", "flight_id"]

      


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift)
        for i in range(len(self.tables)):
            records = redshift.get_records("SELECT COUNT(*) FROM {} where {} IS NULL".format(self.tables[i], self.columns[i]))
            if len(records) > 1 or len(records[0]) > 1:
                raise ValueError(f"Data quality check failed. {self.tables[i]} returned no results")
            num_records = records[0][0]
            if num_records > 1: 
                raise ValueError(f"Data quality check failed. {self.tables[i]} contained 0 rows")
                self.log.info(f"Data quality on table {self.tables[i]} check passed with {records[0][0]} records")
        
                   
        
        self.log.info('Data Quality check Completed')
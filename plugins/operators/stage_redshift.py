from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

'''
This class contain main method that help for
copy  data from data source into stage tables is part of ETL method
for transfer data  
'''

class StageToRedshiftOperator(BaseOperator):
    ui_color =  '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 credentials="",
                 redshift="", 
                 s3="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.credentials = credentials
        self.redshift = redshift
        self.s3 = s3
        
    def execute(self, context):  
        aws_hook = AwsHook(self.credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift) 
        redshift.run("Truncate  {}".format(self.table))  
        copy_to_redshift = ( """ COPY {0} FROM 's3://{1}' 
                                 CREDENTIALS 'aws_access_key_id={2};aws_secret_access_key={3}'
                                 delimiter ',' IGNOREHEADER 1;
                                 """.format(self.table, self.s3,
                                            credentials.access_key,
                                            credentials.secret_key
                                           )
                           )
                            
        self.log.info(f"start copy {self.table}") 
        redshift.run(copy_to_redshift) 
        self.log.info(f"coping  {self.table} completed")
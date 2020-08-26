from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
This class contain main method that help for
load data from stage tables into Dimension tables is part of ETL method
for transfer data  
'''
class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'
  
    @apply_defaults
    def __init__(self,table = "", 
                 redshift = "", 
                 column ='', 
                 clear_data= "",
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift = redshift
        self.column = column
        self.clear_data=clear_data
        self.sql = sql


    def execute(self, context): 
        redshift = PostgresHook(postgres_conn_id=self.redshift)
        self.log.info(f'start Tha Diministion table {self.table} is  loaded data ')
        if self.clear_data == 'YES': 
            redshift.run("DELETE FROM  {0}".format(self.table))  
            redshift.run("""INSERT INTO {0} {1} 
             {2} """.format(self.table, self.column,  self.sql))
        
        else:
            redshift.run("""INSERT INTO {0} {1} 
             {2} """.format(self.table, self.column,  self.sql))
          
        
        self.log.info(f'Tha Diministion table {self.table} is  loaded data ')
         
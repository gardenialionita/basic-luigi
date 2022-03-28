from sqlalchemy import create_engine
import luigi
import pandas as pd

#connect to db1 database and will export all data from names table to DB1_output.csv
class QueryDB1(luigi.Task):  

    #checks if it needs another task to be completed first  
    def requires(self):
        return []    
        
    def output(self):
        return luigi.LocalTarget("DB1_output.csv")    
        
    def run(self):
        engine = create_engine('sqlite:///db1')
        results = pd.read_sql_query('SELECT * from salaries',engine)
        f = self.output().open('w')
        results.to_csv(f,encoding = 'utf-8',index=False,header=True,quoting=2)
        f.close()

#connect to db2 database and will export all data from names table to DB2_output.csv
class QueryDB2(luigi.Task):    
    
    def requires(self):
        return []    
        
    def output(self):
        return luigi.LocalTarget("DB2_output.csv")    
        
    def run(self):
        engine = create_engine('sqlite:///db2')
        results = pd.read_sql_query('SELECT * from names',engine)
        f = self.output().open('w')
        results.to_csv(f,encoding = 'utf-8',index=False,header=True,quoting=2)
        f.close()

class CreateReport(luigi.Task):    
    
    #if those QueryDB1 and QueryDB2 are not executed, the CreateReport class will fail with an error
    def requires(self):
        return [QueryDB1(),QueryDB2()]    
    
    def output(self):
        return luigi.LocalTarget("Report.csv")    
        
    def run(self):
        # reads the data from the classes executed
        df1 = pd.read_csv("DB1_output.csv", header = 0, encoding = 'utf-8',index_col = False)
        df2 = pd.read_csv("DB2_output.csv", header = 0, encoding = 'utf-8',index_col = False)
        #creates a new dataframe which are merged with the common column named “id”
        df3 = pd.merge(df1,df2,how='inner',on=['id'])
        f = self.output().open('w')
        df3.to_csv(f,encoding = 'utf-8',index=False,header=True,quoting=2)
        f.close()

if __name__ == '__main__':
    # instruct luigi which class to execute, and to connect to the Web UI (luigid) in order to create a dashboard with our executed ETL 
    luigi.run(main_task_cls=CreateReport,local_scheduler=False)
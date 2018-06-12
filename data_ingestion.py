import luigi
import pandas as pd

class DataIngestion(luigi.Task):

    def run(self):
        emailsClassificados = pd.read_csv('raw_emails.csv')
        print("Data Ingestion is running...")
        print(len(emailsClassificados))
        print('\n')
        #TODO
        #transforma o arquivo raw_emails.csv para emails.csv


    def output(self):
        return luigi.LocalTarget("emails.csv")
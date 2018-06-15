import luigi
import pandas as pd

class TweetDataIngestion(luigi.Task):

    def run(self):
        tweetsClassificados = pd.read_csv('raw_tweets.csv')
        print("Tweet Data Ingestion is running...")
        print(len(tweetsClassificados))
        print('\n')

        #TODO
        #Transforma o arquivo raw_tweets.csv para tweets.csv automaticamente
        #Por exemplo, transformar as labels para números:
        # Comercial = 1
        # Técnico = 2
        # Cursos = 3


    def output(self):
        return luigi.LocalTarget("tweets.csv")
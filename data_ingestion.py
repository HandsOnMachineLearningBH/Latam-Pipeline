import luigi
import pandas as pd

class TweetDataIngestion(luigi.Task):

    def run(self):
        tweetsClassificados = pd.read_csv('raw_tweets.csv')
        print("Tweet Data Ingestion is running...")
        print(len(tweetsClassificados))
        print('\n')
        #TODO
        #transforma o arquivo raw_tweets.csv para tweets.csv


    def output(self):
        return luigi.LocalTarget("tweets.csv")
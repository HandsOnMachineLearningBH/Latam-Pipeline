import luigi
import pandas as pd

from data_ingestion import TweetDataIngestion


class DataPreProcessingX(luigi.Task):

    def requires(self):
        yield TweetDataIngestion();

    def run(self):

        classificacoes = pd.read_csv(TweetDataIngestion().output().path)

        print("Data Pre-ProcessingX is running...")
        print(len(classificacoes['tweet']))

        textosPuros = classificacoes['tweet']
        textosQuebrados = textosPuros.str.lower().str.split(' ')
        dicionario = set()

        for lista in textosQuebrados:
            dicionario.update(lista)

        totalDePalavras = len(dicionario)
        tuplas = zip(dicionario, range(totalDePalavras))
        tradutor = {palavra: indice for palavra, indice in tuplas}
        print(totalDePalavras)

        def vetorizar_texto(texto, tradutor):
            vetor = [0] * len(tradutor)
            for palavra in texto:
                if palavra in tradutor:
                    posicao = tradutor[palavra]
                    vetor[posicao] += 1

            return vetor

        vetoresDeTexto = [vetorizar_texto(texto, tradutor) for texto in textosQuebrados]

        X = vetoresDeTexto

        tamanho_de_treino = int(len(classificacoes['tweet']))
        treino_dados = X[0:tamanho_de_treino]

        with open(self.output().path, 'a') as the_file:
            the_file.write('tweetNormalizado' + '\n')
            for treino_dado in treino_dados:
                the_file.write(str(treino_dado) + '\n')
            the_file.close()

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineX.csv")


class TargetDataPreProcessing(luigi.Task):

    def requires(self):
        yield TweetDataIngestion();

    def run(self):
        classificacoes = pd.read_csv(TweetDataIngestion().output().path)

        print("Target Data Pre-Processing is running...")
        print(len(classificacoes))

        treino_marcacoes = classificacoes['classificacao']

        with open(self.output().path, 'a') as the_file:
            the_file.write('classificacao' + '\n')
            for marca in treino_marcacoes:
                the_file.write(str(marca) + '\n')
            the_file.close()

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineMarcas.csv")
import luigi
import pandas as pd

from data_ingestion import TweetDataIngestion


def writeExampleOnLineFile(the_file, treino_dado, treino_marcacao):
    for word in treino_dado:
        the_file.write(str(word) + ', ')
    the_file.write(str(treino_marcacao))


class DataPreProcessing(luigi.Task):

    def requires(self):
        yield TweetDataIngestion();

    def run(self):

        classificacoes = pd.read_csv(TweetDataIngestion().output().path)

        print("Data Pre-ProcessingX is running...")

        textosPuros = classificacoes['tweet']
        textosQuebrados = textosPuros.str.lower().str.split(' ')
        dicionario = set()

        for lista in textosQuebrados:
            dicionario.update(lista)

        totalDePalavras = len(dicionario)
        tuplas = zip(dicionario, range(totalDePalavras))
        tradutor = {palavra: indice for palavra, indice in tuplas}

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

        treino_marcacoes = classificacoes['classificacao']

        print('Classificacoes tipo: ')
        print(type(classificacoes))

        with open(self.output().path, 'a') as the_file:
            for treino_dado, treino_marcacao in zip(treino_dados, treino_marcacoes):
                writeExampleOnLineFile(the_file, treino_dado, treino_marcacao)
                the_file.write('\n')
            the_file.close()

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineExamples.csv")
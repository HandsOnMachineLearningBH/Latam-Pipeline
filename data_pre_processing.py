import luigi
import pandas as pd


class DataPreProcessingTextoQuebrado(luigi.Task):

    def run(self):
        classificacoes = pd.read_csv('emails.csv')
        textosPuros = classificacoes['email']
        textosPuros.str.lower().str.split(' ')

    def output(self):
        return luigi.LocalTarget("/tmp/textoQuebrado.csv")


class StoreDataIngestionDicionario(luigi.Task):

    def requires(self):
        yield DataPreProcessingTextoQuebrado()

    def run(self):
        print("Dicionario2")
        textosQuebrados = pd.read_csv(DataPreProcessingTextoQuebrado().output().path)
        print(textosQuebrados)
        dicionario = set()
        for lista in textosQuebrados:
            dicionario.update(lista)

    def output(self):
        return luigi.LocalTarget("/tmp/dicionario.csv")



class DataPreProcessingX(luigi.Task):

    def requires(self):
        yield StoreDataIngestionDicionario()
        yield DataPreProcessingTextoQuebrado()

    def run(self):
        dicionario = pd.read_csv(StoreDataIngestionDicionario().output().path)
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
        textosQuebrados = pd.read_csv(DataPreProcessingTextoQuebrado().output().path)
        vetoresDeTexto = [vetorizar_texto(texto, tradutor) for texto in textosQuebrados]

    def output(self):
        return luigi.LocalTarget("/tmp/vetoresDeTexto.csv")

class DataPreProcessingY(luigi.Task):

    def run(self):
        classificacoes = pd.read_csv('emails.csv')
        marcas = classificacoes['classificacao']

    def output(self):
        return luigi.LocalTarget("/tmp/marcas.csv")

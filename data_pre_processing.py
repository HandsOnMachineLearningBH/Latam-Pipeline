import luigi
import pandas as pd

class DataPreProcessingX(luigi.Task):

    def run(self):
        classificacoes = pd.read_csv('emails.csv')
        textosPuros = classificacoes['email']
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

        marcas = classificacoes['classificacao']

        X = vetoresDeTexto
        Y = marcas

        porcentagem_de_treino = 0.8

        tamanho_de_treino = int(porcentagem_de_treino * len(Y))
        treino_dados = X[0:tamanho_de_treino]

        with open(self.output().path, 'a') as the_file:
            the_file.write('emailNormalizado' + '\n')
            for treino_dado in treino_dados:
                the_file.write(str(treino_dado) + '\n')
            the_file.close()

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineX.csv")

def getTreinoMarcacoes():
    classificacoes = pd.read_csv('emails.csv')
    marcas = classificacoes['classificacao']
    Y = marcas
    porcentagem_de_treino = 0.8
    tamanho_de_treino = int(porcentagem_de_treino * len(Y))
    treino_marcacoes = Y[0:tamanho_de_treino]
    return treino_marcacoes


class DataPreProcessingTarget(luigi.Task):

    def run(self):
        treino_marcacoes = getTreinoMarcacoes()
        with open(self.output().path, 'a') as the_file:
            the_file.write('classificacao' + '\n')
            for marca in treino_marcacoes:
                the_file.write(str(marca) + '\n')
            the_file.close()

    def output(self):
        return luigi.LocalTarget("/tmp/pipelinemarcas.csv")

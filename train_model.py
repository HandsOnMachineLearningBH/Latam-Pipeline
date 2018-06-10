
import luigi
import numpy as np
from sklearn.model_selection import cross_val_score
import pandas as pd
from data_pre_processing import DataPreProcessingX
from data_pre_processing import DataPreProcessingTarget


def fit_and_predict(nome, modelo, treino_dados, treino_marcacoes):

    k = 10
    scores = cross_val_score(modelo, treino_dados, treino_marcacoes, cv=k)
    taxa_de_acerto = np.mean(scores)
    msg = "Taxa de acerto do {0}: {1}".format(nome, taxa_de_acerto)
    print(msg)
    return taxa_de_acerto


def getTreinoMarcacoes():
    classificacoes = pd.read_csv('emails.csv')
    marcas = classificacoes['classificacao']
    Y = marcas
    porcentagem_de_treino = 0.8
    tamanho_de_treino = int(porcentagem_de_treino * len(Y))
    treino_marcacoes = Y[0:tamanho_de_treino]
    return treino_marcacoes


def getTreinoDados():
    classificacoes = pd.read_csv('emails.csv')
    textosPuros = classificacoes['email']
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
    porcentagem_de_treino = 0.8

    tamanho_de_treino = int(porcentagem_de_treino * len(X))
    treino_dados = X[0:tamanho_de_treino]
    return treino_dados


class Train(luigi.Task):

    def requires(self):
        yield DataPreProcessingX()
        yield DataPreProcessingTarget()

    def run(self):

        from sklearn.ensemble import AdaBoostClassifier
        modeloAdaBoost = AdaBoostClassifier(random_state=0)
        treino_dados = getTreinoDados()
        treino_marcacoes = getTreinoMarcacoes()
        resultadoAdaBoost = fit_and_predict("AdaBoostClassifier", modeloAdaBoost, treino_dados, treino_marcacoes)
        print(resultadoAdaBoost)

    def output(self):
        print("")


if __name__ == '__main__':
    luigi.run()

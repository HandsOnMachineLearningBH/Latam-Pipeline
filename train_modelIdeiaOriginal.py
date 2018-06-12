
import luigi
import numpy as np
from sklearn.model_selection import cross_val_score
import pandas as pd
from data_pre_processing import DataPreProcessingX
from data_pre_processing import TargetDataTraining


def fit_and_predict(nome, modelo, treino_dados, treino_marcacoes):
    print(treino_marcacoes.items)
    print(treino_dados)
    k = 10
    scores = cross_val_score(modelo, treino_dados, treino_marcacoes.items, cv=k)
    taxa_de_acerto = np.mean(scores)
    msg = "Taxa de acerto do {0}: {1}".format(nome, taxa_de_acerto)
    print(msg)
    return taxa_de_acerto


class Train(luigi.Task):

    def requires(self):
        yield DataPreProcessingX()
        yield TargetDataTraining()

    def run(self):

        from sklearn.ensemble import AdaBoostClassifier
        modeloAdaBoost = AdaBoostClassifier(random_state=0)
        treino_dados = pd.read_table(DataPreProcessingX().output().path)
        treino_marcacoes = pd.read_table(TargetDataTraining().output().path)
        resultadoAdaBoost = fit_and_predict("AdaBoostClassifier", modeloAdaBoost, treino_dados, treino_marcacoes)
        print(resultadoAdaBoost)
    def output(self):
        print("")


if __name__ == '__main__':
    luigi.run()

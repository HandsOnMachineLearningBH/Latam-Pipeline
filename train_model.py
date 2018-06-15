
import luigi
import numpy as np
from sklearn.model_selection import cross_val_score
import pandas as pd
from data_training import TargetDataTraining, InstanceDataTraining


def fit_and_predict(nome, modelo, treino_dados, treino_marcacoes):

    k = 10
    scores = cross_val_score(modelo, treino_dados, treino_marcacoes, cv=k)
    taxa_de_acerto = np.mean(scores)
    msg = "Taxa de acerto do {0}: {1}".format(nome, taxa_de_acerto)
    print(msg)
    return taxa_de_acerto

class Train(luigi.Task):

    def requires(self):
        yield InstanceDataTraining()
        yield TargetDataTraining()

    def run(self):
        treino_dados = []
        dadosNormalizados = pd.read_table(InstanceDataTraining().output().path)
        print("Train model is running")

        import ast
        for dado in dadosNormalizados['tweetNormalizado']:
            treino_dados.append(ast.literal_eval(dado))

        classificacoes = pd.read_csv(TargetDataTraining().output().path)
        treino_marcacoes = classificacoes['classificacao']

        from sklearn.ensemble import AdaBoostClassifier
        modeloAdaBoost = AdaBoostClassifier(random_state=0)
        resultadoAdaBoost = fit_and_predict("AdaBoostClassifier", modeloAdaBoost, treino_dados, treino_marcacoes)
        print("Escolher modelo com melhor valor: ")
        print(resultadoAdaBoost)

    def output(self):
        print("")


if __name__ == '__main__':
    luigi.run()

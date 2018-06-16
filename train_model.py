
import luigi
import numpy as np
from sklearn.model_selection import cross_val_score
import pandas as pd
from data_training import InstanceDataTraining


def fit_and_predict(nome, modelo, treino_dados, treino_marcacoes):
    k = 10
    scores = cross_val_score(modelo, treino_dados, treino_marcacoes, cv=k)
    print("Fiiiiit")

    taxa_de_acerto = np.mean(scores)
    msg = "Taxa de acerto do {0}: {1}".format(nome, taxa_de_acerto)
    print(msg)
    return taxa_de_acerto

class Train(luigi.Task):

    def requires(self):
        yield InstanceDataTraining()


    def run(self):
        feature_training = []
        trainingData = pd.read_csv(InstanceDataTraining().output().path)
        print("Train model is running")

        trainingData = trainingData.rename(index=str, columns={' 2': 'Target'})
        trainingData = trainingData.rename(index=str, columns={' ': 'Void'})
        trainingData.pop('Void')

        target_training = trainingData.pop('Target').tolist()

        for phrase in trainingData.values:
            feature_training.append(phrase)

        from sklearn.ensemble import AdaBoostClassifier
        modeloAdaBoost = AdaBoostClassifier(random_state=0)
        resultadoAdaBoost = fit_and_predict("AdaBoostClassifier", modeloAdaBoost, feature_training, target_training)
        print("Escolher modelo com melhor valor: ")
        print(resultadoAdaBoost)

    def output(self):
        print("")


if __name__ == '__main__':
    luigi.run()

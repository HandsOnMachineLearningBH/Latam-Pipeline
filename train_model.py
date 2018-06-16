
import luigi
import numpy as np
from sklearn.model_selection import cross_val_score
import pandas as pd
from data_training import DataTraining


def fit_and_predict(nome, modelo, treino_dados, treino_marcacoes):
    k = 10
    scores = cross_val_score(modelo, treino_dados, treino_marcacoes, cv=k)
    taxa_de_acerto = np.mean(scores)
    msg = "Taxa de acerto do {0}: {1}".format(nome, taxa_de_acerto)
    print(msg)
    return taxa_de_acerto

class TrainWithAdaBoostClassifier(luigi.Task):

    def requires(self):
        yield DataTraining()


    def run(self):
        print("Train model is running")

        trainingData = pd.read_csv(DataTraining().output().path)

        trainingData = trainingData.rename(index=str, columns={' 2': 'Target'})
        target_training = trainingData.pop('Target').tolist()

        trainingData = trainingData.rename(index=str, columns={' ': 'Void'})
        trainingData.pop('Void')
        feature_training = []
        for phrase in trainingData.values:
            feature_training.append(phrase)

        from sklearn.ensemble import AdaBoostClassifier
        modeloAdaBoost = AdaBoostClassifier(random_state=0)
        resultadoAdaBoost = fit_and_predict("AdaBoostClassifier", modeloAdaBoost, feature_training, target_training)

        with open(self.output().path, 'a') as the_file:
            the_file.write(str(resultadoAdaBoost))
            the_file.close()
        return resultadoAdaBoost

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineResultadoAdaBoost.csv")


class TrainWithOneVsRest(luigi.Task):

    def requires(self):
        yield DataTraining()

    def run(self):
        print("Train model is running")

        trainingData = pd.read_csv(DataTraining().output().path)

        trainingData = trainingData.rename(index=str, columns={' 2': 'Target'})
        target_training = trainingData.pop('Target').tolist()

        trainingData = trainingData.rename(index=str, columns={' ': 'Void'})
        trainingData.pop('Void')
        feature_training = []
        for phrase in trainingData.values:
            feature_training.append(phrase)

        from sklearn.multiclass import OneVsRestClassifier
        from sklearn.svm import LinearSVC

        modeloOneVsRest = OneVsRestClassifier(LinearSVC(random_state=0))
        resultadoOneVsRest = fit_and_predict("OneVsRest", modeloOneVsRest, feature_training, target_training)

        with open(self.output().path, 'a') as the_file:
            the_file.write(str(resultadoOneVsRest))
            the_file.close()
        return resultadoOneVsRest

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineResultadoOneVsRest.csv")

class Train(luigi.Task):

    def requires(self):
        yield TrainWithAdaBoostClassifier()
        yield TrainWithOneVsRest()

    def run(self):
        print("Chama todos os treinos para decidir quem é o melhor")
        resultadoAdaBoost = TrainWithAdaBoostClassifier().run()
        resultadoOneVsRest = TrainWithOneVsRest().run()
        print(resultadoAdaBoost)
        print(resultadoOneVsRest)

    def output(self):
        return;


if __name__ == '__main__':
    luigi.run()

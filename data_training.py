import luigi
import pandas as pd

from data_pre_processing import DataPreProcessing

def recoverEightyPercentOfDataForTraining(file):
    classificacoes = pd.read_csv(file)
    porcentagem_de_treino = 0.8
    tamanho_de_treino = int(porcentagem_de_treino * len(classificacoes))
    treino_instancias = classificacoes[0:tamanho_de_treino]

    #classificacoes.iloc[0:tamanho_de_treino]
    return treino_instancias


def writeExampleOnLineFile(the_file, example):
    for word in example:
        the_file.write(str(word) + ', ')


class InstanceDataTraining(luigi.Task):

    def requires(self):
        yield DataPreProcessing();

    def run(self):
        print("Data Training is running...")
        examples_for_training = recoverEightyPercentOfDataForTraining(DataPreProcessing().output().path)

        with open(self.output().path, 'a') as the_file:
            for example in examples_for_training.get_values():
                writeExampleOnLineFile(the_file, example)
                the_file.write('\n')
            the_file.close()

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineInstancias.csv")

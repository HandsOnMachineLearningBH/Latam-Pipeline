import luigi
import pandas as pd

from data_pre_processing import DataPreProcessingX, TargetDataPreProcessing


def get80PorcentoDeMarcacoesParaTreino(file):
    classificacoes = pd.read_csv(file)
    marcas = classificacoes['classificacao']
    Y = marcas
    porcentagem_de_treino = 0.8
    tamanho_de_treino = int(porcentagem_de_treino * len(Y))
    treino_marcacoes = Y[0:tamanho_de_treino]
    return treino_marcacoes

class TargetDataTraining(luigi.Task):

    def requires(self):
        yield TargetDataPreProcessing();

    def run(self):
        treino_marcacoes = get80PorcentoDeMarcacoesParaTreino(TargetDataPreProcessing().output().path)
        print("Target Data Training is running...")

        with open(self.output().path, 'a') as the_file:
            the_file.write('classificacao' + '\n')
            for marca in treino_marcacoes:
                the_file.write(str(marca) + '\n')
            the_file.close()

    def output(self):
        return luigi.LocalTarget("/tmp/pipelineTargetDataTraining.csv")


def get80PorcentoDeInstanciasParaTreino(file):
    classificacoes = pd.read_table(file)
    instancias = classificacoes['tweetNormalizado']
    X = instancias
    porcentagem_de_treino = 0.8
    tamanho_de_treino = int(porcentagem_de_treino * len(X))
    treino_instancias = X[0:tamanho_de_treino]
    return treino_instancias


class InstanceDataTraining(luigi.Task):

    def requires(self):
        yield DataPreProcessingX();

    def run(self):
        treino_instancias = get80PorcentoDeInstanciasParaTreino(DataPreProcessingX().output().path)
        print("Instance Data Training is running...")

        with open(self.output().path, 'a') as the_file:
            the_file.write('tweetNormalizado' + '\n')
            for treino_dado in treino_instancias:
                the_file.write(str(treino_dado) + '\n')
            the_file.close()


    def output(self):
        return luigi.LocalTarget("/tmp/pipelineInstancias.csv")

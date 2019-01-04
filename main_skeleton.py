'''
Questo script è un calco per implementare una pipeline Luigi.

Vanno settate, oltre alle logiche in run, il dataset_id e il model_id, lanciando tutto con

> python $filepath $NomeTask --local-scheduler

Se si vuole impostare un parametro da CLI scrivere

> python $filepath $NomeTask --$nomevariabile $valorevariabile --local-scheduler

ricordandosi di sostituire le '_' con '-' in $nomevariabile
'''

import luigi
import os
import pandas as pd
import yaml

from etl import collector
from preproc import report_data, proc_data
from model import set_model, train_xgb

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(os.path.join(dir_path, '..'))

datasetID = ''
modelID = ''


class ETL(luigi.Task):

    dataset_id = luigi.Parameter(default=datasetID)
    path_dataset = luigi.Parameter(default=r'')

    def requires(self):
        return []

    def run(self):
        pass

    def output(self):
        cfg = yaml.load(open(os.path.join('.', 'config', 'dataset', 'etl_'+self.dataset_id+'.yaml'), 'r'))
        return luigi.LocalTarget(cfg['FILEPATH'])
        

class PREPROC(luigi.Task):
    dataset_id = luigi.Parameter(default=datasetID)
    ls_keys = luigi.ListParameter(default=[''])
    sample = luigi.IntParameter(default=None)

    
    def requires(self):
        return [ETL()]

    def run(self):
        pass

    def output(self):
        cfg = yaml.load(open(os.path.join('.', 'config', 'dataset', self.dataset_id+'.yaml'), 'r'))
        return luigi.LocalTarget(os.path.join(os.getcwd(),cfg['FILEPATH']))


class TRAINMODEL(luigi.Task):
    dataset_id = luigi.Parameter(default=datasetID)
    model_id = luigi.Parameter(default=modelID)

    def requires(self):
        return [PREPROC()]

    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join('.', 'data', 'resources', 'models', self.model_id+'.model'))
        

if __name__ == '__main__':
    print('Processiamo il dataset {} con modello {}'.format(datasetID, modelID))
    luigi.run()
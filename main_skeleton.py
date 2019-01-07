'''
Questo script è un calco per implementare una pipeline Luigi.

Vanno settate, oltre alle logiche in run, il dataset_id e il model_id, lanciando tutto con

> python $filepath $NomeTask --local-scheduler

Se si vuole impostare un parametro da CLI scrivere

> python $filepath $NomeTask --$nomevariabile $valorevariabile --local-scheduler

ricordandosi di sostituire le '_' con '-' in $nomevariabile
'''

import daiquiri
import daiquiri.formatter
import logging
import luigi
import os
import pandas as pd
import sys
import yaml

from etl import collector
from preproc import report_data, proc_data
from model import set_model, train_xgb

luigi.interface.setup_interface_logging.has_run = True

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)
print(os.getcwd())

datasetID = ''
modelID = ''
sampleZ = None

daiquiri.setup(
        level=logging.INFO,
        outputs=(
            daiquiri.output.Stream(sys.stdout),
            daiquiri.output.File('./logs/logs.log', level=logging.INFO,
            formatter=daiquiri.formatter.ColorFormatter(
        fmt="\n###### %(asctime)s [%(levelname)s] %(name)s -> %(message)s ######\n")),
        )
    )
logger = daiquiri.getLogger(__name__, propagate=False)


logger.info('Parte la pipeline su dataset {} e modello {}'.format(datasetID, modelID))
if sampleZ:
    logger.info('La run è una prova con un sample n={}'.format(sampleZ))

class ETL(luigi.Task):

    dataset_id = luigi.Parameter(default=datasetID)
    path_dataset = luigi.Parameter(default=r'')

    def requires(self):
        return []

    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join('.', 'config', 'dataset', 'etl_'+self.dataset_id+'.yaml'))
        

class PREPROC(luigi.Task):
    dataset_id = luigi.Parameter(default=datasetID)
    ls_keys = luigi.ListParameter(default=[''])
    sample = luigi.IntParameter(default=None)

    
    def requires(self):
        return [ETL()]

    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join('.', 'config', 'dataset', self.dataset_id+'.yaml'))



class TRAINMODEL(luigi.Task):
    dataset_id = luigi.Parameter(default=datasetID)
    model_id = luigi.Parameter(default=modelID)

    def requires(self):
        return [PREPROC()]

    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget(os.path.join('.', 'data', 'resources', 'models', '{}_{}.model'.format(self.dataset_id, self.model_id)))

        

if __name__ == '__main__':
    print('Processiamo il dataset {} con modello {}'.format(datasetID, modelID))
    luigi.run()
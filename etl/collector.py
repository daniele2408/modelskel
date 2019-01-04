'''
Questo script raccoglie le varie estrazioni e le mette insieme in un unico dataset in data/perimetro
Le estrazioni possono essere .csv già dumpati, in data/resources/csv, oppure venire estratti tramite python qua
'''
import daiquiri
import logging
import os
import pandas as pd
import sys
import yaml

daiquiri.setup(level=logging.INFO, outputs=(
    daiquiri.output.Stream(sys.stdout),
    ))

logger = daiquiri.getLogger(__name__, subsystem="Module ETL")

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(os.path.join(dir_path, '..'))



def collect_sources(filename):
    '''
    Funzione che importa ed estrae dati, come output ha un dataset unico frutto di join delle estrazioni
    '''

    ### IMPORT ###


    ### ESTRAZIONI ###

    pass

    # return dataset

def save_perimetro(dataset_id, save=True, dataset=None, path=None):
    assert save or path, 'Se non si salva fornire un path'
    assert bool(save) == bool(dataset), 'Se si vuole salvare fornire un dataset'
    if save:
        datasetPath = os.path.join('.', 'data', 'perimetro', 'etl_'+dataset_id+'.csv')
        dataset.to_csv(datasetPath, sep=';', index=False)
    else:
        datasetPath = path

    logger.info('Salvo il risultato dell\'ETL in {}, con dataset_id {}'.format(datasetPath, dataset_id))
    yaml.dump({'FILEPATH':datasetPath}, open(os.path.join('.', 'config', 'dataset', 'etl_'+dataset_id+'.yaml'), 'w'))


if __name__ == '__main__':
    
    DATAPATH = ''
    
    save_perimetro('possessi_reco', save=False, path=DATAPATH)
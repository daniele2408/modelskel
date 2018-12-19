'''
Questo script raccoglie le varie estrazioni e le mette insieme in un unico dataset in data/perimetro
Le estrazioni possono essere .csv già dumpati, in data/resources/csv, oppure venire estratti tramite python qua
'''
import os
import pandas as pd
import yaml

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def collect_sources(filename):
    '''
    Funzione che importa ed estrae dati, come output ha un dataset unico frutto di join delle estrazioni
    '''

    ### IMPORT ###


    ### ESTRAZIONI ###

    return dataset

def save_perimetro(dataset, dataset_id):
    datasetPath = os.path.join('..', 'data', 'perimetro', 'etl_'+dataset_id+'.csv')
    dataset.to_csv(datasetPath, sep=';', index=False)
    yaml.dump({'FILEPATH':datasetPath}, open(os.path.join('..', 'config', 'dataset', 'etl_'+dataset_id+'.yaml'), 'w'))


if __name__ == '__main__':
    
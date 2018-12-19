'''
In questo script recuperiamo i dati con una certa frequenza (oppure una tantum) per poterli predire, ricalca il processo di etl
'''
import os
from datetime import datetime


def fetch_data():
    pass


def save_fetch_data(dataset, dataset_id, save=False):
    '''
    Salva un dataset in data/coll con dataset_id + timestamp
    '''

    if save:
        fetch_id = datetime.strptime(datetime.now(), format='%Y%m%d%M')
        dataset.to_csv(os.path.join('..', 'data', 'coll', '{}_fetch_{}.csv'.format(dataset_id, fetch_id)), sep=';', index=False)

    return dataset, fetch_id
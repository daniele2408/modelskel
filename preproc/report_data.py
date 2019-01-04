import daiquiri
import helpers.utils.UtilsDataManip as utils
import logging
import os
import pandas as pd
import sys
import yaml

daiquiri.setup(level=logging.INFO, outputs=(
    daiquiri.output.Stream(sys.stdout),
    ))

logger = daiquiri.getLogger(__name__, subsystem="Module PREPROC")

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(os.path.join(dir_path, '..'))

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 10)

def report_data(dataset_id, supposed_pk, qmin=0.01, sample=None):
    '''
    Funzione che printa info su un dataset estratto

    Args:
        - df, (DataFrame): dataframe
        - supposed_pk, (list): lista dei valori che vorremmo essere unici
        - checkcorr, (list): colonne da correlare

    '''

    cfg = yaml.load(open(os.path.join('.', 'config', 'dataset', 'etl_'+dataset_id+'.yaml'), 'r'))
    datapath= cfg['FILEPATH'].replace('\\', '/')

    logger.info('Stampo un report per il dataset_id {}, file locato in {}'.format(dataset_id, datapath))

    if sample is None:
        df = pd.read_csv(datapath, sep=';',
                        decimal=".",keep_default_na = False, na_values = [""]
                            )
    else:
        df = pd.read_csv(datapath, sep=';',
                        decimal=".",keep_default_na = False, na_values = [""],
                        nrows=sample
                            )

    # sintesi
    descrdf = utils.check_col_types(df)
    logger.info(descrdf)

    diz_tipi = {n:g['colonna'].tolist() for n,g in descrdf.groupby('tipo')}
    for k,v in diz_tipi.items():
        logger.info(k,v,end='\n\n')

    # controlla valori unici
    try:
        assert logger.info(utils.are_uniques(df, supposed_pk)), "I campi [{}] non sono unici".format(', '.join(supposed_pk))
    except AssertionError:
        pass

    # # correlazioni
    # if checkcorr:
    #     print(df[checkcorr].corr())

    return

if __name__ == '__main__':
    report_data('possessi_reco', ['CODICE_FISCALE'])

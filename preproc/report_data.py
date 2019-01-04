import helpers.utils.UtilsDataManip as utils
import os
import pandas as pd
import yaml

os.chdir(os.path.dirname(os.path.abspath(__file__)))
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

    if sample is None:
        df = pd.read_csv(cfg['FILEPATH'].replace('\\', '/'), sep=';',
                        decimal=".",keep_default_na = False, na_values = [""]
                            )
    else:
        df = pd.read_csv(cfg['FILEPATH'].replace('\\', '/'), sep=';',
                        decimal=".",keep_default_na = False, na_values = [""],
                        nrows=sample
                            )

    # sintesi
    descrdf = utils.check_col_types(df)
    print(descrdf)

    diz_tipi = {n:g['colonna'].tolist() for n,g in descrdf.groupby('tipo')}
    for k,v in diz_tipi.items():
        print(k,v,end='\n\n')

    # controlla valori unici
    try:
        assert print(utils.are_uniques(df, supposed_pk)), "I campi [{}] non sono unici".format(', '.join(supposed_pk))
    except AssertionError:
        pass

    # # correlazioni
    # if checkcorr:
    #     print(df[checkcorr].corr())

    return

if __name__ == '__main__':
    report_data('possessi_reco', ['CODICE_FISCALE'])

import helpers.utils.UtilsDataManip as utils
import os
import pandas as pd
import yaml

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def report_data(dataset_id, supposed_pk, qmin=0.01):
    '''
    Funzione che printa info su un dataset estratto

    Args:
        - df, (DataFrame): dataframe
        - supposed_pk, (list): lista dei valori che vorremmo essere unici
        - checkcorr, (list): colonne da correlare

    '''

    cfg = yaml.load(open(os.path.join('..', 'config', 'dataset', 'etl_'+dataset_id+'.yaml'), 'r'))

    df = pd.read_csv(cfg['FILEPATH'], sep=',')

    # sintesi
    print(utils.check_col_types(df))

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
    report_data('crmTrieste', ['CONT_ID', 'COD_FISCALE_P_IVA'])
'''
In questo script dobbiamo assicurarci che il dataset raccolto in fetch_data sia in linea con il trainset
'''
import os
import pandas as pd
import sys
import helpers.utils.UtilsDataManip as util
import yaml

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.append("..")
print(os.getcwd())
from preproc.proc_data import apply_depack, apply_decode

def check_type_diff(df1, df2, cols):
    sametype = set()
    for c in cols:
        if df1[c].dtype != df2[c].dtype:
            print('Per la colonna {}, il primo dataset ha tipo {} mentre il secondo ha tipo {}'.format(c, df1[c].dtype, df2[c].dtype))
        else:
            sametype.add(c)

    if len(cols) == len(sametype):
        print('Tutte le colonne sono dello stesso tipo')

    return sametype

def check_depack_diff(dp1, dp2):
    cat1 = dp1['cat']
    cat2 = dp2['cat']

    cat_shared = set(cat1).intersection(set(cat2))

    for c in cat_shared:
        v1 = set(cat1[c].keys())
        v2 = set(cat2[c].keys())

        missing_train = v2.difference(v1)

        print('Per la colonna {}, al trainset mancano {} valori: {}'.format(
            c,
            len(missing_train),
            '\n'.join(
                [':'.join([val,str(num)]) for val,num in cat2[c].items() if val in missing_train]
                    )
            ))

    print('Controllo depack completato')

    return None

def check_align(dataset_id, df_fetch):
    '''
    Questa funzione:
        - Controlla le colonne in comune e quelle non in comune
        - Di quelle in comune controlla quali sono dello stesso tipo e quali no
        - Di quelle con stesso type categoriche controlla il depack
    '''

    metadata = yaml.load(open(os.path.join('..','config','dataset',dataset_id+'.yaml'), 'r'))
    predittori = metadata['FEAT_INFO']['FEATURE_CATEGORICHE'] + metadata['FEAT_INFO']['FEATURE_CONTINUE'] + metadata['FEAT_INFO']['TARGET']
    diz_encode = metadata['DIZ_ENCODE']
    depack_old = metadata['DEPACK']

    df = pd.read_csv(os.path.join('..', 'data', 'proc', dataset_id+'.csv'), sep=';')
    # devo decodare le cat!
    apply_decode(df, diz_encode)

    apply_decode(df_fetch, diz_encode)

    col_set, col_set_new = set(predittori), set(df_fetch.columns)
    col_shared = col_set.intersection(col_set_new)
    col_only_old = col_set.difference(col_set_new)
    col_only_new = col_set_new.difference(col_set)

    print('''
I due dataset hanno {} in comune.
Il vecchio dataset ha {} colonne in più: {}\n
il nuovo dataset ha {} colonne in più: {}
        '''.format(len(col_shared), len(col_only_old), '\n'.join(col_only_old), len(col_only_new), '\n'.join(col_only_new)))

    print('Controllo il dtype delle colonne')
    sametype = check_type_diff(df, df_fetch, col_shared)

    depack_new = apply_depack(
        df_fetch,
        cat=[c for c in sametype if c in metadata['FEAT_INFO']['FEATURE_CATEGORICHE']],
        cont=[c for c in sametype if c in metadata['FEAT_INFO']['FEATURE_CONTINUE']]
        )

    print('Controllo il depack delle colonne')
    check_depack_diff(depack_old, depack_new)

    return None


def apply_align(dataset):

    return dataset


def main_align(dataset_id, df_fetch):
    check_align(dataset_id, df_fetch)
    dataset = apply_align(df_fetch)

    check_align(dataset_id, dataset)


if __name__ == '__main__':

    trial = True
    if trial:
        df_trial = pd.read_csv(r'C:\Users\LEI00020\gitfolder\modelskel\data\proc\crmTrieste.csv', sep=';')
        df_trial = df_trial.sample(round(df_trial.shape[0]*0.45))
        check_align('crmTrieste', df_trial)

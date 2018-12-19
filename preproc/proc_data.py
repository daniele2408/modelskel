'''
Questo script processa i dati e deve generare lo yaml con info sulle feature, il depack, il label encoding
'''
import numpy as np
import os
import pandas as pd
import yaml

from collections import defaultdict

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def encode_cat(df, depack, qmin=0.01, verbose=True):

    cats = depack['cat']

    diz_encode = defaultdict(dict)

    for c in cats:
        print({i:(val[0],val[1]) for i,val in enumerate(cats[c].items())})
        diz_encode[c] = {val[0]:i for i,val in enumerate(cats[c].items()) if val[1] >= qmin}

        # vc = df[c].value_counts().sort_values(ascending=False)
        # vc_perc = vc / vc.sum()

        # diz_vc = [val for val,num,perc in zip(vc.index, vc, vc_perc) if perc >= qmin]

        # label_encode = {val:i for i,val in enumerate(diz_vc)}
        # escluso_tot = vc_perc[vc_perc<qmin].sum()
        escluso_tot = np.sum([val for val in cats[c].values() if val < qmin])

        if escluso_tot > 0.1:
            raise UserWarning("La somma dei valori sotto {} è {}".format(qmin, round(escluso_tot,2)))

        esclusi = [k for k,val in cats[c].items() if val < qmin]
        escluso_val = [val for k,val in cats[c].items() if val < qmin]
        if verbose and len(esclusi)>0:
            print('''
Per la colonna {} abbiamo escluso il {}% del dataset
perché composto da valori che singolarmente
non toccano un valore di {}%:\n{}
            '''.format(c, round(escluso_tot,2)*100, qmin*100, '\n'.join([':'.join([b,str(round(a,2))]) for a,b in zip(escluso_val, esclusi)])))


    return diz_encode

def apply_encode(df, diz_encode):    
    for c in diz_encode.keys():
        df[c] = df[c].apply(lambda x: diz_encode[c].get(x, 'others_{}'.format(c)))

def apply_decode(df, diz_encode):
    diz_decode = dict()
    for k in diz_encode.keys():
        diz_decode[k] = {v:k for k,v in diz_encode[k].items()}

    for c in diz_decode.keys():
        df[c] = df[c].apply(lambda x: diz_decode[c].get(x, 'others_{}'.format(c)))

def apply_depack(df, cat, cont):
    depack = {'cont':{}, 'cat':{}}
    for c in df.columns:
        if c in cat:
            vc = df[c].value_counts(dropna=False, normalize=True).sort_values(ascending=False)
            depack['cat'][c] = {val:round(no, 4) for val,no in zip(vc.index, vc)}
        elif c in cont:
            depack['cont'][c] = [int(float(df[c].min())), int(float(df[c].max()))]
        else:
            continue

    return depack

def cast_col(df, tofloat, toobj):
    for c in df.columns:
        if c in tofloat:
            df[c] = df[c].astype(float)
        elif c in toobj:
            df[c].replace(np.NaN, '.99999', inplace=True)
            df[c] = df[c].astype(str)
            df[c].replace('.99999', np.NaN, inplace=True)

def start_proc(dataset, keep=None, drop=None, qmin=0.01):

    ### QUALI FEATURE TENERE
    keep = dataset.columns if keep is None and drop is None else keep
    drop = [] if drop is None else drop
    assert len(set(keep).intersection(set(drop))) == 0, 'Non può esserci una feature in drop e in keep'
    dataset = dataset[[c for c in dataset.columns if c in keep and c not in drop]]

    ### ELABORAZIONI VARIE
    print('...Mox elaborandum est...')
    dataset['SESSO'] = np.where(dataset['SESSO']=='M', 1, 0)
    print('...Data elaborata sunt...')

    return dataset


def end_proc(dataset, qmin):
    ### IDENTIFICAZIONE LABEL
    preds_cont = ['ETA']
    preds_cat = ['PROVINCIA']
    chiavi = ['CONT_ID', 'COD_FISCALE_P_IVA']
    target = ['SESSO']

    feature_info = {
        'FEATURE_CONTINUE':preds_cont,
        'FEATURE_CATEGORICHE':preds_cat,
        'KEYS':chiavi,
        'TARGET':target,
    }

    dataset = dataset[chiavi + preds_cat + preds_cont + target].copy()

    ### CAST
    cast_col(dataset, preds_cont, preds_cat)

    ### DEPACK
    depack = apply_depack(dataset, preds_cat, preds_cont)

    ### LABEL ENCODING
    diz_encode = encode_cat(dataset, depack, qmin=qmin)
    apply_encode(dataset, diz_encode)


    return dataset, feature_info, depack, diz_encode

def save_results(dataset_id, dataset, feats, depack, diz_encode):
    datasetPath = os.path.join('..', 'data', 'proc', dataset_id+'.csv')
    dataset.to_csv(datasetPath, sep=';', index=False)

    diz_info = {
        'FILEPATH':datasetPath,
        'FEAT_INFO':feats,
        'DEPACK':depack,
        'DIZ_ENCODE':diz_encode,
    }

    yaml.dump(diz_info, open(os.path.join('..', 'config', 'dataset', dataset_id+'.yaml'), 'w'), default_flow_style=False, allow_unicode=True)

def load_perimetro(dataset_id):
    cfg = yaml.load(open(os.path.join('..', 'config', 'dataset', 'etl_'+dataset_id+'.yaml'), 'r'))

    return pd.read_csv(cfg['FILEPATH'], sep=',')


def main_proc(dataset_id):

    ### CARICO
    dataset = load_perimetro(dataset_id)

    ### PROCCO
    dataset = start_proc(dataset)
    data, feature_info, depack, diz_encode = end_proc(dataset, qmin=0.01)

    ### SCARICO
    save_results(dataset_id, data, feature_info, depack, diz_encode)

if __name__ == '__main__':

    trial = False
    if trial:
        df = pd.DataFrame({
            'artemisio':['a']*20+['b']*1+['c']*80+['d']*2,
            'berta':['x']*20+['y']*1+['z']*80+['w']*2,
            'conte':[20]*20+[10]*1+[33]*82,
            'key':['key']*103,
            'zero':[1]*103
            })
        main_proc('dataprova')

    main_proc('crmTrieste')


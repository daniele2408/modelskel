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

    eccezioni = ['PROVINCIA']  # non vogliamo un warning per questi predittori
    for c in cats:
        print(c)
        print({i+1:(val[0],val[1]) for i,val in enumerate(cats[c].items())})
        diz_encode[c] = {val[0]:i+1 for i,val in enumerate(cats[c].items()) if val[1] >= qmin}

        escluso_tot = np.sum([val for val in cats[c].values() if val < qmin])
        if escluso_tot > 0.1 and c not in eccezioni:
            raise UserWarning("La somma dei valori sotto {} è {}".format(qmin, round(escluso_tot,2)))

        esclusi = [k for k,val in cats[c].items() if val < qmin]
        escluso_val = [val for k,val in cats[c].items() if val < qmin]
        if verbose and len(esclusi)>0:
            print('''
Per la colonna {} abbiamo escluso il {}% del dataset
perché composto da valori che singolarmente non toccano un valore di {}%:\n{}
            '''.format(c, round(escluso_tot,2)*100, qmin*100, '\n'.join([':'.join([str(b),str(round(a,2))]) for a,b in zip(escluso_val, esclusi)])))

        diz_encode[c]['others_{}'.format(c)] = 0

    return diz_encode

def apply_encode(df, diz_encode):    
    for c in diz_encode.keys():
        df[c] = df[c].apply(lambda x: diz_encode[c].get(x, 0))  # se non lo trova vuol dire che è stato escluso, quindi è 0 come specificato

def apply_decode(df, diz_encode):
    diz_decode = dict()
    for k in diz_encode.keys():
        diz_decode[k] = {v:k for k,v in diz_encode[k].items()}

    for c in diz_decode.keys():
        df[c] = df[c].apply(lambda x: diz_decode[c].get(x, 0))

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

    print('...Data elaborata sunt...')

    return dataset


def end_proc(dataset, qmin, nafiller='-999'):

    ### IDENTIFICAZIONE LABEL
    preds_cont = [
        'REDDITO_ME',
        'AN_SC_000070',
        'CN_DF_DS_000020',
        'zone_risk1',
        'CL_Redd',
        'zone_risk2',
        'AN_SC_000010',
        'AN_CN_AT_000140',
        'AN_000280',
        'RISK_SB',
        'GEODELPHI_PLUS',
        'anzianita_cliente',
        'flagEmail',
        'flagTelefono',
        'eta_anagrafica'
        ]
    preds_cat = [
        'PROVINCIA',
        'Imm_Fascia',
        'MTYPE',
        'ATTIVITA_NEW'
        ]
    chiavi = ['CODICE_FISCALE']
    target = ['POSSESSO_VITA_INVESTIMENTO_y']

    feature_info = {
        'FEATURE_CONTINUE':preds_cont,
        'FEATURE_CATEGORICHE':preds_cat,
        'KEYS':chiavi,
        'TARGET':target,
    }

    dataset = dataset[chiavi + preds_cat + preds_cont + target].copy()

    ### CAST
    cast_col(dataset, preds_cont, preds_cat)

    ### REPLACENA per le categoriche, nan è troppo strano
    for c in preds_cat:
        print(c)
        print(dataset[c].head())
        assert (dataset[c]==nafiller).any() == False, "Il nafiller è già presente come valore nel dataset, sostituirlo"
        dataset[c].fillna(nafiller, inplace=True)

    ### DEPACK
    depack = apply_depack(dataset, preds_cat, preds_cont)

    ### LABEL ENCODING
    diz_encode = encode_cat(dataset, depack, qmin=qmin)
    apply_encode(dataset, diz_encode)


    return dataset, feature_info, depack, diz_encode

def save_results(dataset_id, dataset, feats, depack, diz_encode):
    datasetPath = os.path.join('.', 'data', 'proc', dataset_id+'.csv')
    dataset.to_csv(datasetPath, sep=';', index=False)

    diz_info = {
        'FILEPATH':datasetPath,
        'FEAT_INFO':feats,
        'DEPACK':depack,
        'DIZ_ENCODE':diz_encode,
    }

    yaml.dump(diz_info, open(os.path.join('.', 'config', 'dataset', dataset_id+'.yaml'), 'w'), default_flow_style=False, allow_unicode=True)

def load_perimetro(dataset_id, sample=None):
    cfg = yaml.load(open(os.path.join('.', 'config', 'dataset', 'etl_'+dataset_id+'.yaml'), 'r'))

    if sample:
        return pd.read_csv(
            cfg['FILEPATH'].replace('\\', '/'), sep=';',
            nrows=sample,
            decimal=".",keep_default_na = False, na_values = [""]
            )
    else:
        return pd.read_csv(
            cfg['FILEPATH'].replace('\\', '/'), sep=';',
            nrows=sample,
            decimal=".",keep_default_na = False, na_values = [""]
            )


def main_proc(dataset_id, skip=False, sample=None):

    ### CARICO
    dataset = load_perimetro(dataset_id, sample)

    if not skip:
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

    main_proc('possessi_reco', skip=True)


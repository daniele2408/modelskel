import os
import pandas as pd
import yaml

from sklearn.externals import joblib

def predict_on_coll(dataset, fetch_id, model_id, dataset_id, save=True):
    
    cfg = yaml.load(open(os.path.join('..', 'config', 'model', 'parametri_'+model_id+'.yaml'),'r'))
    cfgData = yaml.load(open(os.path.join('..', 'config', 'dataset', 'parametri_'+cfg['DATASET_ID']+'.yaml'),'r'))
    model = joblib.load(os.path.join('..', 'data', 'resources', 'models', model_id+'.model'))

    predittori = cfgData['FEAT_INFO']['FEATURE_CATEGORICHE'] + cfgData['FEAT_INFO']['FEATURE_CONTINUE']
    target = cfgData['FEAT_INFO']['TARGET'][0]

    dataset['score_{}'.format(target)] = model.pred_proba(dataset[predittori])[:,0]

    if save:
        dataset.to_csv(os.path.join('..', 'data', 'pred', '_'.join([dataset_id, model_id, fetch_id]) +'.csv'))

    return dataset

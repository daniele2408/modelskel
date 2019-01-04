import daiquiri
import helpers.viz.graph_func as gf
import logging
import numpy as np
import os
import pandas as pd
import pickle
import plotly.graph_objs as go
import plotly.offline as poff
import sys
import time
import xgboost as xgb
import yaml

from helpers.utils.helpers_func_xgb import objective, apply_hyperopt, modelfit, grid_learn, get_n_est, graph_cv
from helpers.utils.UtilsDataManip import check_col_types
from sklearn.calibration import CalibratedClassifierCV
from sklearn.externals import joblib
from sklearn.metrics import roc_auc_score, roc_curve, log_loss
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(os.path.join(dir_path, '..'))

daiquiri.setup(level=logging.INFO, outputs=(
    daiquiri.output.Stream(sys.stdout),
    ))

logger = daiquiri.getLogger(__name__, subsystem="Module MODEL")

def train_model(model_id):

    ### CARICO PARAMETRI

    logger.info('Carico i parametri')

    cfg = yaml.load(open(os.path.join('.', 'config', 'model', 'parametri_'+model_id+'.yaml'), 'r'))
    params = cfg['PARAMETRI']

    cfgData = yaml.load(open(os.path.join('.', 'config', 'dataset', cfg['DATASET_ID']+'.yaml'), 'r'))

    diz_le = cfgData['DIZ_ENCODE']
    feat_info = cfgData['FEAT_INFO']
    cats = feat_info['FEATURE_CATEGORICHE']
    cont = feat_info['FEATURE_CONTINUE']
    predittori = cats + cont
    target = feat_info['TARGET']
    chiavi = feat_info['KEYS']

    testsize = params['test_size']
    use_grid = params['use_grid']
    use_stratkfold = params['use_stratkfold']
    iperparams = params['iperparams']

    use_hyperopt = params['use_hyperopt']
    max_evals = params['max_evals']
    space = params['space']

    ### CARICO DATASET

    logger.info('Carico il dataset')

    dataset = pd.read_csv(cfgData['FILEPATH'], sep=';')


    ### MODELLO
    X,y = dataset[predittori], dataset[target[0]]
    logger.info('Distribuzione variabile target')
    logger.info(y.value_counts())

    logger.info(X.isnull().sum())

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=testsize, random_state=42, stratify=dataset[target])
    train = pd.concat([X_train, y_train], axis=1)
    test = pd.concat([X_test, y_test], axis=1)

    if use_grid:
        logger.info('use_grid = True, eseguo una CV per trovare il numero ideale di estimator')

        n_est, cvresult = get_n_est(XGBClassifier(**iperparams), dataset, predittori, target)
        # graph_cv(cvresult, ['train-auc-mean', 'train-logloss-mean'], ['test-auc-mean', 'test-logloss-mean'], os.path.join(modelPath, 'cvresults.html'))
        iperparams['n_estimators'] = n_est
        logger.info('Abbiamo selezionato {} come numero di estimator ideale'.format(n_est))

        logger.info('Griglia per learning_rate')
        grid_scores, grid_bestparams, grid_bestscore = grid_learn(XGBClassifier(**iperparams), X_train, y_train, n_est, iperparams['n_jobs'])

        logger.info('Abbiamo selezionato un learning rate pari a {}, con una log loss minimizzata a {}'.format(grid_bestparams['learning_rate'],grid_bestscore))

        logger.info(grid_scores)

    if use_hyperopt:
        logger.info('use_hyperopt = True, Automatic parameter tuning')
        params_opt, min_logloss, trials_results, trials_raw = apply_hyperopt(space, n_est, X_train, y_train, X_test, y_test, max_evals=max_evals)

        must_int = {'max_depth','min_child_weight', 'n_estimators'}
        iperparams = {k:(v[0] if k not in must_int else int(v[0])) for k,v in params_opt.items()}

    logger.info('Fitto il modello')

    alg = XGBClassifier(**iperparams)
    if use_stratkfold:
        logger.info('use_stratkfold = True, Applico una cross-validation stratificata')
        skf = StratifiedKFold(n_splits=5)
        bestmodel = modelfit(alg, train, iperparams['n_estimators'], predittori, target[0], useTrainCV=True, foldObj=skf)        
    else:
        bestmodel = modelfit(alg, train, iperparams['n_estimators'], predittori, target[0], useTrainCV=False)

    logger.info('Calibro il modello')
    X_test['score'] = bestmodel.predict_proba(X_test)[:,1].copy()
    X_test[target[0]] = y_test.copy()

    auc = roc_auc_score(y_test, X_test['score'])
    logloss = log_loss(y_test, X_test['score'])

    return bestmodel, X_test, {'auc':float(auc), 'logloss':float(logloss)}, target[0]

def save_results(model_id, dataset_id, model, data_preds, diz_results, target):
    logger.info('Salvo i risultati e i plot')
    joblib.dump(model, os.path.join('.', 'data', 'resources', 'models', '{}_{}.model'.format(dataset_id, model_id)))
    cfg = yaml.load(open(os.path.join('.', 'config', 'model', 'parametri_'+model_id+'.yaml'), 'r'))
    cfg['RISULTATI'] = diz_results
    yaml.dump(cfg, open(os.path.join('.', 'config', 'model', 'parametri_'+model_id+'.yaml'), 'w'))


    
    roc_fig = gf.roc_curve_annotated(
        data_preds,
        target,
        'score',
        ['accuracy', 'recall', 'precision', 'miss_error_0', 'miss_error_1', 'precision_0'],
        os.path.join('.', 'data', 'resources', 'plot'),
        'roc_curve_{}.html'.format(model_id),
        save=False
        )


    
    lift_fig = gf.lift_chart(
        data_preds,
        target,
        'score',
        os.path.join('.', 'data', 'resources', 'plot'),
        'lift_chart_{}.html'.format(model_id),
        save=False
    )

    grafmetr_fig = gf.grafico_metriche(
        data_preds,
        target,
        'score',
        os.path.join('.', 'data', 'resources', 'plot'),
        'grafico_metriche_{}.html'.format(model_id),
        ['accuracy', 'recall', 'precision', 'miss_error_0', 'miss_error_1', 'precision_0'],
        save=False
    )

    poff.plot(roc_fig, filename=os.path.join('roc_curve_{}.html'.format(model_id)), auto_open=False)
    poff.plot(lift_fig, filename=os.path.join('lift_chart_{}.html'.format(model_id)), auto_open=False)
    poff.plot(grafmetr_fig, filename=os.path.join('grafico_metriche_{}.html'.format(model_id)), auto_open=False)



def main_train(model_id, dataset_id):
    model, data_preds, diz_results, target = train_model(model_id)
    save_results(model_id, dataset_id, model, data_preds, diz_results, target)

if __name__ == '__main__':

    print(os.getcwd())
    assert os.path.exists(os.path.join('.', 'data', 'resources', 'plot'))
    # main_train('model_xgbprova')
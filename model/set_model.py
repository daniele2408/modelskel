import os
import yaml
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def set_model_params(dataset_id, model_id):

    params = {
        'test_size':0.2,
        'n_est_max':2,
        'max_evals':3,
        'n_jobs':4,
        'use_hyperopt':False,
        'use_grid':False,
        'iperparams': {
            'colsample_bytree':0.5868323733872024,
            'subsample':0.6975732844651784,
            'gamma':1.1060552077820258,
            'eval_metric':['auc', 'logloss'],
            'learning_rate':0.1,
            'max_depth':7,
            'min_child_weight':8,
            'reg_alpha':0.560674658894005,
            'reg_lambda':0.04109383840952241,
            'n_estimators':3,
            'n_jobs':3,
            'objective':'binary:logistic'
        },
        'space':{  # spazi per l'hyperopt
        #     'learning_rate' : hp.quniform('learning_rate', 0.025, 0.5, 0.025),
        #     'n_estimators' : hp.quniform('n_estimators', 200,500,100),
            
            'max_depth' : hp.choice('max_depth', [i for i in range(3,11)]),
            'min_child_weight' : hp.quniform('min_child_weight', 1, 20, 2),
            
            'gamma' : hp.uniform('gamma', 0, 20),
            
            'subsample': hp.uniform('subsample', 0.4, 0.8),    
            'colsample_bytree' : hp.uniform('colsample_bytree', 0.4, 0.8),
            
            'reg_alpha':hp.uniform('reg_alpha', 0, 1),
            'reg_lambda':hp.uniform('reg_lambda', 0, 1)
                }
            }

    yaml.dump({'DATASET_ID':dataset_id, 'PARAMETRI':params},open(os.path.join('..', 'config', 'model', 'parametri_'+model_id+'.yaml'),'w'))

if __name__ == '__main__':
    set_model_params('crmTrieste', 'model_xgbprova')
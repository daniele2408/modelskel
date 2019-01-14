import os
import yaml
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(os.path.join(dir_path, '..'))


def set_model_params(dataset_id, model_id):

    params = {
        'target':'',
        'test_size':0.2,
        'n_est_max':700,
        'max_evals':20,  # per hyperopt
        'n_jobs':3,
        'use_grid':False,
        'use_hyperopt':True,
        'use_stratkfold':True,
        'strat_perc':0.3,
        'iperparams': {
            'colsample_bytree':0.7904172784890469,
            'subsample':0.7593440381275615,
            'gamma':8.705364606318168,
            'eval_metric':['auc', 'logloss'],
            'learning_rate':0.1,  # 0.1
            'max_depth':7,
            'min_child_weight':6,
            'reg_alpha':0.32412538307163585,
            'reg_lambda':0.4343403342030415,
            'n_estimators':700,  # 700
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

    yaml.dump({'DATASET_ID':dataset_id, 'PARAMETRI':params},open(os.path.join('.', 'config', 'model', 'parametri_'+model_id+'.yaml'),'w'))

if __name__ == '__main__':
    set_model_params('possessi_reco', 'model_xgbprova')
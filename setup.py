import os


dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)

def create_dirs(root, ls):
    for folder in ls:
        foldername = os.path.join(root, folder)
        os.makedirs(foldername) if not os.path.exists(foldername) else None

create_dirs('.', [
    'etl', 'notebook', 'config', 'logs',
    'config/dataset', 'config/model',
    'preproc', 'model', 'collaudo',
    'data', 'data/resources', 'data/perimetro',
    'data/proc', 'data/pred', 'data/coll',
    'data/resources/csv', 'data/resources/json', 'data/resources/pickle', 'data/resources/plot', 'data/resources/models'
    ])
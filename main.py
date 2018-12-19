import os
import pandas as pd
import yaml

from preproc.report_data import report_data

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)

def main(dataset_id, model_id):

    proc()
    train()
    pred()

    return 

if __name__ == '__main__':
    cfg = yaml.load(r'')
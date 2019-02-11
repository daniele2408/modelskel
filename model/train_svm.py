#%%
from sklearn import svm
import yaml
import os
import pandas as pd
from sklearn.model_selection import train_test_split
#%%
os.chdir(r'C:\Users\LEI00020\Documents\Progetti\newreco')

dataset_id = 'possessi_reco'

cfg = yaml.load(open(os.path.join('.', 'config', 'dataset', dataset_id+'.yaml'), 'r'))
dataPath = cfg['FILEPATH']


df = pd.read_csv(dataPath, sep=';')

print(df.head())
#%%
df['POSSESSO_VITA_INVESTIMENTO_y'].value_counts(normalize=True, dropna=False)
preds = ['Imm_Fascia', 'MTYPE', 'ATTIVITA_NEW']
target = ['POSSESSO_VITA_INVESTIMENTO_y']

# X = df[preds]
# y = df[target]

X = pd.concat([df[df[target]==0].sample(1000), df[df[target]==1].sample(1000)], axis=0)
y = pd.concat([df[df[target]==1].sample(200), df[df[target]==1].sample(200)], axis=0)
#%%
X.shape
#%%

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)


#%%
clf = svm.SVC(gamma='scale', probability=True)

#%%
clf.fit(X_train, y_train)

#%%

predict = clf.predict(X_test)

#%%
predict
Scheletro per ospitare diversi modelli, seguendo la classica linea ETL->PREPROC->MODEL->COLLAUDO, automatizzando certe esigenze, centralizzando i dati e le configurazioni

### ISSUE ETL
Voglio sapere da dove ho preso cosa quando, magari si può creare un file centrale dove c'è scritto il nome del file .sql .sas .whatever, il server da dove si è preso, una descrizione della logica dietro al perimetro

### ISSUE PREPROC
Voglio poter visualizzare un perimetro appena estratto, accorgermi di valori duplicati, accorgermi di variabili mancanti, vedere i tipi delle colonne  
Una volta ottenute le info necessarie, lavoro il dataset e mi serve un modo per tener traccia delle colonne cat, cont, id e target, tener traccia del label encoding delle cat, prevedere un modo per mettere in un bin '_altro' i valori delle cat sotto il 0.01 percentile

### ISSUE MODEL
Voglio settare il parameter tuning, il train e la generazione del modello e dei plot, avendo come riferimento un dataset, e applicare modelli su dataset in maniera modulare

### ISSUE COLLAUDO
Devo poter allineare dataset collaudo e dataset train su stesse colonne, stessi type, stessi valori, i valori mancanti li ficchiamo in altro e prevedere che quando manca >x% di un valore allora spariamo un errore che si deve rifare il modello  
Deve ovviamente generare plot, report e le predizioni

### ISSUE DATA
I dati devono essere tutti nello stesso folder, divisi per provenienza e per linea di modello+dataset a cui appartengono, magari le pred con il timestamp


In config mettiamo un cfg.yaml che tiene i dati generali, mentre in due cartelle dataset e model teniamo gli yaml dei singolo dataset e model, dove hanno il nome del dataset_id e del model_id, qua dentro mettiamo tutti i riferimenti per recuperare dataset/model e metadati

## HOW TO
Hai appena pullato. Setta il rootDir nel config/cfg.yaml

* ETL + PREPROC
    * Definire in etl/collector i dataset da estrarre, oppure se prenderne già presenti, in ogni caso generare uno yaml in config/dataset
    * Lanciare preproc/report_data per avere un'idea del dataset
    * Lanciare preproc/proc_data decidendo come lavorare i dati e SE lavorarli
* TRAIN MODEL
    * Settare i parametri di testing in model/set_model.py (MI RACCOMANDO IL TARGET) e lanciarlo (settare nuovo model_id)

Tutto questo può essere lanciato tramite main_skeleton.py, che presenta una struttura di pipeline Luigi già pronta, dove inserire model_id, dataset_id e implementare le logiche prescelte.
Ricordarsi di impostare dataset_id, model_id (eventualmente un n sample) e lanciare
> python $filepath $NomeTask --local-scheduler

## TO DO
Voglio poter scegliere il target di un dataset senza dover ogni volta generarmi un dataset diverso se l'unica cosa a cambiare è il target
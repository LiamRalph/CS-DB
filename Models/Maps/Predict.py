import sys
sys.path.insert(0, './Maps')
import pandas as pd
from joblib import dump, load
import psycopg2
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from datetime import datetime
import random
import statistics
import MapQueries
import csv




def predict(XGB, mapid):
    
    dfList = MapQueries.getData(mapid)
    if isinstance(dfList, int):
        return -1
    df = pd.DataFrame(dfList)
    #Tick = df.tick
    df.drop(columns=['winner', 'mapid'], inplace=True)
    predictions = XGB.predict_proba(df)

    predictions = [value[1] for value in predictions]
    df['prediction'] = predictions
    #df['Tick'] = Tick
    #pd.DataFrame.to_csv(df, './data/Prediction.csv', index=False)
    return df

if __name__ == "__main__":
    XGB = load('MapModelXGB') 
    while True:
        mapid = input("Mapid: ") 
        predict(XGB, mapid)
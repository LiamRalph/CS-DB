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




def predict(XGB, mapid, addDB):
    #print(XGB.get_booster().feature_names)
    dfList = MapQueries.getData(mapid)
    if isinstance(dfList, int):
        print(mapid)
        return -1
    df = pd.DataFrame(dfList)
    
    winner = df.winner
    df.drop(columns=['winner', 'mapid'], inplace=True)
    #print(df.columns)
    predictions = XGB.predict_proba(df)
    df['winner'] = winner
    predictions = [value[0] for value in predictions]
    df['prediction'] = predictions
    if addDB == 0:
        pd.DataFrame.to_csv(df, './data/Prediction.csv', index=False)
    else:
        Rounds = df.sort_values(by=['roundno', 'tick'], ascending=False)
        prevProb = df.iloc[0]['winner']
        preds = []
        for k, RoundNo in Rounds.iterrows():
            probchangeCT = prevProb-RoundNo['prediction']
            preds.append((mapid, RoundNo['roundno'], RoundNo['tick'], RoundNo['prediction'], probchangeCT))
            prevProb = RoundNo['prediction']
        MapQueries.addPred(preds)
    return df

if __name__ == "__main__":
    XGB = load('MapModelXGB') 
    while True:
        mapid = input("Mapid: ") 
        predict(XGB, mapid, 0)
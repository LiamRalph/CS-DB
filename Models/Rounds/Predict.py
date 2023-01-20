
import sys
sys.path.insert(0, './Rounds')

import pandas as pd
from joblib import dump, load
import psycopg2
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from datetime import datetime
import random
import statistics
import RoundQueries
import csv





def predict(XGB, mapid, roundNo, addDB):
    allMaps = RoundQueries.getMapNames()
    roundstates = RoundQueries.getData(mapid, roundNo)
    dfList = []
    if isinstance(dfList, int):
        return -1
    for roundstate in roundstates:
        for mapname in allMaps:
            if mapname == roundstate["mapname"]:
                roundstate[mapname] = 1
            else:
                roundstate[mapname] = 0
        del(roundstate["mapname"])
        dfList.append(roundstate)
    df = pd.DataFrame(dfList)
    winner = df.winner
    df.drop(columns=['winner'], inplace=True)
    predictions = XGB.predict_proba(df)
    df['winner'] = winner
    predictions = [value[1] for value in predictions]
    df['prediction'] = predictions
    if addDB == 0:
        pd.DataFrame.to_csv(df, './data/Prediction.csv', index=False)
    else:
        preds = []
        RoundData = df.sort_values(by=['tick'], ascending=False).drop_duplicates(subset=['tick'])
        prevProb = RoundData.iloc[0]['winner']
        for k, tick in RoundData.iterrows():
            probchangeCT = prevProb-tick['prediction']
            preds.append((mapid, roundNo, tick['tick'], tick['prediction'], probchangeCT))
            prevProb = tick['prediction']
        RoundQueries.addPred(preds)
    return df
if __name__ == "__main__":
    XGB = load('RoundModelXGB') 
    while True:
        mapid = input("Mapid: ") 
        roundNo = input("Round: ") 
        predict(XGB, mapid, roundNo, 0)
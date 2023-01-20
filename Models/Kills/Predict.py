import sys
sys.path.insert(0, './Kills')
import pandas as pd
from joblib import dump, load
import psycopg2
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from datetime import datetime
import random
import statistics
import KillQueries
import csv




def predict(XGB, mapid, addDB):
    dfList = []
    allMaps = KillQueries.getMapNames()
    allWeps = KillQueries.getWeaponNames()
    kills = KillQueries.getData(mapid)
    if isinstance(kills, int):
        return -1

    for kill in kills:
        for mapname in allMaps:
            if mapname == kill["mapname"]:
                kill[mapname] = 1
            else:
                kill[mapname] = 0
        for weapon in allWeps:
            if weapon == kill["ctweapon"]:
                kill['ctwep_'+weapon] = 1
            else:
                kill['ctwep_'+weapon] = 0
            if weapon == kill["tweapon"]:
                kill['twep_'+weapon] = 1
            else:
                kill['twep_'+weapon] = 0
        
        del kill["ctweapon"], kill["tweapon"], kill["mapname"]
        #print(kill)
        dfList.append(kill)




    df = pd.DataFrame(dfList)
    winner = df.winner
    kill = df.kill
    death = df.death
    roundNo = df.roundno
    tick = df.tick
    df.drop(columns=['winner','mapid', 'kill', 'death', 'roundno', 'tick'], inplace=True)
    predictions = XGB.predict_proba(df)
    predictions = [value[1] for value in predictions]
    df['prediction'] = predictions
    df['winner'] = winner
    df['roundno'] = roundNo
    df['tick'] = tick
    df['kill'] = kill
    df['death'] = death
    if addDB == 0:
        pd.DataFrame.to_csv(df, './data/Prediction.csv', index=False)
    else:
        Kills = df.sort_values(by=['roundno', 'tick'], ascending=False)
        preds = []
        for k, Kill in Kills.iterrows():
            if Kill['winner'] == 1:
                preds.append((mapid, Kill['roundno'], Kill['tick'], Kill['kill'], Kill['death'], Kill['prediction']))
            else:
                preds.append((mapid, Kill['roundno'], Kill['tick'], Kill['kill'], Kill['death'], 1-Kill['prediction']))
        KillQueries.addPred(preds)
    return df 
if __name__ == "__main__":
    XGB = load('KillModelXGB') 
    while True:
        mapid = input("Mapid: ") 
        predict(XGB, mapid, 0)
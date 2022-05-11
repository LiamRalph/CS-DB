from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
import pandas as pd
from joblib import dump, load
import numpy as np
import matplotlib.pyplot as plt
from sklearn.inspection import permutation_importance
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from scipy.special import softmax
from OneHotKills import OHE
import psycopg2
def predictKill(KillValues, gbr):

    IP = '192.168.1.3'
    
    mapid = KillValues['mapid']
    round = int(KillValues['round'])
    tick = int(KillValues['tick'])
    death = int(KillValues['death'])
    killer = int(KillValues['kill'])
    
    conn = psycopg2.connect("dbname=CSGO user=postgres password=1545" + " host='" + IP + "'")
    cur = conn.cursor()
    cur.execute("""
        SELECT CASE WHEN ((%s = R.winner and R.winnerside = 'T') or (%s = R.loser and R.winnerside = 'CT')) THEN 1 ELSE 0 END from rounds R
        inner join maps M on M.mapid = R.mapid
        where R.round = %s and M.mapid = %s
        

        """, (KillValues['teamkill'], KillValues['teamkill'], round, mapid))
    CT = cur.fetchone()
    if CT == None:
        print(KillValues['teamkill'], KillValues['teamkill'], round, mapid)
        return(-1)
    CT = CT[0]
    if CT == 1:
        KillValues = {"CTAlive":int(KillValues["teammembersalive"]), "TAlive":int(KillValues["opponentsalive"]), "distance":KillValues["distance"], "ctweapon":KillValues["weapon"], "CThp":int(KillValues["killerhealth"]), "CTarmor":int(KillValues["killerarmor"]), "CThelmet":KillValues["killerhelmet"], "CTflashduration":float(KillValues["killerflashduration"]), "CTxvel":float(KillValues["killerxvel"]), "CTyvel":float(KillValues["killeryvel"]), "CTzvel":float(KillValues["killerzvel"]), "tweapon":KillValues["victimweapon"], "Thp":int(KillValues["victimhealth"]), "Tarmor":int(KillValues["victimarmor"]), "Thelmet":KillValues["victimhelmet"], "Tflashduration":float(KillValues["victimflashduration"]), "Txvel":float(KillValues["victimxvel"]), "Tyvel":float(KillValues["victimyvel"]), "Tzvel":float(KillValues["victimzvel"])}
    else:
        KillValues = {"TAlive":int(KillValues["teammembersalive"]), "CTAlive":int(KillValues["opponentsalive"]), "distance":KillValues["distance"], "tweapon":KillValues["weapon"], "Thp":int(KillValues["killerhealth"]), "Tarmor":int(KillValues["killerarmor"]), "Thelmet":KillValues["killerhelmet"], "Tflashduration":float(KillValues["killerflashduration"]), "Txvel":float(KillValues["killerxvel"]), "Tyvel":float(KillValues["killeryvel"]), "Tzvel":float(KillValues["killerzvel"]), "ctweapon":KillValues["victimweapon"], "CThp":int(KillValues["victimhealth"]), "CTarmor":int(KillValues["victimarmor"]), "CThelmet":KillValues["victimhelmet"], "CTflashduration":float(KillValues["victimflashduration"]), "CTxvel":float(KillValues["victimxvel"]), "CTyvel":float(KillValues["victimyvel"]), "CTzvel":float(KillValues["victimzvel"])}
    Kill, err = OHE(pd.DataFrame.from_dict([KillValues]))
    
    if err == 1 :
        return -1
    
    Kill['CTvel'] = np.sqrt((Kill['CTxvel']**2)+(Kill['CTyvel']**2))
    Kill['Tvel'] = np.sqrt((Kill['Txvel']**2)+(Kill['Tyvel']**2))
    Kill.drop(columns=['CTxvel', 'CTyvel', 'Txvel', 'Tyvel'], inplace=True)
    Kill['CTflashduration'] = np.where((Kill['CTflashduration'] > 0), 1, 0)
    Kill['Tflashduration'] = np.where((Kill['Tflashduration'] > 0), 1, 0)
    cur.execute("""
    SELECT damage from roundstates
    where mapid = %s and round = %s and victim = %s and tick < %s and tick > %s
    """, (mapid, round, death, tick, tick-256)
    )
    dmg = cur.fetchall()
    dmg = [i[0] for i in dmg]
    victDmg = sum(dmg)

    cur.execute("""
    SELECT damage from roundstates
    where mapid = %s and round = %s and victim = %s and tick < %s and tick > %s
    """, (mapid, round, killer, tick, tick-256)
    )
    dmgKill = cur.fetchall()
    dmgKill  = [i[0] for i in dmgKill ]
    killDmg = sum(dmgKill )

    if CT == 1:
        Kill['CThp'] += killDmg
        Kill['Thp'] += victDmg
    else:
        Kill['Thp'] += killDmg
        Kill['CThp'] += victDmg

    
    
    
    try:
        predictionKill = gbr.predict_proba(Kill)
    except ValueError as e:
        print(e)
        print(Kill)

    if CT == 1:
        return float(predictionKill[0][1])
    else:
        return float(predictionKill[0][0])
    


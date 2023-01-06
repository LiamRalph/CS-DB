from joblib import load

import pandas as pd

from Maps import MapQueries as MapQueries
from Rounds import RoundQueries as RoundQueries
#from Kills import KillQueries as KillQueries


from Maps import Predict as MapPredictor
from Rounds import Predict as RoundPredictor
#from Kills import Predict as KillsPredictor


import connectDB
conn = connectDB.database_credentials()
def getMapsParsed():
    cur = conn.cursor()
    cur.execute("""
        select Ma.mapid  from matches m
        inner join maps Ma on Ma.matchid = m.matchid 
        inner join rounds R on R.mapid = Ma.mapid and R.round = 1
        inner join roundstates RS on RS.mapid = Ma.mapid and RS.round = 1 and RS.tick = 0
        inner join kills K on K.mapid = Ma.mapid and K.round = 1 and K.teammembersalive = 5 and K.opponentsalive = 5
        inner join kill_prob KP on KP.mapid = Ma.mapid and KP.round = R.round
		--inner join map_prob MP on MP.mapid = Ma.mapid and MP.round = R.round
		inner join rs_prob RP on RP.mapid = Ma.mapid and RP.round = R.round and RP.tick = K.tick
        where Ma.winnerrounds > 15
        order by m.matchid asc
        """)
    return cur.fetchall()
def getMaps():
    cur = conn.cursor()
    cur.execute("""
        select Ma.mapid, ma.winnerrounds+ma.loserrounds  from maps Ma
        inner join rounds R on R.mapid = Ma.mapid and R.round = 1
        inner join kills K on K.mapid = Ma.mapid and K.round = 1 and K.teammembersalive = 5 and K.opponentsalive = 5
        where Ma.winnerrounds > 15
        order by ma.matchid desc
        """)
    return cur.fetchall()







MapModel = load('./Maps/MapModelXGB') 
RoundModel = load('./Rounds/RoundModelXGB')
#KillModel = load('./Kills/KillModelXGB') 

alreadyParsed = getMapsParsed()
allMaps = getMaps()

for Map in allMaps:
    if Map[0] not in alreadyParsed:
        MapPreds = MapPredictor.predict(MapModel, Map[0])
        if isinstance(MapPreds, int):
            continue
        RoundsPreds = pd.DataFrame()
        for roundNo in range(1, Map[1]):
            RoundPreds = RoundPredictor.predict(RoundModel, Map[0], roundNo)
            if isinstance(RoundPreds, int):
                continue
            RoundsPreds = RoundsPreds.append(RoundPreds)
        print(MapPreds)
        print(RoundPreds)



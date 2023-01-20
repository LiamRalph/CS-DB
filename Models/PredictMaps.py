from joblib import load

import pandas as pd
import sys
from Maps import MapQueries as MapQueries
from Rounds import RoundQueries as RoundQueries
#from Kills import KillQueries as KillQueries
from psycopg2.extensions import AsIs

from Maps import Predict as MapPredictor
from Rounds import Predict as RoundPredictor
from Kills import Predict as KillPredictor


import connectDB
conn = connectDB.database_credentials()
def getMapsParsed(hist):
    cur = conn.cursor()
    cur.execute("""
        select Ma.mapid  from matches m
        inner join maps Ma on Ma.matchid = m.matchid 
        inner join rounds R on R.mapid = Ma.mapid and R.round = 1
        inner join roundstates RS on RS.mapid = Ma.mapid and RS.round = 1 and RS.tick = 0
        inner join kills K on K.mapid = Ma.mapid and K.round = 1 and K.teammembersalive = 5 and K.opponentsalive = 5
        inner join kill_prob KP on KP.mapid = Ma.mapid and KP.round = R.round and k.tick = KP.tick
		inner join map_prob MP on MP.mapid = Ma.mapid and MP.round = R.round and MP.tick = K.tick
		inner join rs_prob RP on RP.mapid = Ma.mapid and RP.round = R.round and RP.tick = K.tick
        where Ma.winnerrounds > 15 and m.date > %s::date
        order by m.matchid asc
        """, (hist,))
    return cur.fetchall()
def getMaps(hist):
    cur = conn.cursor()
    cur.execute("""
        select Ma.mapid, ma.winnerrounds+ma.loserrounds  from maps Ma
        inner join matches m on m.matchid = ma.matchid
        inner join rounds R on R.mapid = Ma.mapid and R.round = 1
        inner join kills K on K.mapid = Ma.mapid and K.round = 1 and K.teammembersalive = 5 and K.opponentsalive = 5
        where Ma.winnerrounds > 15 and m.date > %s::date
        order by ma.matchid desc
        """, (hist,))
    return cur.fetchall()






def main(hist):
    counter = 0
    unparsedMapIDs = []
    MapModel = load('./Maps/MapModelXGB') 
    RoundModel = load('./Rounds/RoundModelXGB')
    KillModel = load('./Kills/KillModelXGB') 

    alreadyParsed = getMapsParsed(hist)
    allMaps = getMaps(hist)

    for Map in allMaps:
        if Map[0] not in alreadyParsed:
            unparsedMapIDs.append(Map)
    print(str(len(unparsedMapIDs)) + "Maps to Parse")
    for Map in unparsedMapIDs:
        for roundNo in reversed(range(1, Map[1])):
            RoundPreds = RoundPredictor.predict(RoundModel, Map[0], roundNo, 1)
        KillPreds = KillPredictor.predict(KillModel, Map[0], 1)
        MapPreds = MapPredictor.predict(MapModel, Map[0], 1)
        counter += 1
        print("Maps Done " + str(counter) + "/"+ str(len(unparsedMapIDs)), end ="\r")


if __name__ == "__main__":
    hist = int(sys.argv[1])
    if hist == 1:
        hist = '2013-01-01'
    else:
        hist = '2023-01-01'
    #while True:
    main(hist)

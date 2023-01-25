from joblib import load

import pandas as pd
import sys
import time
from Maps import MapQueries as MapQueries
from Rounds import RoundQueries as RoundQueries
#from Kills import KillQueries as KillQueries
from psycopg2.extensions import AsIs

from Maps import Predict as MapPredictor
from Rounds import Predict as RoundPredictor
from Kills import Predict as KillPredictor


import connectDB


def getMaps(hist):
    conn = connectDB.database_credentials()
    cur = conn.cursor()
    cur.execute("""
        select Ma.mapid, ma.winnerrounds+ma.loserrounds,
        CASE WHEN RP.probct is NULL THEN 1 ELSE 0 END,
		CASE WHEN KP.prob is NULL THEN 1 ELSE 0 END,
		CASE WHEN MP.probct is NULL THEN 1 ELSE 0 END
        from maps ma
        inner join matches m on m.matchid = ma.matchid
        inner join kills K on K.mapid = Ma.mapid and K.round = 1 and K.teammembersalive = 5 and K.opponentsalive = 5
        left join kill_prob KP on KP.mapid = Ma.mapid and KP.round = K.round and KP.tick = K.tick
        left join map_prob MP on MP.mapid = Ma.mapid and MP.round = K.round and MP.tick = K.tick
        left join rs_prob RP on RP.mapid = Ma.mapid and RP.round = K.round and RP.tick = K.tick
        where Ma.winnerrounds > 15 and ((KP.prob IS NULL) or (MP.probct IS NULL) or (RP.probct IS NULL)) and m.date > %s::date
        order by ma.matchid desc
        """, (hist,))
    
    return cur.fetchall()






def main(hist):
    counter = 0
    MapModel = load('./Maps/MapModelXGB') 
    RoundModel = load('./Rounds/RoundModelXGB')
    KillModel = load('./Kills/KillModelXGB') 

    Maps = getMaps(hist)
    
    if len(Maps) == 0:
        time.sleep(300)
        return
    print(str(len(Maps)) + " Maps to Parse")
    for Map in Maps:
        
        if Map[2] == 1:
            for roundNo in reversed(range(1, Map[1]+1)):
                RoundPreds = RoundPredictor.predict(RoundModel, Map[0], roundNo, 1)
        if Map[3] == 1:
            KillPreds = KillPredictor.predict(KillModel, Map[0], 1)
        if Map[4] == 1:
            MapPreds = MapPredictor.predict(MapModel, Map[0], 1)
        counter += 1
        print("Maps Done " + str(counter) + "/"+ str(len(Maps)), end ="\r")
    time.sleep(300)

if __name__ == "__main__":
    hist = int(sys.argv[1])
    if hist == 1:
        hist = '2013-01-01'
    else:
        hist = '2023-01-01'
    while True:
        main(hist)

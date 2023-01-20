import pandas as pd
from joblib import dump, load
import psycopg2
import psycopg2.extras
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from datetime import datetime
import math



import connectDB
conn = connectDB.database_credentials()

def getMapData(mapid):
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute("""
                select Map.mapname as mapname, CASE WHEN Map.winnerstart='ct' THEN 1 ELSE 0 END as winner, 
                r.winner as winnerid, r1.ct as ctstart,  
                CASE WHEN R.ct = r1.ct THEN 1 ELSE 0 END as ct,  
                r.round as RoundNo, rp.tick, r.winnerscore-1 as winnerscore, r.loserscore
                ,least(CASE WHEN (r.round > 30 and r.round%%3 = 1) then 80000 else r.winnermoney end, 80000) as winnermoney
                --,r.winnerstreak
                ,least(CASE WHEN (r.round > 30 and r.round%%3 = 1) then 80000 else r.losermoney end, 80000) as losermoney
                --,r.loserstreak
                ,CASE WHEN R.ct = r1.ct THEN rp.probct ELSE 1-rp.probct END as ctRP
                
                from maps Map
                
                inner join rounds r on r.mapid = Map.mapid 
                inner join rounds r1 on r1.mapid = Map.mapid and r1.round = 1
                inner join rs_prob rp on rp.mapid = Map.mapid and r.round = rp.round and (rp.probchangect > 0 or rp.tick = 0)
				
                where Map.mapid = %s  and Map.winnerrounds > 15
                order by r.round ASC 
                """, (mapid,))
    matches = cur.fetchall()

    if len(matches) > 0:
        return matches
    else:
        return -1


def getData(mapid):
    allMaps = getMapNames()
    deletekeys = ['winnerid', 'ctstart', 'mapname'] #, 'ctstart_score', 'tstart_score'
    ret = []
    mapData = getMapData(mapid)
    if isinstance(mapData, int):
        return -1
    for roundNo in mapData:

        roundData = {}
        roundData['mapid'] = mapid
        for mapname in allMaps:
            if mapname == roundNo["mapname"]:
                roundData[mapname] = 1
            else:
                roundData[mapname] = 0

        if roundNo['winnerid'] == roundNo['ctstart']:
            for key in roundNo.keys():
                if 'winner' in key and key not in deletekeys and key != 'winner':
                    newkey = key.replace('winner', '')
                    roundData['ctstart_'+newkey] = roundNo[key]
                elif 'loser' in key and key not in deletekeys:
                    newkey = key.replace('loser', '')
                    roundData['tstart_'+newkey] = roundNo[key]
                else:
                    roundData[key] = roundNo[key]
            ctWinRound = True
        else:
            for key in roundNo.keys():
                if 'winner' in key and key not in deletekeys and key != 'winner':
                    newkey = key.replace('winner', '')
                    roundData['tstart_'+newkey] = roundNo[key]
                elif 'loser' in key and key not in deletekeys:
                    newkey = key.replace('loser', '')
                    roundData['ctstart_'+newkey] = roundNo[key]
                else:
                    roundData[key] = roundNo[key]
            ctWinRound = False
        

        

        # ctscore = int(roundData['ctstart_score'])
        # tscore = int(roundData['tstart_score'])
        # if roundData['roundno'] > 30:
        #     if ctscore%3 == 0 and ctscore>tscore:
        #         roundData['ct_mapPoint']  = 1#ctscore-tscore
        #     else:
        #         roundData['ct_mapPoint']  = 0
        #     if tscore%3 == 0 and tscore>ctscore:
        #         roundData['t_mapPoint'] = 1#tscore-ctscore
        #     else:
        #         roundData['t_mapPoint']  = 0
        #     #winScore = ((math.floor(roundData['roundno']/6)*6)/2) + 4
        #     # roundData['ct_RoundsLeft'] = winScore-ctscore
        #     # roundData['t_RoundsLeft'] = winScore-tscore
        #     roundData['ot'] = 1
        # else:
        #     if ctscore == 15 and tscore != 15:
        #         roundData['ct_mapPoint']  = 1#ctscore-tscore
        #     else:
        #         roundData['ct_mapPoint']  = 0
        #     if tscore == 15 and ctscore != 15:
        #         roundData['t_mapPoint'] = 1#tscore-ctscore
        #     else:
        #         roundData['t_mapPoint']  = 0

        #     # if roundData['roundno'] != 30:
        #     #     roundData['ct_RoundsLeft'] = 16-ctscore
        #     #     roundData['t_RoundsLeft'] = 16-tscore
        #     # else:
        #     #     if ctscore>tscore:
        #     #         roundData['ct_RoundsLeft'] = 1
        #     #         roundData['t_RoundsLeft'] = 0
        #     #     else:
        #     #         roundData['ct_RoundsLeft'] = 0
        #     #         roundData['t_RoundsLeft'] = 1
        #     roundData['ot'] = 0

        for delkey in deletekeys:
            del roundData[delkey]
        ret.append(roundData.copy())
        
        
        

    return ret



def teamID(teamName):
    cur = conn.cursor()
    cur.execute("select teamid from teams where name = %s", (teamName,))
    ret = cur.fetchone()
    return ret 

def getMatches():
    cur = conn.cursor()
    cur.execute("""
        
        select team1, team2, date, m.maps, ma.mapid, m.matchid  from matches m
        inner join maps Ma on Ma.matchid = m.matchid 
        inner join rounds R on R.mapid = Ma.mapid and R.round = 1
        inner join roundstates RS on RS.mapid = Ma.mapid and RS.round = 1 and RS.tick = 0
        inner join rs_prob rp on rp.mapid = Ma.mapid and rp.round = 1 and rp.tick = 0
        order by m.matchid asc
        """)
    return cur.fetchall()




def getMaps(matchid):
    cur = conn.cursor()
    cur.execute("""
        select ma.mapid, ma.mapname from matches Mat
                inner join maps ma on ma.matchid = mat.matchid
                where mat.matchid = %s
        """, (matchid,))
    maps = cur.fetchall()
    ret = []
    for map in maps:
        ret.append(map)

    return ret

def getWinner(mapid):
    cur = conn.cursor()
    cur.execute("""
        select CASE WHEN Map.winnerstart='ct' THEN 1 ELSE 0 END as winner from maps Map
        where Map.Mapid = %s
        """, (mapid,))
    ret = cur.fetchall()[0]
    return ret

def getMapNames():
    cur = conn.cursor()
    cur.execute("""
                select distinct mapname from map_picks Ma
                where mapname != 'Default' and mapname != 'TBA' and mapname != 'Dust_se'
                """)
    results = cur.fetchall()
    mapnames = [x[0] for x in results]
    return mapnames

def addPred(preds):
    try:
        cur = conn.cursor()
        cur.execute('savepoint save_1;');
        args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in preds)
        cur.execute("""
                    insert into map_prob (mapid, round, tick, probct, probchangect)
                    values 
                    """ + (args_str))
        conn.commit()
    except Exception as e:
        if not isinstance(e, psycopg2.errors.UniqueViolation):
            print(e)
        #print(pred[0], pred[1], pred[2], pred[3], pred[4])
        cur.execute('rollback to save_1;');
    

def resetTable():
    cur = conn.cursor()
    command = (
                    """
    DROP table IF EXISTS map_prob;
    """)
    cur.execute(command)
    command = (
                    """
                    CREATE TABLE map_prob (
                        mapid TEXT,
                        round INT,
                        tick INT,
                        probCT float,
                        probChangeCT float,
                        PRIMARY KEY (mapid,round,tick)
                    )
                    """)
    cur.execute(command)
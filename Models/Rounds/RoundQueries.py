import pandas as pd
from joblib import dump, load
import psycopg2
import psycopg2.extras
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from datetime import datetime

import connectDB
conn = connectDB.database_credentials()

def getMapData(mapid,roundNo):
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute("""
                SELECT  M.mapname, RS.Tick, RS.CTalive, RS.Talive, RS.CTdistA, RS.TdistA, RS.CTdistB, RS.TdistB, RS.ctvalue, RS.tvalue, RS.CThp, RS.Thp, RS.TimeSincePlant,
                CASE WHEN RS.plantsite='A' THEN 1 ELSE 0 END as plantedA,
                CASE WHEN RS.plantsite='B' THEN 1 ELSE 0 END as plantedB, 
                CASE WHEN R.winnerside ='ct' THEN 1 ELSE 0 END  as winner from roundstates RS 
                inner join maps M on M.mapid = RS.mapid
                inner join rounds R on R.mapid = RS.mapid and R.round = RS.round 
                inner join matches Ma on Ma.matchid = M.matchid
                where M.mapid = %s and R.round = %s and M.winnerrounds > 15 and (rs.tick = 0 or rs.damage > 0 or rs.tick %% 2 != 1)
                order by R.round ASC
                """, (mapid,roundNo))
    matches = cur.fetchall()
    #r.winnermoney, r.losermoney,
    if len(matches) > 0:
        return matches
    else:
        return -1


def getData(mapid,roundNo):
    
    ret = []
    mapData = getMapData(mapid,roundNo)
    
    if isinstance(mapData, int):
        return -1

    ret.extend(mapData)

    return ret



def teamID(teamName):
    cur = conn.cursor()
    cur.execute("select teamid from teams where name = %s", (teamName,))
    ret = cur.fetchone()
    return ret 

def getMatches():
    cur = conn.cursor()
    cur.execute("""
        
        select team1, team2, date, m.maps, ma.mapid, m.matchid, ma.winnerrounds+ma.loserrounds  from matches m
        inner join maps Ma on Ma.matchid = m.matchid 
        inner join rounds R on R.mapid = Ma.mapid and R.round = 1
        inner join roundstates RS on RS.mapid = Ma.mapid and RS.round = 1 and RS.tick = 0
        where m.date < '2023-01-01' and m.date > '2018-01-01'
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

def getWinner(mapid, RoundNo):
    cur = conn.cursor()
    cur.execute("""
        select CASE WHEN winnerside='ct' THEN 1 ELSE 0 END as winner from Rounds 
        where Mapid = %s and round = %s
        """, (mapid, RoundNo))
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
                    insert into rs_prob (mapid, round, tick, probct, probchangect)
                    values 
                    """ + (args_str))
        conn.commit()
    except Exception as E:
        print(E)
        #print(pred[0], pred[1], pred[2], pred[3], pred[4])
        cur.execute('rollback to save_1;');

            

    conn.commit()

def resetTable():
    cur = conn.cursor()
    command = (
                    """
    DROP table IF EXISTS rs_prob;
    """)
    cur.execute(command)
    command = (
                    """
                    CREATE TABLE rs_prob (
                        mapid TEXT,
                        round INT,
                        tick int,
                        probCT float,
                        probChangeCT float,
                        PRIMARY KEY (mapid,round, tick)
                    )
                    """)
    cur.execute(command)
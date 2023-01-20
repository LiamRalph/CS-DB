import pandas as pd
from joblib import dump, load
import psycopg2
import psycopg2.extras
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from datetime import datetime

import connectDB
conn = connectDB.database_credentials()

def getMapData(mapid):
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute("""
                SELECT 
                K.round as roundno, K.mapid, M.mapname, K.tick, K.kill, K.death,  
                K.teammembersalive as CTAlive, K.opponentsalive TAlive, K.distance, K.killerz-K.victimz as CTheightDif, 
                CASE WHEN K.weapon = ANY(ARRAY['Decoy Grenade', 'Flashbang', 'C4', 'HE Grenade', 'Incendiary Grenade', 'Molotov', 'Smoke Grenade']) THEN 'Grenade' Else K.weapon END as CTweapon, K.killerhealth as CThp, CASE WHEN K.killerarmor>0 THEN 1 ELSE 0 END as CTarmor, CASE WHEN K.killerhelmet='t' THEN 1 ELSE 0 END as CThelmet, K.killerflashduration as CTflashduration, 
                ABS(K.killerxvel) as CTxvel, ABS(K.killeryvel) as CTyvel, K.killerzvel as CTzvel, 
                CASE WHEN K.victimweapon = ANY(ARRAY['Decoy Grenade', 'Flashbang', 'C4', 'HE Grenade', 'Incendiary Grenade', 'Molotov', 'Smoke Grenade']) THEN 'Grenade' Else K.victimweapon END as Tweapon, K.victimhealth as Thp, CASE WHEN K.victimarmor>0 THEN 1 ELSE 0 END as Tarmor, CASE WHEN K.victimhelmet='t' THEN 1 ELSE 0 END as Thelmet, K.victimflashduration as Tflashduration,
                ABS(K.victimxvel) as Txvel, ABS(K.victimyvel) as Tyvel, K.victimzvel as Tzvel,
                --CASE WHEN k.victimspottedkiller='t' THEN 1 ELSE 0 END as CTspotted, 1 as Tspotted,
                CASE WHEN k.victimreloading='t' THEN 1 ELSE 0 END as Treload, 0 as CTreload,
                1 as winner From kills K
                inner join rounds R on R.mapid = K.mapid and R.round = K.round 
                inner join maps M on m.mapid = K.mapid
                where ((K.teamkill = R.winner and R.winnerside = 'ct') or (K.teamkill = R.loser and R.winnerside = 't'))	and K.mapid = %s and K.weapon != '<nil>' and K.weapon != 'World' and K.victimweapon != '<nil>' and K.victimweapon != 'World'
                order by K.round, tick
                """, (mapid,))
    killsCT = cur.fetchall()
    cur.execute("""
                SELECT 
                K.round as roundno, K.mapid, M.mapname, K.tick, K.kill, K.death,  
                K.teammembersalive as TAlive, K.opponentsalive CTAlive, K.distance, K.victimz-K.killerz as CTheightDif, 
                CASE WHEN K.weapon = ANY(ARRAY['Decoy Grenade', 'Flashbang', 'C4', 'HE Grenade', 'Incendiary Grenade', 'Molotov', 'Smoke Grenade']) THEN 'Grenade' Else K.weapon END as Tweapon, K.killerhealth as Thp, CASE WHEN K.killerarmor>0 THEN 1 ELSE 0 END as Tarmor, CASE WHEN K.killerhelmet='t' THEN 1 ELSE 0 END as Thelmet, K.killerflashduration as Tflashduration, 
                ABS(K.killerxvel) as Txvel, ABS(K.killeryvel) as Tyvel, K.killerzvel as Tzvel, 
                CASE WHEN K.victimweapon = ANY(ARRAY['Decoy Grenade', 'Flashbang', 'C4', 'HE Grenade', 'Incendiary Grenade', 'Molotov', 'Smoke Grenade']) THEN 'Grenade' Else K.victimweapon END as CTweapon, K.victimhealth as CThp, CASE WHEN K.victimarmor>0 THEN 1 ELSE 0 END as CTarmor, CASE WHEN K.victimhelmet='t' THEN 1 ELSE 0 END as CThelmet, K.victimflashduration as CTflashduration, 
                ABS(K.victimxvel) as CTxvel, ABS(K.victimyvel) as CTyvel, K.victimzvel as CTzvel, 
                --CASE WHEN k.victimspottedkiller='t' THEN 1 ELSE 0 END as Tspotted, 1 as CTspotted,
                CASE WHEN k.victimreloading='t' THEN 1 ELSE 0 END as CTreload, 0 as Treload,
                0 as winner From kills K 
                inner join rounds R on R.mapid = K.mapid and R.round = K.round 
                inner join maps M on m.mapid = K.mapid
                where ((K.teamkill = R.winner and R.winnerside = 't') or (K.teamkill = R.loser and R.winnerside = 'ct'))	and K.mapid = %s and K.weapon != '<nil>' and K.weapon != 'World' and K.victimweapon != '<nil>' and K.victimweapon != 'World'
                order by K.round, tick
                """, (mapid,))
    killsT = cur.fetchall()
    kills = killsCT+killsT
    #r.winnermoney, r.losermoney,
    if len(kills) > 0:
        return kills
    else:
        return -1
def getDMG(mapid):
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute("""
                select 
                victim, damage, round as roundno, tick from roundstates
                where mapid = %s and damage > 0
                """, (mapid,))
    matches = cur.fetchall()
    #r.winnermoney, r.losermoney,
    if len(matches) > 0:
        return matches
    else:
        return -1
def getData(mapid):
    

    ret = []
    mapData = getMapData(mapid)
    if isinstance(mapData, int):
        return -1
    dmgEvents = getDMG(mapid)
    if isinstance(dmgEvents, int):
        return -1
    for kill in mapData:
        for event in dmgEvents:
            if kill['roundno'] == event['roundno'] and (kill['tick'] > event['tick'] and kill['tick']-128 < event['tick']):
                if kill['winner'] == 1:
                    if kill['kill'] == event['victim']:
                        kill['cthp'] += event['damage']
                    if kill['death'] == event['victim']:
                        kill['thp'] += event['damage']
                else:
                    if kill['kill'] == event['victim']:
                        kill['thp'] += event['damage']
                    if kill['death'] == event['victim']:
                        kill['cthp'] += event['damage']
                
                
                

        #print(kill['mapid'], kill['round'], kill['tick'], kill['cthp'], kill['thp'])
        ret.append(kill)
        
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


def getMapNames():
    cur = conn.cursor()
    cur.execute("""
                select distinct mapname from map_picks Ma
                where mapname != 'Default' and mapname != 'TBA' and mapname != 'Dust_se'
                """)
    results = cur.fetchall()
    mapnames = [x[0] for x in results]
    return mapnames
def getWeaponNames():
    cur = conn.cursor()
    cur.execute("""
                select distinct weapons.* from 
                (select distinct CASE WHEN weapon = ANY(ARRAY['Decoy Grenade', 'Flashbang', 'C4', 'HE Grenade', 'Incendiary Grenade', 'Molotov', 'Smoke Grenade']) THEN 'Grenade' Else weapon END from kills
                UNION ALL
                select distinct CASE WHEN victimweapon = ANY(ARRAY['Decoy Grenade', 'Flashbang', 'C4', 'HE Grenade', 'Incendiary Grenade', 'Molotov', 'Smoke Grenade']) THEN 'Grenade' Else victimweapon END from kills) weapons
                where weapons.weapon != '<nil>' and weapons.weapon != 'World'
                """)
    results = cur.fetchall()
    weapons = [x[0] for x in results]
    return weapons

def addPred(preds):
    try:
        cur = conn.cursor()
        cur.execute('savepoint save_1;');
        args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in preds)
        cur.execute("""
                    insert into kill_prob (mapid, round, tick, kill, death, prob)
                    values 
                    ON CONFLICT DO NOTHING
                    """ + (args_str))
        conn.commit()
    except Exception as E:
        print(E)
        #print(pred[0], pred[1], pred[2], pred[3], pred[4])
        cur.execute('rollback to save_1;');
    

def resetTable():
    cur = conn.cursor()
    command = (
                    """
    DROP table IF EXISTS kill_prob;
    """)
    cur.execute(command)
    command = (
                    """
                    CREATE TABLE kill_prob (
                        mapid TEXT,
                        round INT,
                        tick INT,
                        kill INT,
                        death INT,
                        prob float,
                        PRIMARY KEY (mapid,round,kill,death)
                    )
                    """)
    cur.execute(command)
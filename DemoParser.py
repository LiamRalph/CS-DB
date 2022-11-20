import requests, connectDB
import os
import patoolib
import time
from tqdm import tqdm 
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2, psycopg2.extras
import shutil
import re
import cleanLogs
def main():
    #Init Variables

    errors = []
    for file in os.listdir("./logs/Cleaned/Error/Kill"):
        errors.append(file.replace('.txt', ''))
    for file in os.listdir("./logs/Cleaned/Error/Round"):
        if file.replace('.txt', '') not in errors:
            errors.append(file.replace('.txt', ''))
    for file in os.listdir("./logs/Cleaned/Error/WinProb"):
        if file.replace('.txt', '') not in errors:
            errors.append(file.replace('.txt', ''))
    
    conn = connectDB.database_credentials()
    cur = conn.cursor()
    cur.execute("""
                SELECT demoid, Match.matchid, count(Map) as mapCount, case when Match.date > '2021-01-01'::date then 1 else 0 end from matches Match
                    INNER JOIN maps Map ON Map.matchid = Match.matchid
                where Match.date > (%s::date - INTERVAL'1 MONTH')::date
                GROUP BY demoid, Match.matchid
                
                ORDER BY date DESC """, (datetime.today().strftime('%Y-%m-%d'),)
                )  
    matches = []
    for row in cur:
        matches.append(row)
    cur.close()

    
    matchCounter = 0


    parsedParts = [x.split('.', 1)[0].replace('.txt', '') for x in os.listdir('./logs/Unclean/Parts')]
    

    cur = conn.cursor()
    cur.execute("""
                select distinct m.mapid from matches ma 
                inner join maps M on M.matchid = Ma.matchid
                inner join roundstates RS on RS.mapid = M.mapid
				inner join kills K on K.mapid = RS.mapid and K.round = RS.round
				
                where rs.round = 1 and RS.tick = 0
				group by m.mapid having count(K.*) > 0
                """
                )  
    alreadyParsed = []
    for row in cur:
        alreadyParsed.append(row[0])
    cur.close()

    alreadyParsed = alreadyParsed + parsedParts + errors
  
    for match in matches:
        for file in os.listdir("./working_demos/"):
            os.remove("./working_demos/"+file)

        demoid =  match[0]
        matchid = match[1]
        mapCount =  match[2]
        date = match[3]

        parse = False
        for i in range(mapCount):
            if str(matchid)+'-'+str(i+1) not in alreadyParsed:
                parse = True


        if parse:
            matchCounter += 1
            demo_path = "./demos/"+str(demoid)+".rar" 

            if os.path.isfile(demo_path): 
                try:
                    patoolib.extract_archive(demo_path, outdir="./working_demos", verbosity=-1)
                except patoolib.util.PatoolError:
                    print(demoid + " extract error")
                    continue
                if(len(os.listdir("./working_demos")) == mapCount):

                    demoNames = os.listdir("./working_demos")
                    path = './working_demos/'
                    demoPaths = [os.path.join(path,i) for i in demoNames]
                    copy = demoPaths.copy()
                    if len(demoPaths) > 1:
                        try:
                            demoPaths = sorted(demoPaths , key=lambda x:int(x.split('-')[-2][-1]))
                        except (ValueError, IndexError):
                            demoPaths = copy.sort(key=os.path.getmtime)
                            pass
                        
                        if demoPaths is None:
                            try:
                                demoPaths = sorted(copy , key=lambda x:int(x.split('/')[2].split('_')[0]))
                            except (ValueError, IndexError):
                                demoPaths = copy.sort(key=os.path.getmtime)
                                pass
                        if demoPaths is None:
                            demoPaths = copy
                    mapNo = 1
                    for demo in demoPaths:
                        cur = conn.cursor()
                        cur.execute("""SELECT winnerrounds+loserrounds from maps where mapid = %s""", (str(matchid)+'-'+str(mapNo),))
                        rounds = cur.fetchone()[0]

                        cur.execute("""SELECT * from roundstates where mapid = %s and round = 1 and tick = 0""", (str(matchid)+'-'+str(mapNo),))
                        rsOne = cur.fetchone()
                        if rsOne is None:
                            if date == 1:
                                command = 'go run "./demoScraper.go" -demo '+ demo + " -filename " + str(matchid)+'-'+str(mapNo)+ " -rs 1" + " -roundsTotal " + str(rounds) 
                            
                            else:
                                command = 'go run "./demoScraper.go" -demo '+ demo + " -filename " + str(matchid)+'-'+str(mapNo)+ " -rs 0" + " -roundsTotal " + str(rounds) 

                            os.system(command)
                            os.remove(demo)
                            cleanLogs.main("./logs/Unclean/", str(matchid)+'-'+str(mapNo)+'.txt')
                        mapNo += 1
                else:
                    #print(str(matchid) + ' has maps in Parts')
                    demoNames = os.listdir("./working_demos")
                    path = './working_demos/'
                    demoPaths = [os.path.join(path,i) for i in demoNames]
                    demoPaths = sorted(demoPaths , key=os.path.getctime)
                    mapNo = 0
                    for demo in demoPaths:
                        if '-p1.dem' in demo:
                            mapNo += 1
                            fileName = str(matchid)+'-'+str(mapNo)+'.1'
                            print(str(matchid)+'-'+str(mapNo) + ' is in Parts')
                        elif '-p2.dem' in demo:
                            if mapNo == 0:
                                mapNo += 1
                            fileName = str(matchid)+'-'+str(mapNo)+'.2'
                        elif '-p3.dem' in demo:
                            if mapNo == 0:
                                mapNo += 1
                            fileName = str(matchid)+'-'+str(mapNo)+'.3'
                        else:
                            mapNo += 1
                            fileName = str(matchid)+'-'+str(mapNo)
                        if date == 1:
                            try:
                                cur = conn.cursor()
                                cur.execute("""SELECT winnerrounds+loserrounds from maps where mapid = %s""", (str(matchid)+'-'+str(mapNo),))
                                rounds = cur.fetchone()[0]
                                command = 'go run "./demoScraper.go" -demo '+ demo + " -filename " + fileName+ " -rs 1" + " -roundsTotal " + str(rounds) 
                            except:
                                print("Non standard demo parts")
                                continue
                        os.system(command)
                        os.remove(demo)
                        
    print(str(matchCounter) + " Matches Parsed. ", end='\r')
                        
if __name__ == "__main__":
    main()
    
    
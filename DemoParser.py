import requests, connectDB
import os
import sys
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
def main(odd, monthsBack):
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
                SELECT demoid, Match.matchid, count(Map) as mapCount, count(Map) filter (where Map.mapname = 'Default' or Map.mapname = 'TBA') from matches Match
                    INNER JOIN maps Map ON Map.matchid = Match.matchid
                where (Match.matchid %% 2) = any(%s) and match.date > CURRENT_DATE - (INTERVAL '%s Month')
                GROUP BY demoid, Match.matchid
                ORDER BY date DESC """, (odd, monthsBack)) 
    matches = []
    for row in cur:
        matches.append(row)
    cur.close()
    if odd == '1':
        odd = '_odd'
    else:
        odd = ''
    
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
        for file in os.listdir("./working_demos"+odd+"/"):
            os.remove("./working_demos"+odd+"/"+file)

        demoid =  match[0]
        matchid = match[1]
        mapCount =  match[2]
        default  =  match[3]
        
        parse = False
        for i in range(mapCount):
            if str(matchid)+'-'+str(i+1) not in alreadyParsed:
                parse = True


        if parse:
            matchCounter += 1
            demo_path = "./demos/"+str(demoid)+".rar" 

            if os.path.isfile(demo_path): 
                try:
                    patoolib.extract_archive(demo_path, outdir="./working_demos"+odd+"/", verbosity=-1)
                except patoolib.util.PatoolError:
                    print(str(demoid) + " extract error")
                    continue
                if(len(os.listdir("./working_demos"+odd+"/")) == mapCount-int(default)):

                    demoNames = os.listdir("./working_demos"+odd+"/")
                    path = './working_demos'+odd+"/"
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
                    if default:
                        mapNo = 2
                    for demo in demoPaths:
                        cur = conn.cursor()
                        cur.execute("""SELECT winnerrounds+loserrounds from maps where mapid = %s""", (str(matchid)+'-'+str(mapNo),))
                        rounds = cur.fetchone()[0]
                        print("Current Match: " + str(matchid)+'-'+str(mapNo), end='\r')
                        command = 'go run "./demoScraper.go" -demo '+ demo + " -filename " + str(matchid)+'-'+str(mapNo) + " -roundsTotal " + str(rounds) 

                        os.system(command)
                        os.remove(demo)
                        cleanLogs.main("./logs/Unclean/", str(matchid)+'-'+str(mapNo)+'.txt')
                        mapNo += 1
                else:
                    #print(str(matchid) + ' has maps in Parts')
                    demoNames = os.listdir("./working_demos"+odd+"/")
                    path = "./working_demos"+odd+"/"
                    demoPaths = [os.path.join(path,i) for i in demoNames]
                    demoPaths = sorted(demoPaths , key=os.path.getctime)
                    for demo in demoPaths:
                        if '-m1-' in demo:
                            mapNo = 1
                        elif '-m2-' in demo:
                            mapNo = 2
                        elif '-m3-' in demo:
                            mapNo = 3
                        elif '-m4-' in demo:
                            mapNo = 4
                        elif '-m5-' in demo:
                            mapNo = 5
                        else:
                            continue


                        if '-p1.dem' in demo:
                            fileName = 'Parts/'+str(matchid)+'-'+str(mapNo)
                            print(str(matchid)+'-'+str(mapNo) + ' is in Parts')
                        else:
                            fileName = 'Parts/'+str(matchid)+'-'+str(mapNo)

                        try:

                            cur = conn.cursor()
                            cur.execute("""SELECT winnerrounds+loserrounds from maps where mapid = %s""", (str(matchid)+'-'+str(mapNo),))
                            rounds = cur.fetchone()[0]
                            print("Current Match: " + str(matchid)+'-'+str(mapNo), end='\r')
                            command = 'go run "./demoScraper.go" -demo '+ demo + " -filename " + fileName + " -roundsTotal " + str(rounds) 
                            os.system(command)
                        except:
                            print("Non standard demo parts")
                            continue
                        
                        os.remove(demo)
                        cleanLogs.main("./logs/Unclean/Parts/", str(matchid)+'-'+str(mapNo)+'.txt')
    print(str(matchCounter) + " Matches Parsed. ", end='\r')
                        
if __name__ == "__main__":
    odd = list(sys.argv[1].split(','))
    monthsBack = int(sys.argv[2])
    main(odd, monthsBack)
    
    
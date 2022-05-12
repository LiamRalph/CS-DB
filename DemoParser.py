import requests
import os
import patoolib
import time
import datetime
from tqdm import tqdm 
from bs4 import BeautifulSoup
from datetime import date
from dateutil.relativedelta import relativedelta
import psycopg2, psycopg2.extras
import shutil
import re
import cleanLogs
def main():
    #Init Variables


    #subnet = '1.3'
    subnet = '86.113'
    IP = 'csgo.cqtpbfnejnsi.us-east-2.rds.amazonaws.com'



    conn = psycopg2.connect("dbname=CSGO user=postgres password=Hoc.ey1545" + " host='" + IP + "'")
    cur = conn.cursor()
    cur.execute("""
                SELECT demoid, Match.matchid, count(Map) as mapCount from matches Match
                    INNER JOIN maps Map ON Map.matchid = Match.matchid
                GROUP BY demoid, Match.matchid
                ORDER BY demoid DESC """
                )  
    matches = []
    for row in cur:
        matches.append(row)
    cur.close()


    matchCounter = 0


    parsedParts = [x.split('.', 1)[0]+'.txt' for x in os.listdir('./logs/Unclean/Parts')]
    alreadyParsed = os.listdir('./logs/Unclean') + parsedParts

    

    for match in matches:
        for file in os.listdir("./working_demos/"):
            os.remove("./working_demos/"+file)
        demoid =  match[0]
        matchid = match[1]
        mapCount =  match[2]
        
        if str(matchid)+'-'+str(mapCount)+".txt" not in alreadyParsed and str(matchid)+'-'+str(mapCount-1)+".txt" not in alreadyParsed and str(matchid)+'-'+str(mapCount-2)+".txt" not in alreadyParsed:
            matchCounter += 1
            demo_path = "./demos/"+str(demoid)+".rar" 
            if os.path.isfile(demo_path): 
                patoolib.extract_archive(demo_path, outdir="./working_demos", verbosity=-1)
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
                        command = 'go run "./demoScraper.go" -demo '+ demo + " -filename " + str(matchid)+'-'+str(mapNo)
                        os.system(command)
                        os.remove(demo)
                        cleanLogs.main(str(matchid)+'-'+str(mapNo)+'.txt')
                        mapNo += 1
                else:
                    demoNames = os.listdir("./working_demos")
                    path = './working_demos/'
                    demoPaths = [os.path.join(path,i) for i in demoNames]
                    demoPaths = sorted(demoPaths , key=os.path.getmtime)
                    mapNo = 1
                    for demo in demoPaths:
                        part = False
                        if '-p1.dem' in demo:
                            fileName = str(matchid)+'-'+str(mapNo)+'.1'
                            part = True
                        elif '-p2.dem' in demo:
                            fileName = str(matchid)+'-'+str(mapNo)+'.2'
                            part = False
                        else:
                            fileName = str(matchid)+'-'+str(mapNo)
                        command = 'go run "./demoScraper.go" -demo '+ demo + " -filename " + fileName
                        os.system(command)
                        os.remove(demo)
                        if not part:
                            mapNo += 1
    print(str(matchCounter) + " Matches Parsed", end='\r')
                        
if __name__ == "__main__":
    main()
    
    
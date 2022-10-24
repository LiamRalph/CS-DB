import requests, time, os, csv, psycopg2, psycopg2.extras, connectDB
from bs4 import BeautifulSoup
from datetime import date, datetime
from dateutil.relativedelta import relativedelta


def main():
    global OriginalURL 
    global IP
    global players
    global teams
    players = []
    teams = []
    #subnet = '1.3'


    OriginalURL = "https://www.hltv.org"
    header = ["MatchLink", "MatchID", "DemoLink", "DemoID", "TeamNames", "TeamIDs", "PlayerNames", 'team1', 'team2', 'team1rank', 'team2rank', "PlayerIDs", "Sides", "MapScores", "HalfScore", "MapNames", "Winner", "Loser", "Time", "Date", "UNIX", "TournamentName", "TournamentID", "Maps", "POTM" ]

    
    while True:
        createDB()
        savedMatchLinks = getAlreadySaved()
        queryString = '/results?'
        queryString = getQuery(queryString)
        URL = OriginalURL + queryString
        matchLinks = []
        scrollThroughPages(matchLinks, savedMatchLinks, URL)
        matchCount = len(matchLinks)
        matchCounter = 1
        if matchCount > 0:
            print(str(matchCount) + " Matches Found")
        matchDicts = []
        for match in reversed(matchLinks):
            matchInfoDict = dict.fromkeys(header)
            getMatchInfo(matchInfoDict, match)
            if  "team1rank" in matchInfoDict and "team2rank" in matchInfoDict:
                addToFile(matchInfoDict, header)
                addToDB(matchInfoDict)
                output = str(matchCounter) + " / " + str(matchCount)
                print(output, end='\r')
                matchCounter += 1
                matchDicts.append(matchInfoDict)
        time.sleep(300)
   
    
    
    
    
    
def tableExists(tableName):
    conn = connectDB.database_credentials()
    cur = conn.cursor()
    cur.execute("SELECT * from information_schema.tables where table_name=%s", (tableName,))
    return bool(cur.rowcount)
def createDB():
    """ create tables in the PostgreSQL database"""
    command = (
        """
        CREATE TABLE matches (
            MatchLink text,	
	        MatchID integer,	
            DemoLink text,	
            DemoID integer,		
            Winner text,
            Loser text,	
            team1 integer,
            team2 integer,
            team1rank integer,
            team2rank integer,
            Time time,	
            Date date,	
            UNIX bigint,	
            TournamentName text,	
            TournamentID integer,	
            Maps integer,	
            POTM text,	
            PRIMARY KEY (MatchID)
        )
        """)
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = connectDB.database_credentials()
        cur = conn.cursor()
        if not tableExists('matches'):
            cur.execute(command)
        if not tableExists('teams'):
            command = (
                    """
                    CREATE TABLE teams (
                        name TEXT, 
                        teamid INT ,
                        PRIMARY KEY (teamid)
                    )
                    """)
            cur.execute(command)
        if not tableExists('players'):
            command = (
                    """
                    CREATE TABLE players (
                        name TEXT, 
                        playerid INT,
                        PRIMARY KEY (playerid)
                        );
                    INSERT into players (name, playerid) values ('World', -1)
                       
                    """)
            cur.execute(command)

        if not tableExists('maps'):
            command = (
                    """
                    CREATE TABLE maps (
                        matchid INT, 
                        mapid TEXT, 
                        mapNumber INT, 
                        mapname TEXT, 
                        winnerid INT,
                        loserid INT,
                        winnerrounds INT,
                        loserrounds INT,
                        winnerstart TEXT, 
                        loserstart TEXT,
                        winnerhalf INT,
                        loserhalf INT,
                        PRIMARY KEY (mapid), 
                        CONSTRAINT map_winner_fk
                            FOREIGN KEY(winnerid) 
	                            REFERENCES teams(teamid),
                        CONSTRAINT map_loser_fk
                            FOREIGN KEY(loserid) 
	                            REFERENCES teams(teamid),
                        CONSTRAINT map_match_fk
                            FOREIGN KEY(matchid) 
	                            REFERENCES matches(matchid)
                        )
                    """)
            cur.execute(command)

        if not tableExists('team_matches'):
            command = (
                    """
                    CREATE TABLE team_matches (
                        teamid INT, 
                        matchid INT,
                        PRIMARY KEY (teamid, matchid),
                        CONSTRAINT team_matches_fk
                            FOREIGN KEY(teamid) 
	                            REFERENCES teams(teamid),
                        CONSTRAINT matches_team_fk
                            FOREIGN KEY(matchid) 
                                REFERENCES matches(matchid)
                        )
                    """)
            cur.execute(command)

        if not tableExists('player_maps'):
            command = (
                    """
                    CREATE TABLE player_maps (
                        playerid INT, 
                        mapid TEXT,
                        teamid INT,
                        PRIMARY KEY (playerid, mapid),
                        CONSTRAINT player_maps_fk
                            FOREIGN KEY(playerid) 
                                REFERENCES players(playerid),
                        CONSTRAINT maps_player_fk
                            FOREIGN KEY(mapid) 
                                REFERENCES maps(mapid),
                        CONSTRAINT maps_player_team_fk
                            FOREIGN KEY(teamid) 
                                REFERENCES teams(teamid)       
                    )
                    """)
            cur.execute(command)    
        if not tableExists('kills'):
            command = (
                    """
                    CREATE TABLE kills (
                        mapid TEXT,
                        round INT,
                        tick INT,
                        teammembersAlive INT,
                        opponentsAlive INT,
                        teamkill INT,
                        kill INT,
                        teamdeath INT,
                        death INT,
                        weapon TEXT,
                        headshot boolean,
                        killerhealth INT,
                        killerarmor INT,
                        killerhelmet boolean,
                        victimhealth INT,
                        victimarmor INT,
                        victimhelmet boolean,
                        killerx float,
                        killery float,
                        killerz float,
                        killerpitch float,
                        killeryaw float,
                        victimx float,
                        victimy float,
                        victimz float,
                        victimpitch float,
                        victimyaw float,
                        killerflashduration float,
                        victimflashduration float,
                        victimplantingordefusing boolean,
                        victimweapon TEXT,
                        victimreloading boolean,
                        victimspottedkiller boolean,
                        horizontaldif float,
                        verticaldif float,
                        distance float,
                        heightdif float,
                        KillerXvel float, 
                        KillerYvel float, 
                        KillerZvel float, 
                        VictimXvel float, 
                        VictimYvel float, 
                        VictimZvel float,
                        ExpectedKill float,
                        PRIMARY KEY (mapid, round, kill, death),
                        CONSTRAINT killer_kills_fk
                            FOREIGN KEY(kill) 
                                REFERENCES players(playerid),
                        CONSTRAINT victim_kills_fk
                            FOREIGN KEY(death) 
                                REFERENCES players(playerid),
                        CONSTRAINT maps_kills_fk
                            FOREIGN KEY(mapid) 
                                REFERENCES maps(mapid)
                    )
                    """)
            cur.execute(command)
        if not tableExists('rounds'):
            command = (
                    """
                    CREATE TABLE rounds (
                        mapid TEXT,
                        round INT,
                        winnerside TEXT,
                        winner INT,
                        loser INT,
                        CT INT,
                        T INT,
                        winnerscore INT,
                        loserscore INT,
                        winnervalue INT,
                        loservalue INT,
                        winneralive INT,
                        loseralive INT,
                        winnersaved INT,
                        losersaved INT,
                        winnermoney INT,
                        losermoney INT,
                        winnerstreak INT,
                        loserstreak INT,
                        CTprobabilityMap float,
                        TprobabilityMap float,
                        PRIMARY KEY (mapid, round),
                        CONSTRAINT maps_rounds_fk
                            FOREIGN KEY(mapid) 
                                REFERENCES maps(mapid)
                    )
                    """)
            cur.execute(command)

        if not tableExists('roundstates'):
            command = (
                    """
                    CREATE TABLE roundstates (
                        mapid TEXT,
                        round INT,
                        Tick INT,
                        CT INT,
                        T INT,
                        CTalive INT,
                        Talive INT,
                        CTdistA float,
                        TdistA float,
                        CTdistB float,
                        TdistB float,
                        CTvalue INT,
                        Tvalue INT,
                        CThp INT,
                        Thp INT, 
                        TimeSincePlant INT,
                        PlantSite TEXT,
                        CTprobability float,
                        Tprobability float,
                        ProbabilityChange float,
                        attacker INT,
                        victim INT,
                        damage INT,
                        PRIMARY KEY (mapid, round, tick, attacker, victim),
                        CONSTRAINT maps_rounds_fk
                            FOREIGN KEY(mapid, round) 
                                REFERENCES rounds(mapid, round)
                    )
                    """)
            cur.execute(command)
        if not tableExists('saves'):
            command = (
                    """
                    CREATE TABLE saves (
                        mapid TEXT,
                        round INT,
                        tick INT,
                        teammembersAlive INT,
                        opponentsAlive INT,
                        Save INT,
                        SavedValue INT,
                        PRIMARY KEY (mapid, round),
                        CONSTRAINT save_maps_fk
                            FOREIGN KEY(mapid) 
                                REFERENCES maps(mapid)
                    )
                    """)
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
def getQuery(queryString):
    #stars = int(input("Stars: "))
    stars = 0
    #dateDelta = int(input("Months Back: "))
    dateDelta = 96
    Today = date.today()
    TodayDate = Today.strftime("%Y-%m-%d")

    conn = connectDB.database_credentials()
    cur = conn.cursor()
    cur.execute("""select date from matches
                    order by date DESC
                    Limit 1""")
    
    Start = cur.fetchone() 
    if Start != None:
        if Start[0] >= date(2022,5,8):
            stars = 1
        else:
            stars = 2
        StartDate = Start[0].strftime("%Y-%m-%d")
        #StartDate = '2022-05-08' 
        queryString += 'startDate='+StartDate+'&endDate='+TodayDate+'&content=demo&stars='+str(stars)
    else:
        stars = 2
        queryString += 'startDate=2014-01-01'+'&endDate='+TodayDate+'&content=demo&stars='+str(stars)

    
    return(queryString)
def getAlreadySaved():
    conn = connectDB.database_credentials()
    cur = conn.cursor()
    cur.execute("SELECT matchlink from matches")
    savedMatchLinks = [r[0] for r in cur.fetchall()]
    return(savedMatchLinks)
def scrollThroughPages(matchLinks, savedMatchLinks, URL):
    nextPageExists = True
    while(nextPageExists):
        directoryPage = requests.get(URL, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0'})
        directorySoup = BeautifulSoup(directoryPage.text, "html.parser")
        getMatchesFromPage(matchLinks, savedMatchLinks, directorySoup)
        nextPageLink = directorySoup.find('a', class_='pagination-next')
        if "href" in str(nextPageLink):
            nextPageExists = True
            URL = OriginalURL+nextPageLink["href"]
        else:
            nextPageExists = False
        time.sleep(2)
def getMatchesFromPage(matchLinks, savedMatchLinks, directorySoup):
    matchLinks += (OriginalURL+a["href"] for a in directorySoup.find_all('a', class_='a-reset') if a['href'].startswith('/matches/') and a['class'] == ['a-reset'] and OriginalURL+a["href"] not in savedMatchLinks)   
def getMatchInfo(matchInfoDict, match):
    while True:
        try: 
            matchPage = requests.get(match, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0'})
        except requests.exceptions.ConnectionError as e:
            print("Connection Error, Retrying in 30 seconds")
            time.sleep(30)
            continue
        break
    
    matchSoup = BeautifulSoup(matchPage.text, "html.parser")
    matchInfoDict["MatchLink"] = match
    matchInfoDict["MatchID"] = match.split("/")[4]
    for a in matchSoup.find_all('a', class_='flexbox left-right-padding'):
            if(a['href'].startswith('/download/')):
                matchInfoDict["DemoLink"] = OriginalURL + a['href']
                matchInfoDict["DemoID"] = a['href'].split("/")[3]      
    #print(matchInfoDict["MatchLink"])
    maps =  matchSoup.find_all('div', class_='map-name-holder')
    scores =  matchSoup.find_all('div', class_='results-team-score')
    sides = matchSoup.find_all('div', class_='results-center-half-score')
    mapCount = int(len(scores)/2)
    matchInfoDict["Maps"] = mapCount
    matchInfoDict["MapScores"] = []
    matchInfoDict["MapNames"] = []
    matchInfoDict["Sides"] = []
    matchInfoDict["HalfScore"] = []
    mapPlayedCount = 0
    for i in range(0, mapCount*2, 2):
        if(scores[i].text.isdigit() and scores[i+1].text.isdigit()):
            matchInfoDict["MapScores"].append([int(scores[i].text), int(scores[i+1].text)])
            mapPlayedCount += 1
        
    for i in range(0, mapPlayedCount, 1):   
        matchInfoDict["MapNames"].append(maps[i].find('div', class_='mapname').text)
        if len(sides[i].find_all('span')) > 0:
            matchInfoDict["Sides"].append(sides[i].find_all('span')[1].get('class'))
            matchInfoDict["Sides"].append(sides[i].find_all('span')[3].get('class'))

            matchInfoDict["HalfScore"].append(int(sides[i].find_all('span')[1].text))
            matchInfoDict["HalfScore"].append(int(sides[i].find_all('span')[3].text))
        else:
            matchInfoDict["Sides"].append("N/A")
            matchInfoDict["Sides"].append("N/A")

            matchInfoDict["HalfScore"].append(-1)
            matchInfoDict["HalfScore"].append(-1)

    try:
        matchInfoDict["Winner"] = matchSoup.find('div', class_='won').find_parent('div').find('a')["href"].split("/")[3]
        matchInfoDict["Loser"] = matchSoup.find('div', class_='lost').find_parent('div').find('a')["href"].split("/")[3]
    except (AttributeError, TypeError):
        matchInfoDict["Winner"] = "Tie"
        matchInfoDict["Loser"] = "Tie"
        pass

    matchInfoDict["TeamIDs"] = []
    matchInfoDict["TeamNames"] = []
    team1 = matchSoup.find('div', class_='team1-gradient')
    team1url = team1.find("a")["href"]
    team1ID = int(team1url.split("/")[2])
    team1name = team1url.split("/")[3]
    matchInfoDict["TeamIDs"].append(int(team1url.split("/")[2]))
    matchInfoDict["TeamNames"].append(team1url.split("/")[3])
    team2 = matchSoup.find('div', class_='team2-gradient')
    team2url = team2.find("a")["href"]
    team2ID = int(team2url.split("/")[2])
    team2name = team2url.split("/")[3]
    matchInfoDict["TeamIDs"].append(int(team2url.split("/")[2]))
    matchInfoDict["TeamNames"].append(team2url.split("/")[3])
    if([team1name, team1ID] not in teams):
                teams.append([team1name, team1ID])
    if([team2name, team2ID] not in teams):
                teams.append([team2name, team2ID])



    matchInfoDict["team1"] = team1ID
    matchInfoDict["team2"] = team2ID
    ranks = matchSoup.find_all('div', class_='teamRanking')
    if not ranks:
        return
    rank1 = ranks[0].find("span")
    rank2 = ranks[1].find("span")

    try:
        if rank1.text == "Unranked":
            matchInfoDict["team1rank"] = 30
        else:
            matchInfoDict["team1rank"] = int(rank1.next_sibling.replace("#", ""))
    except IndexError:
        matchInfoDict["team1rank"] = 30

    try:
        if rank2.text == "Unranked":
            matchInfoDict["team2rank"] = 30
        else:
            matchInfoDict["team2rank"] = int(rank2.next_sibling.replace("#", ""))
    except IndexError:
        matchInfoDict["team2rank"] = 30


    
    
    IDs = matchSoup.find_all('td', class_='player player-image')
    playerIDs = []
    playerNames = []
    for player in IDs:
        try:
            playerLink = player.find("a")["href"]
            playerID = int(playerLink.split("/")[2])
            playerName = playerLink.split("/")[3]
            playerIDs.append(int(playerLink.split("/")[2]))
            playerNames.append(playerLink.split("/")[3])
            if([playerName, playerID] not in players):
                players.append([playerName, playerID])
        except KeyError:
            print("No Link Found")
            playerIDs.append(-1)
            playerNames.append(player.find("img")["alt"])
            print(match.split("/")[4])
            if([player.find("img")["alt"], -1] not in players):
                players.append([player.find("img")["alt"], -1])
    if len(playerNames) < 5 or len(playerIDs) < 5:
        matchInfoDict = {}
    matchInfoDict["PlayerIDs"] = [playerIDs[0:5], playerIDs[5:10]]
    matchInfoDict["PlayerNames"] = [playerNames[0:5], playerNames[5:10]]

    
    matchInfoDict["Time"] = matchSoup.find('div', class_='time').text
    matchInfoDict["UNIX"] = matchSoup.find('div', class_='time')["data-unix"]

    ts = int(matchInfoDict["UNIX"])/1000
    date = datetime.utcfromtimestamp(ts)
    date = date.strftime('%Y-%m-%d')
    matchInfoDict["Date"] = date
    
    
    matchInfoDict["TournamentName"] = matchSoup.find('div', class_='event text-ellipsis').find("a").text
    matchInfoDict["TournamentID"] = int(matchSoup.find('div', class_='event text-ellipsis').find("a")["href"].split("/")[2])
    

     
    potm = matchSoup.find('div', class_='highlighted-player potm-container')
    if potm is not None:
        potm = potm.find('span', class_='player-nick').text
        matchInfoDict["POTM"] = potm
    else:
        matchInfoDict["POTM"] = "N/A"
    time.sleep(1)
def addToFile(matchInfoDict, header):
    with open("./MatchInfoFile.csv", 'a', newline='') as matchInfoFile:
        writer = csv.DictWriter(matchInfoFile, fieldnames=header)
        writer.writerow(matchInfoDict)   

def addToDB(matchInfoDict):

    scores = matchInfoDict.pop("MapScores", None)
    maps = matchInfoDict.pop("MapNames", None)
    sides = matchInfoDict.pop("Sides", None)
    halfScores = matchInfoDict.pop("HalfScore", None)
    teamsMatch = list(zip(matchInfoDict.pop("TeamNames", None), matchInfoDict.pop("TeamIDs", None)))
    playerIDs = matchInfoDict.pop("PlayerIDs", None)
    playerNames = matchInfoDict.pop("PlayerNames", None), 
    if matchInfoDict["DemoID"] is None or playerIDs is None:
        return
    playerIDs1 = playerIDs[0]
    playerNames1 = playerNames[0][0]
    playersMatch1 = list(zip(playerNames1, playerIDs1))

    playerIDs2 = playerIDs[1]
    playerNames2 = playerNames[0][1]
    playersMatch2 = list(zip(playerNames2, playerIDs2))

    if matchInfoDict["DemoID"] is None:
        return
    columns = matchInfoDict.keys()
    values = [matchInfoDict[column] for column in columns]
    sql = "INSERT into matches (%s) values %s  ON CONFLICT DO NOTHING RETURNING MatchID"
    conn = None
    try:
        conn = connectDB.database_credentials()
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (psycopg2.extensions.AsIs(','.join(columns)), tuple(values)))
        # close communication with the database
        mapCount = 1
        for team in teamsMatch:
            addTeam(team)
            sql = "INSERT into team_matches (teamid, matchid) values (%s, %s) ON CONFLICT DO NOTHING RETURNING MatchID"
            cur.execute(sql, (team[1], matchInfoDict["MatchID"]))
        for map in maps:
            #matchid INT, mapid INT, mapname TEXT, teamid INT, score INT
            mapID = str(matchInfoDict["MatchID"]) + "-" + str(mapCount)
            sql = "INSERT into maps (matchid, mapid, mapNumber, mapname, winnerid, loserid, winnerrounds, loserrounds, winnerstart, loserstart, winnerhalf, loserhalf) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  ON CONFLICT DO NOTHING RETURNING mapid"
            if int(scores[mapCount-1][0]) > int(scores[mapCount-1][1]):
                cur.execute(sql, (matchInfoDict["MatchID"], mapID, mapCount, map, teamsMatch[0][1], teamsMatch[1][1], scores[mapCount-1][0], scores[mapCount-1][1], sides[(mapCount-1)*2][0], sides[((mapCount-1)*2)+1][0], halfScores[(mapCount-1)*2], halfScores[((mapCount-1)*2)+1]))
            else:                
                cur.execute(sql, (matchInfoDict["MatchID"], mapID, mapCount, map, teamsMatch[1][1], teamsMatch[0][1], scores[mapCount-1][1], scores[mapCount-1][0], sides[((mapCount-1)*2)+1][0], sides[(mapCount-1)*2][0], halfScores[((mapCount-1)*2)+1], halfScores[(mapCount-1)*2]))
            for player in playersMatch1:
                addPlayer(player)     
                sql = "INSERT into player_maps (playerid, mapid, teamid) values (%s, %s, %s)  ON CONFLICT DO NOTHING;"
                cur.execute(sql, (player[1], mapID, teamsMatch[0][1]))
            for player in playersMatch2:
                addPlayer(player)     
                sql = "INSERT into player_maps (playerid, mapid, teamid) values (%s, %s, %s)  ON CONFLICT DO NOTHING;"
                cur.execute(sql, (player[1], mapID, teamsMatch[1][1]))
            mapCount += 1
        
        

        #results = cur.fetchall()
        #for result in results:
            #print(result[0])
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def addPlayer(Player):
    sql = """
            INSERT into players (name, playerid) values (%s, %s)  ON CONFLICT DO NOTHING RETURNING playerid
        """
    conn = None
    try:
        conn = connectDB.database_credentials()
        cur = conn.cursor()
        cur.execute(sql, (Player[0], Player[1]))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def addTeam(Team):    
    sql = """
            INSERT into teams (name, teamid) values (%s, %s)  ON CONFLICT DO NOTHING RETURNING teamid
        """
    conn = None
    try:
        conn = connectDB.database_credentials()
        cur = conn.cursor()
        cur.execute(sql, (Team[0], Team[1]))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
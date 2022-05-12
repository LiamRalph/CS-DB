import os
import json
import math
import time
import Levenshtein
import psycopg2
from joblib import dump, load
from PredictorKills import predictKill
from PredictorRounds import predictRound
from PredictorMaps import predictMap
#Iterate through logs

def main(mapID):
    #mapID = '2356098-1.txt'

    gbrK = load('ExKillModelXGB')
    gbrR = load('ExRoundModelXGB')
    gbrM = load('ExMapModelXGB')
    #subnet = '1.3'
    subnet = '86.113'
    IP = 'csgo.cqtpbfnejnsi.us-east-2.rds.amazonaws.com'


    try:
        allLines = []
        
        with open("./logs/Unclean/"+mapID) as log:
            error = False
            notLiveRound = False
            headers = []
            logList = list(log)
            logList = reversed(logList)
            firstKillFound = False
            output = []
            roundOutput = []
            roundProbOutput = []
            worldKillCounter = 0

            for line in logList:
                
                if notLiveRound:
                    if worldKillCounter == 10:
                        notLiveRound = False
                        worldKillCounter = 0
                    if "Kill" in line:
                        weapon = json.loads(line.replace("'", "").replace("\\", ""))["Weapon"]
                        killer = json.loads(line.replace("'", "").replace("\\", ""))["Kill"]
                        if weapon == "World":
                            worldKillCounter += 1
                            continue
                    if '"Save":' in line:
                        peopleAlive = int(json.loads(line.replace("'", "").replace("\\", ""))["teamMembersAlive"]) + int(json.loads(line.replace("'", "").replace("\\", ""))["opponentsAlive"])
                        if peopleAlive == 10:
                            worldKillCounter += 1
                            continue

                if line.startswith("Round: NOT_LIVE"):
                    notLiveRound = True
                elif "CTalive" in line and not firstKillFound:
                    outputLine = json.loads(line.replace("'", "").replace("\\", ""))
                    if outputLine['Round'] == '1' and outputLine['CTalive'] == '5' and outputLine['Talive'] == '5' and int(outputLine['Tick']) < 10:
                        firstKillFound = True
                    roundProbOutput.append(outputLine)

                elif "Kill" in line and not firstKillFound:
                    #print(line)
                    line = json.loads(line.replace("'", "").replace("\\", ""))
                    

                    deltaX = float(line['KillerX']) - float(line['VictimX'])
                    deltaY = float(line['KillerY']) - float(line['VictimY'])
                    deltaZ = float(line['KillerZ']) - float(line['VictimZ'])
                    distance = math.sqrt(((deltaX)**2) + (deltaY**2))
                    horizAngleToKiller = math.degrees(math.atan2(deltaY, deltaX))

                    horizontalDif = (((horizAngleToKiller - float(line['VictimYaw']))+180)%360)-180
                    verticalDif = math.degrees(math.atan2(deltaZ, distance)) - float(line['VictimPitch'])
                    line['horizontalDif'] = horizontalDif
                    line['verticalDif'] = verticalDif*-1
                    line['distance'] = distance
                    line['heightDif'] = deltaZ
                    output.append(line)
                elif "SavedValue" in line and not firstKillFound:
                    line = json.loads(line.replace("'", "").replace("\\", ""))
                    output.append(line)
                elif "Players" in line:
                    headers.append(line.replace("'", "").replace("\\", ""))
                elif "WinnerScore" in line and not firstKillFound:
                    outputLine = json.loads(line.replace("'", "").replace("\\", ""))
                    roundOutput.append(outputLine)

                #elif str(line).startswith('Match: Game State Changed'):

            conn = psycopg2.connect("dbname=CSGO user=postgres password=Hoc.ey1545" + " host='" + IP + "'")
            cur = conn.cursor()
            cur.execute("""SELECT Team.name as name, Team.teamid as id
                                FROM teams Team
                                    INNER JOIN team_matches TM ON TM.teamid = Team.teamid
                                    INNER JOIN matches Match ON Match.matchid = TM.matchid
                                    WHERE TM.teamid = Team.teamid and Match.matchid = %s 
                                    group by Team.name, Team.teamid
                        """, (mapID[:-6],))
            team1DB = cur.fetchone()
            team2DB = cur.fetchone()
            cur.close()
            cur = conn.cursor()
            cur.execute("SELECT mapname from maps where mapid = %s", (mapID[:-4],))
            mapname = cur.fetchone()
            cur.close()
            if len(headers) != 2:
                return

            team1log = team1logOG = json.loads(headers[0])["Team"].lower()
            team2log = team2logOG = json.loads(headers[1])["Team"].lower()

            excessWordsTeams = ["esports", " esports", "parimatch", "gg.bet", "luk0il", ".", ".1xbet", "[not ready]", "[not ready] ", "[ready]", " gaming", "[READY] "]
            excessWordsPlayers = [".Parimatch", "_LigaStavok", "BIG ", "forZe-", "_x_","* HyperX","* twitch.tv","* EGB.com","* Hellcase.com","*66esports", "LG * ", "Virtus.pro G2A ", "luk0il", " x LUKOIL", "parimatch", " GG.BET", "*LBETç«žæŠ€", "-", "* Parimatch", "(1)", "^.^", "_ligastavok", " x parimatch", "flames*", "Flames*", "mrs - ", "big", "avangar", "cr4zy", "AVANGAR|", "G2|", "ENVYUS ", "-iwnl-", "Cloud9", "[C]", "[D]", "CLG ", "CLG", "Heroic", "mouz|", " Betway", "HEET"]
            for word in excessWordsTeams:
                if word in team1log:
                    team1log = team1log.replace(word, "")
                if word in  team2log:
                    team2log = team2log.replace(word, "")

            if team1log == "ninjas in pyjamas":
                team1log = "nip"
            if team2log == "ninjas in pyjamas":
                team2log = "nip"

            if team1log == "vp":
                team1log = "virtus pro"
            if team2log == "vp":
                team2log = "virtus pro"
            
            if team1log == "navi":
                team1log = "natus-vincere"
            if team2log == "navi":
                team2log = "natus-vincere"

            
            teamsConvert = {}
            team1ratio = 0
            team2ratio = 0
            if Levenshtein.ratio(team1DB[0], team1log) > Levenshtein.ratio(team2DB[0], team1log):
                team1ratio = Levenshtein.ratio(team1DB[0], team1log)
                team1ID = team1DB[1]
            else:
                team1ratio = Levenshtein.ratio(team2DB[0], team1log)
                team1ID = team2DB[1]
            teamsConvert[team1logOG] = team1ID
            if Levenshtein.ratio(team1DB[0], team2log) > Levenshtein.ratio(team2DB[0], team2log):
                team2ratio = Levenshtein.ratio(team1DB[0], team2log)
                team2ID = team1DB[1]
            else:
                team2ratio = Levenshtein.ratio(team2DB[0], team2log)
                team2ID = team2DB[1]
            teamsConvert[team2logOG] = team2ID
            if team1ratio >= 0.7:
                if team1ID == team1DB[1]:
                    team2ID =  team2DB[1]
                else:
                    team2ID =  team1DB[1]
            if team2ratio >= 0.7:
                if team2ID == team1DB[1]:
                    team1ID = team2DB[1]
                else:
                    team1ID = team1DB[1]

            if team1ID == team2ID:
                print(mapID)
                print("Log Names: " + team1log + " - " + team2log)
                print("DB Names: " + str(team1DB) + " - " + str(team2DB))
                print(str(team1ratio) + " - " + str(team2ratio))
                print(str(team1ID) + " - " + str(team2ID))
                print("----------------")
                file = './logs/Cleaned/Error/Round/'+mapID
                with open(file, 'w+') as txtFile:
                    txtFile.write(headers[0])
                    txtFile.write(headers[1])
                    for line in reversed(output):
                        txtFile.write(json.dumps(line) + "\n")
                    return


            cur = conn.cursor()
            cur.execute("""SELECT Player.name as name, Player.playerid as id
                                FROM players Player
                                    INNER JOIN player_maps PM ON PM.playerid = Player.playerid
                                    INNER JOIN maps M ON M.mapid = PM.mapid
                                    WHERE PM.playerid = Player.playerid and M.mapid = %s
                        """, (mapID[:-4],))
            
            playersDB = cur.fetchmany(10)
            team1log = json.loads(headers[0])["Players"]
            team2log = json.loads(headers[1])["Players"]
            errorOutput = ""
            playersConvert = {}
            alternateIds = {"roejJ":"roej", "terminacoR": "acoR", "Hani": "teses", "Feitan": "yekindar", "HANMA": "jame", "fq": "forester", "kio": "kioshima", "k1o": "kioshima", "superk1o": "kioshima", "superk1o 26": "kioshima", "k1o[C]": "kioshima", "nahtE": "Ethan", "CLG nahtE": "Ethan", "NahteCS": "Ethan", "Cloud9 brax-iwnl-": "swag", "mouz|chrisJ": "chrisJ", "Cutler": "reltuc", "hhh":"sanji", "nathanS":"nbk", "fermonster":"fer", "Ackerman":"s1mple", "Mikasa":"Perfecto" }
            team1IDS = []
            team2IDS = []

            for player in playersDB:
                highestRatio = 0
                dbName = player[0]
                team = 0
                for playerLog in team1log:
                    OGLogName = playerLog
                    if playerLog in alternateIds.keys():
                        playerLog = alternateIds[playerLog]
                    for word in excessWordsPlayers:
                        if word in playerLog:
                            playerLog = playerLog.replace(word, "")
                        if word in  playerLog:
                            playerLog = playerLog.replace(word, "")
                    levRatio = Levenshtein.ratio(playerLog.lower(), dbName)
                    if levRatio > highestRatio or dbName in playerLog:
                        highestRatio = levRatio
                        mostLikelyID = player[1]
                        mostLikelyName = OGLogName
                        team = 1
                for playerLog in team2log:
                    OGLogName = playerLog
                    if playerLog in alternateIds.keys():
                        playerLog = alternateIds[playerLog]
                    for word in excessWordsPlayers:
                        if word in playerLog:
                            playerLog = playerLog.replace(word, "")
                        if word in  playerLog:
                            playerLog = playerLog.replace(word, "")
                    levRatio = Levenshtein.ratio(playerLog.lower(), dbName)
                    if levRatio > highestRatio or dbName in playerLog:
                        highestRatio = levRatio
                        mostLikelyID = player[1]
                        mostLikelyName = OGLogName
                        team = 2
                if team == 1:
                    team1IDS.append(mostLikelyID)
                if team == 2:
                    team2IDS.append(mostLikelyID)
                errorOutput += "(" + dbName + " = " + mostLikelyName+ " = "  + str(mostLikelyID) +") "
                playersConvert[mostLikelyName] = mostLikelyID

            OGrounds = roundOutput.copy()      
            file = './logs/Cleaned/RoundLogs/'+mapID
            with open(file, 'w+') as txtFile:
                
                txtFile.write(headers[0])
                txtFile.write(headers[1])
                try:
                    for line in reversed(roundOutput):
                        line["mapid"] = mapID[:-4]
                        txtFile.write(json.dumps(line) + "\n")
                        if "WinnerScore" in line:
                            if int(line["LoserAlive"]) == 0:
                                line["LoserSaved"] = 0
                            line["Winner"] = teamsConvert[line["Winner"].lower()]
                            line["Loser"] = teamsConvert[line["Loser"].lower()]
                            if line["WinnerSide"] == 'CT':
                                line["CT"] = line["Winner"]
                                line["T"] = line["Loser"]
                            else:
                                line["T"] = line["Winner"]
                                line["CT"] = line["Loser"]


                            
                            CTstartMoney = 0
                            TstartMoney = 0


                            cur = conn.cursor()
                            cur.execute("""SELECT COALESCE(CASE WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R2.winneralive ELSE R2.loseralive END WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R2.loseralive ELSE R2.winneralive END END, 0) from rounds R inner join maps M on M.mapid = R.mapid left join rounds R2 on R2.mapid = R.mapid and R2.round = R.round-1 where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            CTstartAliveLast = cur.fetchone()[0]

                            cur = conn.cursor()
                            cur.execute("""SELECT COALESCE(CASE WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R2.winneralive ELSE R2.loseralive END WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R2.loseralive ELSE R2.winneralive END END, 0) from rounds R inner join maps M on M.mapid = R.mapid left join rounds R2 on R2.mapid = R.mapid and R2.round = R.round-1 where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            TstartAliveLast = cur.fetchone()[0]


                            cur = conn.cursor()
                            cur.execute("""SELECT CASE WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R.winnerstreak ELSE R.loserstreak END WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R.loserstreak ELSE R.winnerstreak END END from rounds R inner join maps M on M.mapid = R.mapid where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            CTstartStreak = cur.fetchone()[0]

                            cur = conn.cursor()
                            cur.execute("""SELECT CASE WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R.winnerstreak ELSE R.loserstreak END WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R.loserstreak ELSE R.winnerstreak END END from rounds R inner join maps M on M.mapid = R.mapid where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            TstartStreak = cur.fetchone()[0]

                            cur = conn.cursor()
                            cur.execute("""SELECT CASE WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R.winnerscore-1 ELSE R.loserscore END WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R.loserscore ELSE R.winnerscore-1 END END from rounds R inner join maps M on M.mapid = R.mapid where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            CTstartScore = cur.fetchone()[0]

                            cur = conn.cursor()
                            cur.execute("""SELECT CASE WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R.winnerscore-1 ELSE R.loserscore END WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R.loserscore ELSE R.winnerscore-1 END END from rounds R inner join maps M on M.mapid = R.mapid where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            TstartScore = cur.fetchone()[0]

                            cur = conn.cursor()
                            cur.execute("""SELECT CASE WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R.winnermoney ELSE R.losermoney END WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R.losermoney ELSE R.winnermoney END END from rounds R inner join maps M on M.mapid = R.mapid where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            CTstartMoney += int(cur.fetchone()[0])

                            cur = conn.cursor()
                            cur.execute("""SELECT CASE WHEN M.winnerstart='t' THEN CASE WHEN R.winner=M.winnerid THEN R.winnermoney ELSE R.losermoney END WHEN M.winnerstart='ct' THEN CASE WHEN R.winner=M.winnerid THEN R.losermoney ELSE R.winnermoney END END from rounds R inner join maps M on M.mapid = R.mapid where R.mapid = %s and R.round = %s""", (line["mapid"], line["Round"]))
                            TstartMoney += int(cur.fetchone()[0])

                            cur = conn.cursor()
                            cur.execute("""SELECT mapname from maps where mapid = %s""", (line["mapid"],))
                            mapname = cur.fetchone()[0]



                            #"tick": int(line["Tick"]), "CTstartMoney": int(CTstartMoney), "TstartMoney": int(TstartMoney), "CTstartStreak": int(CTstartStreak), "TstartStreak": int(TstartStreak)
                            MapValues = {"mapname": mapname, "Round": int(line["Round"]), "CTstartScore": CTstartScore, "TstartScore": TstartScore, "CTstartMoney": int(CTstartMoney), "TstartMoney": int(TstartMoney), "CTstartStreak": int(CTstartStreak), "TstartStreak": int(TstartStreak), "CTstartAliveLast": int(CTstartAliveLast), "TstartAliveLast": int(TstartAliveLast)}
                            line["CTprobabilityMap"], err = predictMap(MapValues, gbrM)
                            #line["CTprobabilityMap"] = 0.5
                            line["TprobabilityMap"] = 1-line["CTprobabilityMap"]








                            columns = line.keys()
                            values = [line[column] for column in columns]
                            try:
                                cur.execute("insert into rounds (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values))) 
                                conn.commit()
                            except (psycopg2.errors.ForeignKeyViolation, psycopg2.errors.InFailedSqlTransaction):
                                conn.rollback()
                                continue
                                
                except KeyError as e:
                    conn.rollback()
                    print(mapID + 'Round Error')
                    print(teamsConvert)
                    print(line)
                    file = './logs/Cleaned/Error/Round/'+mapID
                    txtFile.close()
                    with open(file, 'w+') as txtFile:
                        txtFile.write(headers[0])
                        txtFile.write(headers[1])
                        for line in reversed(OGrounds):
                            txtFile.write(json.dumps(line) + "\n")
                    return
        
            file = './logs/Cleaned/WinProb/'+mapID
            with open(file, 'w+') as txtFile:
                txtFile.write(headers[0])
                txtFile.write(headers[1])
                try:
                    prevRound = 1
                    prevProb = 0.5
                    prevProbMap = 0.5
                    #prevHP = 1000
                    roundProbOutput = roundProbOutput[1:]
                    for line in roundProbOutput:

                        del line["BombPlanted"]
                        
                        line["mapid"] = mapID[:-4]
                        txtFile.write(json.dumps(line) + "\n")
                        line["CT"] = teamsConvert[line["CT"].lower()]
                        line["T"] = teamsConvert[line["T"].lower()]
                        A = False
                        B = False
                        planted = False
                        if line["PlantSite"] == 'A':
                            A = True
                            planted = True
                        if line["PlantSite"] == 'B':
                            B = True
                            planted = True
                        if line["Attacker"] != "None":
                            line["Attacker"] = playersConvert[line["Attacker"]]
                        else:
                            line["Attacker"] = -1
                        if line["Victim"] != "None":
                            line["Victim"] = playersConvert[line["Victim"]]
                        else:
                            line["Victim"] = -1
                        
                        RoundValues = {"tick": int(line["Tick"]), "ctalive": int(line["CTalive"]), "talive": int(line["Talive"]), "ctdista": float(line["CTdistA"]),"tdista": float(line["TdistA"]),"ctdistb": float(line["CTdistB"]),"tdistb": float(line["TdistB"]), "ctvalue": int(line["CTvalue"]), "tvalue": int(line["Tvalue"]), "cthp": int(line["CThp"]), "thp": int(line["Thp"]), "planteda": A, "plantedb": B, "bombplanted": planted, "timesinceplant": int(line["TimeSincePlant"])}
                        line["CTprobability"], err = predictRound(RoundValues, gbrR)
                        #line["CTprobability"] = 0.5
                        line["Tprobability"] = 1-line["CTprobability"]

                        # if line["Attacker"] != -1 and line["Victim"] != -1:
                        #     print(prevRound)
                        #     print(int(line["Round"]))
                        #     print(line["Attacker"])
                        #     print(line["Victim"])
                        #     print(line["CTprobability"])
                        #     print(prevProb)
                        #     print(prevProb - line["CTprobability"])
                        #     print('------------------------------')
                        if prevRound == int(line["Round"]):
                            if line["Attacker"] != -1 and line["Victim"] != -1:
                                    line["ProbabilityChange"] = prevProb - line["CTprobability"] 
                            else:
                                line["ProbabilityChange"] = 0
                        else:
                            line["ProbabilityChange"] = 0.5
                        
                        columns = line.keys()
                        values = [line[column] for column in columns]
                        cur.execute("insert into roundstates (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values))) 
                        conn.commit()
                        prevRound = int(line["Round"])
                        prevProb = line["CTprobability"]
                        #prevHP = int(line["CThp"]) + int(line["Thp"])
                except KeyError as e:
                    print(print(playersConvert))
                    print(e)
                    print('KeyError')
                    file = './logs/Cleaned/Error/WinProb/'+mapID
                    txtFile.close()
                    conn.rollback()
                    with open(file, 'w+') as txtFile:
                        txtFile.write(headers[0])
                        txtFile.write(headers[1])
                        for line in reversed(roundProbOutput):
                            txtFile.write(json.dumps(line) + "\n")
                    return
                except psycopg2.errors.ForeignKeyViolation:
                    print(mapID)
                    conn.rollback()
                    pass
                except TypeError as e:
                        print(e)
                        print(mapID)
                        print(line)
                        pass
            


            OGkills = output.copy()
            file = './logs/Cleaned/KillLogs/'+mapID
            with open(file, 'w+') as txtFile:
                delete = False
                txtFile.write(headers[0])
                txtFile.write(headers[1])
                try:
                    for line in reversed(output):
                        line["mapid"] = mapID[:-4]
                        txtFile.write(json.dumps(line) + "\n")
                        if "Kill" in line :
                            if line["Kill"] != "(nil)":
                                line["Kill"].replace('(1)', "")
                                line["Death"].replace('(1)', "")
                                if 'ax1le' not in line["Kill"].lower():
                                    line["Kill"] = playersConvert[line["Kill"]]
                                    line
                                else:
                                    line["Kill"] = 16555
                                    line
                                if line["Kill"] in team1IDS:
                                    line["teamKill"] = team1ID
                                if line["Kill"] in team2IDS:
                                    line["teamKill"] = team2ID

                                if 'ax1le' not in line["Death"].lower():
                                    line["Death"] = playersConvert[line["Death"]]
                                    line
                                else:
                                    line["Death"] = 16555
                                    line = line
                                if line["Death"] in team1IDS:
                                    line["teamDeath"] = team1ID
                                if line["Death"] in team2IDS:
                                    line["teamDeath"] = team2ID

                                
                                KillValues = {"tick":line["Tick"],"death":line["Death"],"kill":line["Kill"],"round":line["Round"], "mapid":line["mapid"], "teamkill":line["teamKill"],"teammembersalive":int(line["teamMembersAlive"]), "opponentsalive":int(line["opponentsAlive"]), "distance":line["distance"], "weapon":line["Weapon"], "killerhealth":int(line["KillerHealth"]), "killerarmor":int(line["KillerArmor"]),  "killerhelmet":line["KillerHelmet"],  "killerflashduration":float(line["KillerFlashDuration"]), "killerxvel":float(line["KillerXvel"]), "killeryvel":float(line["KillerYvel"]), "killerzvel":float(line["KillerZvel"]), "victimweapon":line["VictimWeapon"], "victimhealth":int(line["VictimHealth"]), "victimarmor":int(line["VictimArmor"]),   "victimhelmet":line["VictimHelmet"],  "victimflashduration":float(line["VictimFlashDuration"]), "victimxvel":float(line["VictimXvel"]), "victimyvel":float(line["VictimYvel"]), "victimzvel":float(line["VictimZvel"]), "killerz":line["KillerZ"], "victimz":line["VictimZ"]}
                                #line["ExpectedKill"] = predictKill(KillValues, gbrK)
                                line["ExpectedKill"] = 0.5


                            else:
                                line["Kill"] = -1
                                line["teamKill"] = -1
                                line["ExpectedKill"] = -1
                                line["Death"] = playersConvert[line["Death"]]
                                if line["Death"] in team1IDS:
                                    line["teamDeath"] = team1ID
                                if line["Death"] in team2IDS:
                                    line["teamDeath"] = team2ID
                            columns = line.keys()
                            values = [line[column] for column in columns]        
                            cur.execute("insert into kills (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values)))     
                            conn.commit()
                        if "SavedValue" in line :
                            line["Save"].replace('(1)', "")
                            if 'ax1le' not in line["Save"].lower():
                                line["Save"] = playersConvert[line["Save"]]
                                line
                            else:
                                line["Save"] = 16555

                            columns = line.keys()
                            values = [line[column] for column in columns]
                            cur.execute("insert into saves (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values)))
                            conn.commit()
                except KeyError as e:
                    conn.rollback()
                    print(e)
                    delete = True
                    print(mapID)
                    print(line)
                    print(playersConvert)
                    fileErr = './logs/Cleaned/Error/Kill/'+mapID
                    txtFile.close()
                    with open(fileErr, 'w+') as txtFile:
                        txtFile.write(str(team1DB) + " - " + str(team2DB) +"\n")
                        txtFile.write(str(errorOutput) +"\n")
                        txtFile.write(str(playersDB) + "\n")
                        txtFile.write(headers[0])
                        txtFile.write(headers[1])
                        for lineOG in reversed(OGkills):
                            txtFile.write(json.dumps(lineOG) + "\n")
                    return

            if delete:
                os.remove(file)
            cur.close()     
            conn.commit()

            



    except UnicodeDecodeError:
        print(mapID)       
        conn.rollback()            

if __name__ == "__main__":
    main()

import os
import sys
import json
import math
import time
import Levenshtein
import psycopg2
import traceback
import connectDB

#Iterate through logs

def main(path, mapID):
    #mapID = '2356098-1.txt'

    #subnet = '1.3'
    subnet = '86.113'
    IP = 'csgo.cqtpbfnejnsi.us-east-2.rds.amazonaws.com'


    try:
        allLines = []
        file = path+mapID
        with open(file) as log:
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
                prevRoundLog = 0
                if notLiveRound:
                    if worldKillCounter == 10:
                        notLiveRound = False
                        worldKillCounter = 0
                    if "Kill" in line:
                        weapon = json.loads(line.replace("'", "").replace("\\", ""), strict=False )["Weapon"]
                        killer = json.loads(line.replace("'", "").replace("\\", ""), strict=False)["Kill"]
                        if weapon == "World":
                            worldKillCounter += 1
                            continue
                    if '"Save":' in line:
                        peopleAlive = int(json.loads(line.replace("'", "").replace("\\", ""), strict=False)["teamMembersAlive"]) + int(json.loads(line.replace("'", "").replace("\\", ""), strict=False)["opponentsAlive"])
                        if peopleAlive == 10:
                            worldKillCounter += 1
                            continue

                if line.startswith("Round: NOT_LIVE"):
                    notLiveRound = True
                elif "CTalive" in line and not firstKillFound:
                    outputLine = json.loads(line.replace("'", "").replace("\\", ""), strict=False)
                    if outputLine['Round'] == '1' and outputLine['CTalive'] == '5' and outputLine['Talive'] == '5' and int(outputLine['Tick']) < 10:
                        firstKillFound = True
                    roundProbOutput.append(outputLine)

                elif "Kill" in line and not firstKillFound:
                    #print(line)
                    line = json.loads(line.replace("'", "").replace("\\", ""), strict=False)
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
                    line = json.loads(line.replace("'", "").replace("\\", ""), strict=False)
                    output.append(line)
                elif "Players" in line:
                    headers.append(line.replace("'", "").replace("\\", ""))
                elif "WinnerScore" in line and not firstKillFound:
                    outputLine = json.loads(line.replace("'", "").replace("\\", ""), strict=False)
                    if outputLine['Winner'] != '' and outputLine['Loser'] != '':
                        roundOutput.append(outputLine)


                #elif str(line).startswith('Match: Game State Changed'):

            conn = connectDB.database_credentials()
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

            team1log = team1logOG = json.loads(headers[0], strict=False)["Team"].lower()
            team2log = team2logOG = json.loads(headers[1], strict=False)["Team"].lower()

            excessWordsTeams = ["esports", " esports", "parimatch", "gg.bet", "luk0il", ".", ".1xbet", "[not ready]", "[not ready] ", "[ready]", " gaming", "[READY] ", " Sportsbet", "Team "]
            excessWordsPlayers = ["NTC","Imp_", "North.", ".Parimatch", "_LigaStavok", "BIG ", "forZe-", "_x_","* HyperX","* twitch.tv","* EGB.com","* Hellcase.com","*66esports", "LG * ", "Virtus.pro G2A ", "luk0il", " x LUKOIL", "parimatch", " GG.BET", "*LBETç«žæŠ€", "-", "* Parimatch", "(1)", "^.^", "_ligastavok", " x parimatch", "flames*", "Flames*", "mrs - ", "big", "avangar", "cr4zy", "AVANGAR|", "G2|", "ENVYUS ", "-iwnl-", "Cloud9", "[C]", "[D]", "CLG ", "CLG", "Heroic", "mouz|", " Betway", "HEET"]
            for word in excessWordsTeams:
                if word.lower() in team1log.lower():
                    team1log = team1log.lower().replace(word.lower(), "")
                if word.lower() in  team2log.lower():
                    team2log = team2log.lower().replace(word.lower(), "")



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
            team1log = json.loads(headers[0], strict=False)["Players"]
            team2log = json.loads(headers[1], strict=False)["Players"]
            errorOutput = ""
            playersConvert = {}
            alternateIds = {"North.jt-": "JTMythic","roejJ":"roej", "terminacoR": "acoR", "Hani": "teses", "Feitan": "yekindar", "HANMA": "jame", "fq": "forester", "kio": "kioshima", "k1o": "kioshima", "superk1o": "kioshima", "superk1o 26": "kioshima", "k1o[C]": "kioshima", "nahtE": "Ethan", "CLG nahtE": "Ethan", "NahteCS": "Ethan", "Cloud9 brax-iwnl-": "swag", "mouz|chrisJ": "chrisJ", "Cutler": "reltuc", "hhh":"sanji", "nathanS":"nbk", "fermonster":"fer", "Ackerman":"s1mple", "Mikasa":"Perfecto" }
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
                        if word.lower() in playerLog.lower():
                            playerLog = playerLog.lower().replace(word.lower(), "")
                        if word.lower() in  playerLog.lower():
                            playerLog = playerLog.lower().replace(word.lower(), "")
                    levRatio = Levenshtein.ratio(playerLog.lower(), dbName)
                    if levRatio > highestRatio or dbName == playerLog.lower():
                        highestRatio = levRatio
                        mostLikelyID = player[1]
                        mostLikelyName = OGLogName
                        team = 1
                for playerLog in team2log:
                    OGLogName = playerLog
                    if playerLog in alternateIds.keys():
                        playerLog = alternateIds[playerLog]
                    for word in excessWordsPlayers:
                        if word.lower() in playerLog.lower():
                            playerLog = playerLog.lower().replace(word.lower(), "")
                        if word.lower() in  playerLog.lower():
                            playerLog = playerLog.lower().replace(word.lower(), "")
                    levRatio = Levenshtein.ratio(playerLog.lower(), dbName)
                    if levRatio > highestRatio or dbName == playerLog.lower():
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
                prevWinner = -1
                txtFile.write(headers[0])
                txtFile.write(headers[1])
                try:
                    for line in reversed(roundOutput):
                        if int(line["Round"]) == 0:
                            continue
                        line["mapid"] = mapID[:-4]
                        txtFile.write(json.dumps(line) + "\n")
                        if "WinnerScore" in line:
                            if int(line["LoserAlive"]) == 0:
                                line["LoserSaved"] = 0


                            line["Winner"] = teamsConvert[line["Winner"].lower()]
                            line["Loser"] = teamsConvert[line["Loser"].lower()]
                            if line["WinnerSide"] == 'ct':
                                line["CT"] = line["Winner"]
                                line["T"] = line["Loser"]
                            else:
                                line["T"] = line["Winner"]
                                line["CT"] = line["Loser"]
                            if line["Winner"] == line["Loser"]:
                                return
                            columns = line.keys()
                            values = [line[column] for column in columns]
                            while True:
                                try:
                                    cur.execute("insert into rounds (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values))) 
                                    break
                                except (psycopg2.errors.ForeignKeyViolation, psycopg2.errors.InFailedSqlTransaction) as e:
                                    print(e)
                                    conn.rollback()
                                    return
                                except psycopg2.OperationalError:
                                    time.sleep(60)
                                    print("Internet Timeout")
                                except Exception as e:
                                    print(e)
                                    print(mapID)
                                    break

                    conn.commit()
                                
                except KeyError as e:
                    print("KeyError")
                    conn.rollback()
                    print(e)
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
                except ValueError as e:
                    print("ValueError")
                    print(e)
                    print(mapID)
                    traceback.print_exc()


            file = './logs/Cleaned/WinProb/'+mapID

            with open(file, 'w+') as txtFile:
                txtFile.write(headers[0])
                txtFile.write(headers[1])
                try:
                    roundProbOutput = roundProbOutput[1:]
                    for line in roundProbOutput:
                        if int(line["Round"]) == 0:
                            continue
                        del line["BombPlanted"]
                        
                        line["mapid"] = mapID[:-4]
                        txtFile.write(json.dumps(line) + "\n")
                        line["CT"] = teamsConvert[line["CT"].lower()]
                        line["T"] = teamsConvert[line["T"].lower()]

                        if line["Attacker"] != "None":
                            line["Attacker"] = playersConvert[line["Attacker"]]
                        else:
                            line["Attacker"] = -1
                        if line["Victim"] != "None":
                            line["Victim"] = playersConvert[line["Victim"]]
                        else:
                            line["Victim"] = -1
    
                        columns = line.keys()
                        values = [line[column] for column in columns]
                        while True:
                            try:
                                #print(line["Round"] + '@' + line["Tick"] + '                            ', end="\r")
                                cur.execute("insert into roundstates (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values))) 
                                break
                            except psycopg2.OperationalError:
                                time.sleep(60)
                                print("Internet Timeout")
                                conn = connectDB.database_credentials()
                                cur = conn.cursor()
                    conn.commit()
                except KeyError as e:
                    print("KeyError")
                    print(playersConvert)
                    print(e)
                    print(line["mapid"])
                    print('KeyError')
                    traceback.print_exc()
                    file = './logs/Cleaned/Error/WinProb/'+mapID
                    txtFile.close()
                    conn.rollback()
                    with open(file, 'w+') as txtFile:
                        txtFile.write(headers[0])
                        txtFile.write(headers[1])
                        for line in reversed(roundProbOutput):
                            txtFile.write(json.dumps(line) + "\n")
                    return
                except psycopg2.errors.ForeignKeyViolation as e:
                    print("Foregin Key Violation")
                    print(e)
                    print(mapID)
                    conn.rollback()
                    pass
                except TypeError as e:
                        print("TypeError")
                        print(e)
                        print(mapID)
                        print(line)
                        traceback.print_exc()
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
                                line["Kill"] = playersConvert[line["Kill"]]

                                if line["Kill"] in team1IDS:
                                    line["teamKill"] = team1ID
                                if line["Kill"] in team2IDS:
                                    line["teamKill"] = team2ID


                                line["Death"] = playersConvert[line["Death"]]

                                if line["Death"] in team1IDS:
                                    line["teamDeath"] = team1ID
                                if line["Death"] in team2IDS:
                                    line["teamDeath"] = team2ID

                            else:
                                line["Kill"] = -1
                                line["teamKill"] = -1
                                line["Death"] = playersConvert[line["Death"]]
                                if line["Death"] in team1IDS:
                                    line["teamDeath"] = team1ID
                                if line["Death"] in team2IDS:
                                    line["teamDeath"] = team2ID
                            columns = line.keys()
                            values = [line[column] for column in columns]        
                            cur.execute("insert into kills (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values)))     
                        if "SavedValue" in line :
                            line["Save"].replace('(1)', "")
                            if 'ax1le' not in line["Save"].lower():
                                line["Save"] = playersConvert[line["Save"]]
                                line
                            else:
                                line["Save"] = 16555

                            columns = line.keys()
                            values = [line[column] for column in columns]
                            while True:
                                try:
                                    cur.execute("insert into saves (%s) values %s ON CONFLICT DO NOTHING", (psycopg2.extensions.AsIs(','.join(columns)), tuple(values)))
                                    break
                                except psycopg2.OperationalError:
                                    print(mapID)
                                    time.sleep(60)
                                    print("Internet Timeout")
                    conn.commit()
                except KeyError as e:
                    print("Key Error")
                    conn.rollback()
                    print(e)
                    delete = True
                    print(mapID)
                    print(line)
                    print(playersConvert)
                    traceback.print_exc()
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
        print("Unicode Error")
        print(mapID)        

    except json.JSONDecodeError as e:
        print("JSON Decode Error")
        print(mapID)   
        print(e)
        print(line)  

    except Exception as e:
        print(e)
        print(mapID)     

            
if __name__ == "__main__":
    main(sys.argv[1])

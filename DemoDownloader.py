import csv, os, time, requests, patoolib, connectDB
from tqdm import tqdm 
import datetime
import psycopg2
import DemoParser




#Get already downloaded demos (Make sure .dem file count is >= len(mapScores))
#Get demo links and ID's



def main():
    global URL

    URL = 'https://www.hltv.org/download/demo/'
    alreadyDownloadedIDs = []


    while True:
        oldLinks = []
        try:
            getAlreadyDownloaded(alreadyDownloadedIDs)

            demoLinks = getDemoLinks(alreadyDownloadedIDs)
            if demoLinks:
                downloadDemos(demoLinks, alreadyDownloadedIDs)
            if oldLinks != demoLinks:
                DemoParser.main([0,1], 1)
            oldLinks = demoLinks
            time.sleep(300)
        except Exception as e:
            print(e)
            time.sleep(300)
            continue


def getAlreadyDownloaded(alreadyDownloadedIDs):
    for demo in os.listdir("./demos/"):
        alreadyDownloadedIDs.append(int(demo[0:-4]))

def getDemoLinks(alreadyDownloadedIDs):
    sql = """
            SELECT demoid from matches 
        """
    conn = None
    try:
        conn = connectDB.database_credentials()
        cur = conn.cursor()
        cur.execute(sql)
        matches = cur.fetchall()
        demoLinks = []
        if matches is not None:
            demoLinks = [int(match[0]) for match in matches if int(match[0]) not in alreadyDownloadedIDs]
        cur.close()
        return(demoLinks)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def downloadDemos(demoLinks, alreadyDownloadedIDs):
    print(str(len(demoLinks)) + " demo links found", end='\r')
    start = match_start = time.time()
    length = len(demoLinks)
    matchCount = 0
    i = 0
    for demo in demoLinks:
        demo = str(demo)
        while True:
            try:
                if os.path.isfile("./PAUSE"):
                        input("Remove PAUSE and hit ENTER to continue")
                r = requests.get(URL + demo, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0'}, allow_redirects=True, stream=True)
                with tqdm.wrapattr(open('./demos/'+demo+".rar", "wb"), "write", miniters=1, total=int(r.headers.get('content-length', 0)), desc=demo) as fout:
                    for chunk in r.iter_content(chunk_size=4096):
                        fout.write(chunk)
                fout.close()
            except requests.exceptions.ConnectionError as e:
                print("Connection Error, Retrying in 30 seconds")
                time.sleep(30)
                continue
            break
        i += 1
        alreadyDownloadedIDs.append(demo)
        matchCount+=1
        print("Match "+str(matchCount)+'/'+str(length)+" Complete")
        end = time.time()
        print("Match Time: "+str(datetime.timedelta(seconds=(end - match_start))))
        time.sleep(10)
        match_start = time.time()
    #print("Run Time:   "+str(datetime.timedelta(seconds=(time.time() - start))), end='\r')
    demoLinks = []
    return

if __name__ == "__main__":
    main()

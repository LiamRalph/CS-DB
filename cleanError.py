import os
import cleanLogs
for file in os.listdir("./ErrorLogs/"):
    cleanLogs.main("./ErrorLogs/", file)
    os.remove("./ErrorLogs/"+file)

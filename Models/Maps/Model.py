from sklearn import datasets
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.decomposition import PCA
from sklearn.metrics import mean_squared_error
import pandas as pd
from joblib import dump, load
import numpy as np
import matplotlib.pyplot as plt
from sklearn.inspection import permutation_importance
import psycopg2
from xgboost import XGBClassifier
from xgboost import plot_importance
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV
import math, random
import statistics
import MapQueries
from sklearn.feature_selection import SelectFromModel
from numpy import sort
import warnings
warnings.simplefilter("ignore", UserWarning)
import sys, os
import csv
import KFoldTesting

counter = 0
dfList = []
loadCSV = int(sys.argv[1])
reBackTest = int(sys.argv[2])
mapIDsLoaded = []


matches = MapQueries.getMatches()
if loadCSV == 0:
    for match in matches:
        team1 = match[0]
        team2 = match[1]
        date = match[2]
        mapCount= match[3]
        mapid = match[4]
        matchid = match[5]
        counter += 1
        #mapSides = Queries.getMapNamesWR(date)
        if mapid in mapIDsLoaded:
            continue

        mapsPlayed = MapQueries.getMaps(matchid)
        for mapPlayed in mapsPlayed:
            mapid = mapPlayed[0]
            mapname = mapPlayed[1]
            rounds = MapQueries.getData(mapid)
            if isinstance(rounds, int):
                continue

            mapIDsLoaded.append(mapid)
            for roundstate in rounds:
                dfList.append(roundstate)
            
        
        # elif isinstance(team1data, int):
        #     print()
        #     print(team1data)
        # else:
        #     print()
        #     print(team2data)


        print("Matches Tried " + str(counter) + "/"+ str(len(matches)) + " ------ "+ str(len(dfList)) + " Rounds States In Data Set", end ="\r")
            


    
    keys = dfList[0].keys()
    with open('./data/Rounds.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dfList)
    print()
del dfList
df =  pd.read_csv('./data/Rounds.csv', dtype={'ctstart_score':int,'tstart_score':int,'ctstart_streak':int,'tstart_streak':int,'Vertigo':int,'Ancient':int,'Cobblestone':int,'Inferno':int,'Tuscan':int,'Mirage':int,'Anubis':int,'Train':int,'Overpass':int,'Dust2':int,'Cache':int,'Nuke':int})




#Target = df.winner
#df.drop(columns=['winner'], inplace=True)

if reBackTest == 0:
    MapQueries.resetTable()
    Preds = KFoldTesting.KFold(df,25)
    Preds.to_csv('./data/Predictions.csv', index=False)
    mapids = Preds.mapid.unique()
    counter = 0
    for mapid in mapids:
        counter += 1
        Rounds = Preds.loc[Preds['mapid'] == mapid].sort_values(by=['roundno', 'tick'], ascending=False)
        prevProb = Preds.iloc[0]['winner']
        preds = []
        for k, RoundNo in Rounds.iterrows():
            probchangeCT = prevProb-RoundNo['prediction']
            preds.append((mapid, RoundNo['roundno'], RoundNo['tick'], RoundNo['prediction'], probchangeCT))
            prevProb = RoundNo['prediction']
        print("Matches Tried " + str(counter) + "/"+ str(len(mapids)), end ="\r")
        MapQueries.addPred(preds)










Target = df.winner
df.drop(columns=['winner', 'mapid'], inplace=True)
X_train, X_test, y_train, y_test = train_test_split(df, Target, test_size=0.2)
XGB = XGBClassifier()
XGB.fit(X_train, y_train)



#
# Print Coefficient of determination R^2
#

#print(XGB.best_params_)
mse = mean_squared_error(y_test, XGB.predict(X_test))
print("The mean squared error (MSE) on test set: {:.4f}".format(mse))
y_pred = XGB.predict(X_test)
predictions = [value for value in y_pred]
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))

#del X_train, y_train


dump(XGB,'MapModelXGB') 

# plot feature importance
plot_importance(XGB)
plt.show()





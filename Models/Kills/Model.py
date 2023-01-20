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
import KillQueries
from sklearn.feature_selection import SelectFromModel
from numpy import sort
from sklearn.model_selection import StratifiedKFold
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

matches = KillQueries.getMatches()
if loadCSV == 0:
    allMaps = KillQueries.getMapNames()
    allWeps = KillQueries.getWeaponNames()
    for match in matches:
        team1 = match[0]
        team2 = match[1]
        date = match[2]
        mapCount= match[3]
        mapid = match[4]
        matchid = match[5]
        counter += 1
        if matchid in mapIDsLoaded:
            continue

        mapsPlayed = KillQueries.getMaps(matchid)
        for mapPlayed in mapsPlayed:
            mapid = mapPlayed[0]
            mapname = mapPlayed[1]
            #mapid = '2350385-3'
            kills = KillQueries.getData(mapid)
            
            if isinstance(kills, int):
                continue

            mapIDsLoaded.append(matchid)
            for kill in kills:
                for mapname in allMaps:
                    if mapname == kill["mapname"]:
                        kill[mapname] = 1
                    else:
                        kill[mapname] = 0
                for weapon in allWeps:
                    if weapon == kill["ctweapon"]:
                        kill['ctwep_'+weapon] = 1
                    else:
                        kill['ctwep_'+weapon] = 0
                    if weapon == kill["tweapon"]:
                        kill['twep_'+weapon] = 1
                    else:
                        kill['twep_'+weapon] = 0
                
                del kill["ctweapon"], kill["tweapon"], kill["mapname"]
                #print(kill)
                dfList.append(kill)
            

        print("Matches Tried " + str(counter) + "/"+ str(len(matches)) + " ------ "+ str(len(dfList)) + " Kills In Data Set", end ="\r")
            


    
    keys = dfList[0].keys()
    with open('./data/Kills.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dfList)
    print()
del dfList
df =  pd.read_csv('./data/Kills.csv')


if reBackTest == 0:
    KillQueries.resetTable()
    Preds = KFoldTesting.KFold(df,25)
    Preds.to_csv('./data/Predictions.csv', index=False)
    mapids = Preds.mapid.unique()
    counter = 0
    for mapid in mapids:
        counter += 1
        Kills = Preds.loc[Preds['mapid'] == mapid].sort_values(by=['roundno', 'tick'], ascending=False)
        preds = []
        for k, Kill in Kills.iterrows():
            if Kill['winner'] == 1:
                preds.append((mapid, Kill['roundno'], Kill['tick'], Kill['kill'], Kill['death'], Kill['prediction']))
            else:
                preds.append((mapid, Kill['roundno'], Kill['tick'], Kill['kill'], Kill['death'], 1-Kill['prediction']))
        print("Matches Tried " + str(counter) + "/"+ str(len(mapids)), end ="\r")
        KillQueries.addPred(preds)





Target = df.winner


df.drop(columns=['winner', 'mapid', 'kill', 'death', 'roundno', 'tick'], inplace=True)

X_train, X_test, y_train, y_test = train_test_split(df, Target, test_size=0.2)

#
# Standardize the dataset
#
#sc = StandardScaler()
X_train = pd.DataFrame(X_train, columns = df.columns)
X_test  = pd.DataFrame(X_test, columns = df.columns)

params = {
#     'n_estimators':100,
#     'max_depth':4,
#     'learning_rate': 0.05,
#     'subsample'    : 0.5

}

XGB = XGBClassifier(objective='binary:logistic')
XGB.fit(X_train, y_train)




# XGB = XGBClassifier()
# test_params = {
   #'learning_rate': [0.01,0.025,0.05,0.1],
    # 'subsample'    : [0.9,0.5,0.2,0.1],
    #'n_estimators' : [50,100,500,1000],
    #'max_depth'    : [2,4,6,8,10]
# }

# XGB = GridSearchCV(estimator = XGB, param_grid = test_params, verbose = 1, cv = 3, n_jobs = -1)
# XGB.fit(X_train, y_train)
# print(XGB.best_params_)






#
# Print Coefficient of determination R^2
#
#print(XGB.best_params_)
mse = mean_squared_error(y_test, XGB.predict(X_test))
print("The mean squared error (MSE) on test set: {:.4f}".format(mse))


del X_train, y_train


dump(XGB,'KillModelXGB') 

# plot feature importance
plot_importance(XGB)
plt.show()



y_pred = XGB.predict(X_test)
predictions = [value for value in y_pred]
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))




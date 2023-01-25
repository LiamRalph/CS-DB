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
import RoundQueries
from sklearn.feature_selection import SelectFromModel
from numpy import sort
from sklearn.model_selection import StratifiedKFold
import warnings
warnings.simplefilter("ignore", UserWarning)
import sys, os
import csv
import KFoldTesting

counter = 0
rsCount = 0
loadCSV = int(sys.argv[1])
reBackTest = int(sys.argv[2])
mapIDsLoaded = []
dfList = []
matches = RoundQueries.getMatches()
if loadCSV == 0:
    
    allMaps = RoundQueries.getMapNames()
    for match in matches:
        
        counter += 1
        mapid = match[4]
        if mapid in mapIDsLoaded:
            continue
        mapIDsLoaded.append(mapid)
        for roundNo in range(1, match[6]+1):
            rounds = RoundQueries.getData(mapid,roundNo)
            if isinstance(rounds, int):
                continue
            rsCount += len(rounds)
            for roundstate in rounds:
                roundstate['mapid'] = mapid
                roundstate['roundno'] = roundNo
                for mapname in allMaps:
                    if mapname == roundstate["mapname"]:
                        roundstate[mapname] = 1
                    else:
                        roundstate[mapname] = 0
                del(roundstate["mapname"])
                dfList.append(roundstate)



        print("Matches Tried " + str(counter) + "/"+ str(len(matches)) + " ------ "+ str(rsCount) + " Rounds States In Data Set", end ="\r")
        keys = dfList[0].keys()
        if counter == 1:
            with open('./data/Rounds.csv', 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
            output_file.close()
        elif counter % 1000 == 0:
            with open('./data/Rounds.csv', 'a', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writerows(dfList)
            dfList = []

    with open('./data/Rounds.csv', 'a', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writerows(dfList)
    print()    
del dfList





if reBackTest == 0:
    RoundQueries.resetTable()
    KFoldTesting.KFold(25)
    Preds =  pd.read_csv('./data/Predictions.csv')
    mapids = Preds.mapid.unique().tolist()
    counter = 0
    for mapid in mapids:
        preds = []
        Rounds = Preds.loc[Preds['mapid'] == mapid].sort_values(by=['roundno', 'tick'], ascending=False).drop_duplicates(subset=['mapid', 'roundno', 'tick'])
        roundNums = Rounds.roundno.unique().tolist()
        counter += 1
        for RoundNo in roundNums:
            RoundData = Rounds.loc[Rounds['roundno'] == RoundNo]
            prevProb = RoundData.iloc[0]['winner']
            for k, tick in RoundData.iterrows():
                probchangeCT = prevProb-tick['prediction']
                preds.append((mapid, RoundNo, tick['tick'], tick['prediction'], probchangeCT))
                prevProb = tick['prediction']

        print("Matches Tried " + str(counter) + "/"+ str(len(mapids)), end ="\r")
        RoundQueries.addPred(preds)



df =  pd.read_csv('./data/Rounds.csv', dtype={'ctalive':int,'talive':int,'roundno':int,'Vertigo':int,'Ancient':int,'Cobblestone':int,'Inferno':int,'Tuscan':int,'Mirage':int,'Anubis':int,'Train':int,'Overpass':int,'Dust2':int,'Cache':int,'Nuke':int})
Target = df.winner
df.drop(columns=['winner', 'roundno', 'mapid'], inplace=True)

X_train, X_test, y_train, y_test = train_test_split(df, Target, test_size=0.2)

#
# Standardize the dataset
#
#sc = StandardScaler()
X_train = pd.DataFrame(X_train, columns = df.columns)
X_test  = pd.DataFrame(X_test, columns = df.columns)


params = {
# #     'n_estimators':100,
# #     'max_depth':4,
# #     'learning_rate': 0.05,
# #     'subsample'    : 0.5

}
XGB = XGBClassifier(**params)
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


dump(XGB,'RoundModelXGB') 

# plot feature importance
plot_importance(XGB)
plt.show()

del X_train, y_train


y_pred = XGB.predict(X_test)
predictions = [value for value in y_pred]
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))




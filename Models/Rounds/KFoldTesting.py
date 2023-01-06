import numpy as np
import pandas as pd
from xgboost import XGBClassifier
import csv
def KFold(splitCount):
    df =  pd.read_csv('./data/Rounds.csv', dtype={'ctalive':int,'talive':int,'roundno':int,'Vertigo':int,'Ancient':int,'Cobblestone':int,'Inferno':int,'Tuscan':int,'Mirage':int,'Anubis':int,'Train':int,'Overpass':int,'Dust2':int,'Cache':int,'Nuke':int})
    splits = np.array_split(df, splitCount)
    del df
    splitNo = 0
    for split in splits:
        
        test = split
        train = pd.DataFrame()


        trainSplitNo = 0
        for trainSplit in splits:
            if trainSplitNo != splitNo:
                train = train.append(trainSplit, ignore_index=True)
            trainSplitNo += 1
    







        Preds = KFoldTesting(train, test).to_dict('records')
        keys = Preds[0].keys()
        if splitNo == 1:
            with open('./data/Predictions.csv', 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(Preds)
            Preds = []
        else:
            with open('./data/Predictions.csv', 'a', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writerows(Preds)
            Preds = []

        
        splitNo += 1
        print("Splits Done " + str(splitNo) + "/"+ str(splitCount), end ="\r")
    return Preds

def KFoldTesting(train, test):
    Pred = pd.DataFrame()
    TargetTrain = train.winner
    TargetTest = test.winner
    mapids = test.mapid
    roundno = test.roundno
    tick = test.tick
    train.drop(columns=['winner', 'mapid', 'roundno', 'prediction'], inplace=True, errors='ignore')
    test.drop(columns=['winner','mapid', 'roundno'], inplace=True, errors='ignore')
    XGB = XGBClassifier()
    XGB.fit(train, TargetTrain)
    y_pred = XGB.predict_proba(test)
    predictions = [value[1] for value in y_pred]
    Pred['mapid'] = mapids
    Pred['roundno'] = roundno
    Pred['tick'] = tick
    Pred['prediction'] = predictions
    Pred['winner'] = TargetTrain
    train['winner'] = TargetTrain
    test['winner'] = TargetTest
    return(Pred)
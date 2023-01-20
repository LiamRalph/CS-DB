import numpy as np
import pandas as pd
from xgboost import XGBClassifier

def KFold(df, splitCount):
    splits = np.array_split(df, splitCount)
    Preds = pd.DataFrame()
    splitNo = 0
    for split in splits:
        
        test = split


        train = pd.DataFrame()
        trainSplitNo = 0
        for trainSplit in splits:
            if trainSplitNo != splitNo:
                train = train.append(trainSplit.copy(), ignore_index=True)
            trainSplitNo += 1
        
        PredsFold = KFoldTesting(train.copy(), test.copy())
        Preds = Preds.append(PredsFold)
        print("Splits Done " + str(splitNo) + "/"+ str(splitCount), end ="\r")
        splitNo += 1
    return Preds

def KFoldTesting(train, test):

    Target = train.winner
    mapids = test.mapid
    train.drop(columns=['winner','mapid'], inplace=True)
    test.drop(columns=['winner','mapid'], inplace=True)
    XGB = XGBClassifier()
    XGB.fit(train, Target)
    y_pred = XGB.predict_proba(test)
    print(XGB.classes_)
    predictions = [value[1] for value in y_pred]
    test['prediction'] = predictions
    test['mapid'] = mapids
    test['winner'] = Target
    return(test)
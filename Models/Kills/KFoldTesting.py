import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import mean_squared_error

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
        splitNo += 1
        print("Splits Done " + str(splitNo) + "/"+ str(splitCount), end ="\r")
        
    return Preds

def KFoldTesting(train, test):
    trainTraget = train.winner
    testTarget = test.winner
    mapids = test.mapid
    kill = test.kill
    death = test.death
    roundNo = test.roundno
    tick = test.tick
    train.drop(columns=['winner','mapid', 'kill', 'death', 'roundno', 'tick'], inplace=True)
    test.drop(columns=['winner','mapid', 'kill', 'death', 'roundno', 'tick'], inplace=True)
    XGB = XGBClassifier(objective='binary:logistic')
    XGB.fit(train, trainTraget)
    mse = mean_squared_error(testTarget, XGB.predict(test))
    print("The mean squared error (MSE) on test set: {:.4f}".format(mse))
    y_pred = XGB.predict_proba(test)
    predictions = [value[1] for value in y_pred]
    
    Pred = pd.DataFrame()
    Pred['mapid'] = mapids
    Pred['roundno'] = roundNo
    Pred['tick'] = tick
    Pred['kill'] = kill
    Pred['death'] = death
    Pred['winner'] = testTarget
    Pred['prediction'] = predictions
    return(Pred)
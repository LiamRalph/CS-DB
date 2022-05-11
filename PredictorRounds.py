from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
import pandas as pd
from joblib import dump, load
import numpy as np
import matplotlib.pyplot as plt
from sklearn.inspection import permutation_importance
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
from scipy.special import softmax


def predictRound(RoundValues, gbr):
    df = pd.DataFrame.from_dict([RoundValues])
    ExRoundCT = gbr.predict_proba(df)
    return float(ExRoundCT[0][1]), 0



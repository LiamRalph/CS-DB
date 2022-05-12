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
from OneHotMaps import OHE


def predictMap(MapValues, gbr):

    df = pd.DataFrame.from_dict([MapValues])
    df = OHE(df)
    ExMapCT = gbr.predict_proba(df)
    return float(ExMapCT[0][1]), 0



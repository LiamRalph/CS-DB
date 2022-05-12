from sklearn.preprocessing import OneHotEncoder
import pandas as pd
import numpy as np


def OHE(df):
 
    df = pd.get_dummies(df, columns = ['mapname'], dtype=np.int8)

    maps = ['mapname_Inferno', 'mapname_Dust2', 'mapname_Mirage', 'mapname_Overpass', 'mapname_Nuke', 'mapname_Ancient', 'mapname_Vertigo', 'mapname_Cobblestone', 'mapname_Tuscan', 'mapname_Train', 'mapname_Cache']
   
    for map in maps:
        if map not in df.columns:
            df[map] = 0
    #return df
    return df
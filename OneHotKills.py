from sklearn.preprocessing import OneHotEncoder
import pandas as pd



def OHE(df):
    df = pd.get_dummies(df, columns = ['ctweapon', 'tweapon'])
    df['CThelmet'] = df['CThelmet'].map({'false': 0, 'true': 1})
    df['Thelmet'] = df['Thelmet'].map({'false': 0, 'true': 1})
    weapons = ['tweapon_C4', 'ctweapon_C4', 'ctweapon_AK-47',	'ctweapon_AUG',	'ctweapon_AWP',	'ctweapon_CZ75 Auto',	'ctweapon_Desert Eagle',	'ctweapon_Dual Berettas',	'ctweapon_FAMAS',	'ctweapon_Five-SeveN',	'ctweapon_G3SG1',	'ctweapon_Galil AR',	'ctweapon_Glock-18',	'ctweapon_Knife',	'ctweapon_M249',	'ctweapon_M4A1',	'ctweapon_M4A4',	'ctweapon_MAC-10',	'ctweapon_MAG-7',	'ctweapon_MP5-SD',	'ctweapon_MP7',	'ctweapon_MP9',	'ctweapon_Negev',	'ctweapon_Nova',	'ctweapon_P2000',	'ctweapon_P90',	'ctweapon_PP-Bizon',	'ctweapon_R8 Revolver',	'ctweapon_SCAR-20',	'ctweapon_SG 553',	'ctweapon_SSG 08',	'ctweapon_Sawed-Off',	'ctweapon_Tec-9',	'ctweapon_UMP-45',	'ctweapon_USP-S',	'ctweapon_XM1014',	'ctweapon_Zeus x27','ctweapon_P250',	'tweapon_AK-47',	'tweapon_AUG',	'tweapon_AWP',	'tweapon_CZ75 Auto',	'tweapon_Desert Eagle',	'tweapon_Dual Berettas',	'tweapon_FAMAS',	'tweapon_Five-SeveN',	'tweapon_G3SG1', 'tweapon_Glock-18',	'tweapon_Knife',	'tweapon_M249',	'tweapon_M4A1',	'tweapon_M4A4',	'tweapon_MAC-10',	'tweapon_MAG-7',	'tweapon_MP5-SD',	'tweapon_MP7',	'tweapon_MP9',	'tweapon_Negev',	'tweapon_Nova',	'tweapon_P2000',	'tweapon_P90',	'tweapon_PP-Bizon',	'tweapon_R8 Revolver',	'tweapon_SCAR-20',	'tweapon_SG 553',	'tweapon_SSG 08',	'tweapon_Sawed-Off',	'tweapon_Tec-9',	'tweapon_UMP-45',	'tweapon_USP-S',	'tweapon_XM1014',	'tweapon_Zeus x27',	'tweapon_P250', 'ctweapon_Decoy Grenade', 'ctweapon_Flashbang', 'ctweapon_HE Grenade',	'ctweapon_Incendiary Grenade', 'ctweapon_Molotov', 'ctweapon_Smoke Grenade', 'tweapon_Flashbang', 'tweapon_Galil AR', 'tweapon_HE Grenade', 'tweapon_Incendiary Grenade', 'tweapon_Molotov', 'tweapon_Smoke Grenade', 'tweapon_Decoy Grenade']
    remove_weapons = ['ctweapon_World','tweapon_World']


    
    remove_weapons = list(filter(lambda weapon: weapon in df.columns, remove_weapons))

    for weapon in weapons:
        if weapon not in df:
            df[weapon] = 0

    if len(remove_weapons) > 0:
        for weapon in remove_weapons:
            if df[weapon].item() == 1:
                return df, 1
    if len(remove_weapons) > 0:
        for weapon in remove_weapons:
            if weapon in df:
                df = df.drop(columns=weapon)  
    return df, 0
from datetime import timedelta
import pandas as pd
import numpy as np


def fix_cordinates(recovered_df, correct_df):
    recovered_df.loc[recovered_df["Country/Region"] == "Timor-Leste", ["Lat", "Long"]] = correct_df[correct_df["Country/Region"] == "Timor-Leste"][["Lat", "Long"]].values
    recovered_df.loc[recovered_df["Province/State"] == "Hebei", ["Lat", "Long"]] = correct_df[correct_df["Province/State"] == "Hebei"][["Lat", "Long"]].values
    recovered_df.loc[recovered_df["Province/State"] == "Henan", ["Lat", "Long"]] = correct_df[correct_df["Province/State"] == "Henan"][["Lat", "Long"]].values
    recovered_df.loc[recovered_df["Country/Region"] == "Mozambique", ["Lat", "Long"]] = correct_df[correct_df["Country/Region"] == "Mozambique"][["Lat", "Long"]].values
    recovered_df.loc[recovered_df["Country/Region"] == "Syria", ["Lat", "Long"]] = correct_df[correct_df["Country/Region"] == "Syria"][["Lat", "Long"]].values
    return recovered_df
    
def fix_canada(recovered_df):
    recovered_df.loc[recovered_df["Country/Region"] == "Canada", ["Province/State"]] = "UNK"
    return recovered_df

### Trasnform wide format to long format
def transform_wide_to_long(covid_df):
    res = pd.melt(covid_df, id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name="date", value_name="n_cases")
    return res



# Compute delta between observations
def transform_compute_delta(df):
    df["date"] = pd.to_datetime(df.date, format="%m/%d/%y")
    res = df.sort_values(by=['Country/Region', 'Province/State', "date"])
    res["delta"] = res["n_cases"].diff()
    
    ### Fix first observation for each Country - Province
    different_loc = df[['Country/Region', "Province/State"]].drop_duplicates().shape[0]
    res.loc[range(0, different_loc), "delta"] = res.loc[range(0, different_loc), "n_cases"]
    return res



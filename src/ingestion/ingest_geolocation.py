import pandas as pd 
from src.config import input_path, engine

def geolocation():
    df = pd.read_csv(input_path/"olist_geolocation_dataset.csv")
    df.to_sql("geolocation_raw", engine, if_exists = "replace", index=False)
    return df
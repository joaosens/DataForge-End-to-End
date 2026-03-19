import pandas as pd 
from src.config import input_path, engine


def customers():
    df = pd.read_csv(input_path/"olist_customers_dataset.csv")
    df.to_sql("customers_raw", engine, if_exists = "replace", index=False)
    return df

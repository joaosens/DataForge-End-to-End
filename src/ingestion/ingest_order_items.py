import pandas as pd 
from src.config import input_path, engine

def order_items():
    df = pd.read_csv(input_path/"olist_order_items_dataset.csv")
    df.to_sql("order_items_raw", engine, if_exists = "replace", index=False)
    return df
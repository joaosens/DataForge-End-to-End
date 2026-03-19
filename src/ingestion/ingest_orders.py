import pandas as pd 
from src.config import PATH, engine

def orders():
    df = pd.read_csv(PATH/"olist_orders_dataset.csv")
    df.to_sql("orders_raw", engine, if_exists = "append", index=False)

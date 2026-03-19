import pandas as pd 
from src.config import PATH, engine

def sellers():
    df = pd.read_csv(PATH/"olist_sellers_dataset.csv")
    df.to_sql("sellers_raw", engine, if_exists = "append", index=False)

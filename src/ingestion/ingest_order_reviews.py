import pandas as pd 
from src.config import PATH, engine

def reviews():
    df = pd.read_csv(PATH/"olist_order_reviews_dataset.csv")
    df.to_sql("reviews_raw", engine, if_exists = "append", index=False)

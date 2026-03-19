import pandas as pd 
from src.config import PATH, engine

def products():
    df = pd.read_csv(PATH/"olist_products_dataset.csv")
    df.to_sql("products_raw", engine, if_exists = "append", index=False)

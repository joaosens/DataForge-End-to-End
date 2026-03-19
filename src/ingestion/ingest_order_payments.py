import pandas as pd 
from src.config import PATH, engine

def payments():
    df = pd.read_csv(PATH/"olist_order_payments_dataset.csv")
    df.to_sql("payments_raw", engine, if_exists = "append", index=False)

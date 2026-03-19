import pandas as pd 
from src.config import PATH, engine

def translation():
    df = pd.read_csv(PATH/"olist_category_name_translation_dataset.csv")
    df.to_sql("translation_raw", engine, if_exists = "append", index=False)

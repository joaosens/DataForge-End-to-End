import pandas as pd
import pyarrow   #Case needs to convert the dataframe to parquets, but this sample I will convert on csv.
from src.config import engine, output_path
from src.transform.utils import normalize_cities

def t_customers(df) -> pd.DataFrame:
    df["city_final"] = normalize_cities(df, "customer_city", "customer_state")
    df = df.drop(columns=["city_clean"])
    df.to_sql("customers_clean", engine, if_exists="replace", index=False)
    df.to_parquet(output_path/"customers_clean.parquet", index=False)
    
def t_geolocation(df) -> pd.DataFrame:
    df["city_final"] = normalize_cities(df, "geolocation_city", "geolocation_state")
    df = df.groupby("geolocation_zip_code_prefix", as_index=False).agg(
        geolocation_lat=("geolocation_lat", "mean"),
        geolocation_lng=("geolocation_lng", "mean"),
        geolocation_city=("city_final", lambda x: x.mode()[0]),
        geolocation_state=("geolocation_state", lambda x: x.mode()[0]),
    )
    df = df.drop(columns=["city_clean"])
    df.to_sql("geolocation_clean", engine, if_exists="replace", index=False)
    df.to_parquet(output_path/"geolocation_clean.parquet", index=False)

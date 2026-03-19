import pandas as pd
import pyarrow   #Case needs to convert the dataframe to parquets, but this sample I will convert on csv.
from unidecode import unidecode
from rapidfuzz import process
from src.config import engine, output_path

def normalize_cities(df, city_col, state_col) -> pd.Series:
    # Normalization
    df["city_clean"] = (
        df[city_col]
        .apply(unidecode)
        .str.lower()
        .str.replace("-", " ")
        .str.replace("'", "")
    )
    cities = df["city_clean"].unique()
    city_series = pd.Series(cities)
    city_state = df.groupby("city_clean")[state_col].agg(lambda x: x.mode()[0])
    city_freq = df["city_clean"].value_counts()
    mapping = {}
     # 'Blocking' strategy to reduce complexity from O(n²) to O(k * n/k²) by grouping cities by first letter.
    for _, group in city_series.groupby(city_series.str[0]):
        group_list = group.tolist()
        for city in group_list:
            # 'Fuzzy Matching' to find similar city names using Levenshtein distance.
            matches = process.extract(city, group_list, limit=4)
            similars = [m for m in matches if m[1] > 90 and m[0] != city]
            for match_city, score, _ in similars:
                state_c = city_state.get(city)
                state_s = city_state.get(match_city)
                if state_c is None or state_s is None:
                    continue
                if state_c != state_s:
                    continue
                freq_c = city_freq.get(city, 0)
                freq_s = city_freq.get(match_city, 0)
                # 'Canonicalization' to normalize city name variants to the most frequent form.
                canonical = city if freq_c >= freq_s else match_city
                mapping[city] = canonical
                mapping[match_city] = canonical
    # Apply mapping to replace all variant city names with their canonical form.
    return df["city_clean"].replace(mapping)

def t_customers(df) -> pd.DataFrame:
    df["city_final"] = normalize_cities(df, "customer_city", "customer_state")
    df = df.drop(columns=["city_clean"])
    df.to_sql("customers_clean", engine, if_exists="replace", index=False)
    df.to_csv(output_path/"customers_clean.csv", index=False)
    
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
    df.to_csv(output_path/"geolocation_clean.csv", index=False)

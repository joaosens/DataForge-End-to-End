import pandas as pd
from unidecode import unidecode
from rapidfuzz import process

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

    

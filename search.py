import pandas as pd
from src.config import output_path

df = pd.read_csv(output_path/"geolocation_clean.csv")
print("\nDuplicates Sum:\n", df.duplicated().sum())

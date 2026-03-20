import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv() 

input_path = Path(__file__).parent.parent/"data/raw"
input_path.mkdir(parents=True, exist_ok=True)
output_path = Path(__file__).parent.parent/"data/processed"
output_path.mkdir(parents=True, exist_ok=True)
conn = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(conn)
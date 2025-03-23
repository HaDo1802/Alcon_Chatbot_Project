from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=True)  # 👈 Load first!

import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ✅ Load environment variables AFTER dotenv is called
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_DB = os.getenv('MYSQL_DB', 'financials_db')


def get_mysql_engine():
    encoded_password = quote_plus(MYSQL_PASSWORD)
    url = f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    return create_engine(url)

def save_to_mysql(df: pd.DataFrame, table_name: str):
    engine = get_mysql_engine()
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        print(f"✅ Data saved to MySQL table: {table_name}")

def fetch_from_mysql(table_name: str) -> pd.DataFrame:
    engine = get_mysql_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)

import logging
from .db import save_to_mysql

def load_data(df, _, table_name):
    try:
        save_to_mysql(df, table_name)
        logging.info(f"✅ Saved {table_name} to MySQL database.")
    except Exception as e:
        logging.error(f"❌ Failed to save {table_name} to MySQL: {e}")

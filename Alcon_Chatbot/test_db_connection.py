from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import os

load_dotenv()

url = f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
engine = create_engine(url)

try:
    inspector = inspect(engine)
    print("✅ Connected successfully.")
    print("📋 Tables:", inspector.get_table_names())
except Exception as e:
    print("❌ Connection failed:", e)

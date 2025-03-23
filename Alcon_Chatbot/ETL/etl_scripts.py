import time
import logging
from typing import List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ETL import extract, transform, load
from project_config import app_config as cfg  # Updated to use new path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def etl_process(tickers: List[str]):
    """
    Execute the ETL (Extract, Transform, Load) process for financial data.
    """
    start_time = time.time()
    logging.info("🚀 Starting ETL process")

    # Extract
    logging.info("📥 Extracting annual data")
    annually_income_statement_df = extract.get_income_statement(tickers, 'annually')
    annually_balance_sheet_df = extract.get_balance_sheet(tickers, 'annually')
    annually_cash_flow_df = extract.get_cashflow(tickers, 'annually')

    logging.info("📥 Extracting quarterly data")
    quarterly_income_statement_df = extract.get_income_statement(tickers, 'quarterly')
    quarterly_balance_sheet_df = extract.get_balance_sheet(tickers, 'quarterly')
    quarterly_cash_flow_df = extract.get_cashflow(tickers, 'quarterly')

    # Transform
    logging.info("🛠️ Transforming data")
    annual_dfs = [annually_income_statement_df, annually_balance_sheet_df, annually_cash_flow_df]
    quarterly_dfs = [quarterly_income_statement_df, quarterly_balance_sheet_df, quarterly_cash_flow_df]

    transformed_annual_dfs = [
        transform.add_custom_metrics(transform.add_quarter_and_year_columns(df)) for df in annual_dfs
    ]
    transformed_quarterly_dfs = [
        transform.add_custom_metrics(transform.add_quarter_and_year_columns(df)) for df in quarterly_dfs
    ]

    # Load (to MySQL)
    logging.info("💾 Loading data to MySQL")
    table_names = [
        'annually_income_statement', 'annually_balance_sheet', 'annually_cash_flow',
        'quarterly_income_statement', 'quarterly_balance_sheet', 'quarterly_cash_flow'
    ]
    for df, table in zip(transformed_annual_dfs + transformed_quarterly_dfs, table_names):
        load.load_data(df, None, table)

    end_time = time.time()
    logging.info(f"✅ ETL process completed in {end_time - start_time:.2f} seconds")

def main():
    etl_process(cfg.tickers)

if __name__ == "__main__":
    main()

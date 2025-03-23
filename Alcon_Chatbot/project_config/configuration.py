import streamlit as st
import os
import random
from dotenv import load_dotenv

from project_config import app_config as cfg  # ✅ Updated path

# Load secrets
load_dotenv()
deploy = cfg.deploy
CONFIG_PASSWORD = st.secrets["CONFIG_PASSWORD"] if deploy else os.getenv("CONFIG_PASSWORD")

# ETL and vector imports
from ETL import etl_scripts
import populate_vectordb as pvf

def add_ticker():
    new_ticker = st.text_input("Enter a new ticker symbol (e.g., AAPL):")
    if st.button("Add Ticker"):
        if new_ticker and new_ticker not in cfg.tickers:
            cfg.tickers.append(new_ticker)
            random_color = "#{:06x}".format(random.randint(0, 0xFFFFFF))
            cfg.COLOR_THEME[new_ticker] = random_color
            st.success(f"Ticker {new_ticker} has been added with color {random_color}.")
        else:
            st.warning("Ticker is either empty or already exists.")

def remove_ticker():
    ticker_to_remove = st.selectbox("Select a ticker to remove", cfg.tickers)
    if st.button("Remove Ticker"):
        if ticker_to_remove in cfg.tickers:
            cfg.tickers.remove(ticker_to_remove)
            cfg.COLOR_THEME.pop(ticker_to_remove, None)
            st.success(f"Ticker {ticker_to_remove} removed.")
        else:
            st.warning("Ticker not found.")

def display_configs_tab():
    st.title("⚙️ Configurations")

    config_password = st.text_input("Enter your config password:", type="password")
    if config_password == CONFIG_PASSWORD:
        st.subheader("Modify Tickers")
        st.markdown("Currently tracked: " + ", ".join(cfg.tickers))
        action = st.radio("Choose an action", ("Add a New Ticker", "Remove an Existing Ticker"))
        if action == "Add a New Ticker":
            add_ticker()
        else:
            remove_ticker()

        st.subheader("Run ETL Pipeline")
        if st.button("Run ETL Pipeline"):
            etl_scripts.main()
            st.success("ETL pipeline completed!")

        st.subheader("Run Vector Database Population")
        if st.button("Run Vector Database Population"):
            pvf.main()
            st.success("Vector DB population done!")

    else:
        st.info("Please enter the configuration password to continue.")

def main():
    display_configs_tab()

if __name__ == "__main__":
    main()

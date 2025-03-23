import time
import csv
import logging
from datetime import datetime

import streamlit as st
from openai import OpenAI

# Configuration imports
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from project_config import app_config as cfg

# OpenAI API Key Handling
deploy = cfg.deploy
if deploy:
    __import__('pysqlite3')
    sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
else:
    from dotenv import load_dotenv
    load_dotenv('.env')

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# OpenAI Embeddings for VectorDB (used internally, not user-provided)
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings

EMBEDDING_API_KEY = os.getenv("OPENAI_API_KEY")
embeddings = OpenAIEmbeddings(model="text-embedding-3-large", openai_api_key=EMBEDDING_API_KEY)

# Define ChromaDB location
persist_directory = "./chroma_langchain_db"

# Lazy load ChromaDB collections
def get_vectordb(selected_collection):
    collection_name = "alcon_collection_financial_statements_annually" if selected_collection == "Annually" else "alcon_collection_financial_statements_quarterly"
    return Chroma(persist_directory=persist_directory, embedding_function=embeddings, collection_name=collection_name)

# Intent detection (simple heuristic)
def needs_rag_context(query):
    finance_keywords = ["revenue", "net income", "EBITDA", "statement", "cash flow", "financial", "expenses", "profit", "ALC", "Alcon"]
    return any(keyword in query.lower() for keyword in finance_keywords)

# Retrieve relevant entries from ChromaDB
def find_relevant_entries_from_chroma_db(query, selected_collection):
    vectordb = get_vectordb(selected_collection)
    results = vectordb.similarity_search_with_score(query, k=3)
    filtered_results = [doc.page_content for doc, score in results if score > 0.5]
    return "\n".join(filtered_results) if filtered_results else ""

# Generate GPT-based response with optional context
def generate_gpt_response(user_query, chroma_result, client):
    current_year = datetime.now().year
    last_quarter = (datetime.now().month - 1) // 3

    if chroma_result:
        combined_prompt = f"""User query: {user_query}

You are a financial analyst at ALCON Inc. Provide an answer based on:
{chroma_result}

The current year is {current_year}, and the last available quarter is Q{last_quarter}.
Format your response clearly and concisely."""
    else:
        combined_prompt = f"""User query: {user_query}
You are a helpful AI assistant. Answer the user question directly. Current date is {datetime.now().strftime('%B %d, %Y')}.
"""

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a financial assistant providing data-driven insights."},
            {"role": "user", "content": combined_prompt}
        ]
    )
    return response.choices[0].message.content

# Log response times
def log_response_time(query, response_time, is_first_prompt):
    csv_file = 'responses.csv'
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['Timestamp', 'Query', 'Response Time (seconds)', 'Is First Prompt'])
        writer.writerow([datetime.now(), query, f"{response_time:.2f}", "Yes" if is_first_prompt else "No"])

# Query processing pipeline
import traceback

def query_interface(user_query, is_first_prompt, selected_collection, client):
    start_time = time.time()

    try:
        # Intent check (basic keyword detection)
        financial_keywords = ["revenue", "income", "statement", "EPS", "operating", "quarter", "year", "2023", "financial", "cost", "expenses", "cash", "debt", "asset", "liability"]
        is_financial = any(word in user_query.lower() for word in financial_keywords)

        if is_financial:
            chroma_result = find_relevant_entries_from_chroma_db(user_query, selected_collection)
            if chroma_result and "No relevant financial data found" not in chroma_result:
                gpt_response = generate_gpt_response(user_query, chroma_result, client)
            else:
                gpt_response = generate_gpt_response(user_query, "", client)
        else:
            # Fallback to general GPT answer
            current_date = datetime.now().strftime("%B %d, %Y")
            fallback_prompt = f"""User asked: "{user_query}"\nRespond normally. Today is {current_date}."""
            gpt_response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful financial assistant."},
                    {"role": "user", "content": fallback_prompt}
                ]
            ).choices[0].message.content

        if gpt_response:
            response_time = time.time() - start_time
            log_response_time(user_query, response_time, is_first_prompt)
            return gpt_response

    except Exception as e:
        logging.error("Error in query_interface: %s", e)
        traceback.print_exc()  # Print the full traceback for debugging
        return "There was an error processing your request. Please try again later."


# Streamlit Chat Interface
def display_chatbot():
    st.title(" Alcon Financial Chatbot")

    api_key = st.text_input("Enter your OpenAI API key:", type="password")
    if not api_key:
        st.info("🔑 Please enter your OpenAI API key to continue.", icon="🔒")
        return

    client = OpenAI(api_key=api_key)
    selected_collection = st.radio("Select Data Period:", ("Annually", "Quarterly"))

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Ask a financial question..."):
        with st.chat_message("user"):
            st.markdown(prompt)

        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.spinner("Analyzing data..."):
            response = query_interface(prompt, len(st.session_state.messages) == 1, selected_collection, client)

        with st.chat_message("assistant"):
            st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})

def main():
    display_chatbot()

if __name__ == "__main__":
    main()

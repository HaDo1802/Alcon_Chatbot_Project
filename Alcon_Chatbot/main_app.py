import streamlit as st

# Main app layout
st.set_page_config(
    page_title="Alcon Chatbot Platform",
    page_icon="🤖",
    layout="wide"
)

# Welcome screen
st.title("🤖 Welcome to Alcon Chatbot Platform")
st.markdown("""
Welcome to the **Alcon AI Assistant Hub**.  
Use the sidebar to navigate between tools like:
- 🧠 Chatbot Assistant
- 📊 Dashboard & Analytics
- 🔁 Model Retraining (Coming soon)

---

Feel free to explore each tool and feature!
""")

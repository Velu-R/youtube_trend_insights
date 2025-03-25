import streamlit as st

def render_chat_bot():
    user_query = st.chat_input("Ask me about youtube trends")
    if user_query:
        with st.chat_message('user'):
            st.write(user_query)
    with st.chat_message('ai'):
        st.write("Hello Buddy")
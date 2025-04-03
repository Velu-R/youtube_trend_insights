import streamlit as st

def get_chat_history(conversation):
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    st.session_state.chat_history.append(conversation)  
    return st.session_state.chat_history

def render_chat_bot():
    """
        A simple chatbot interface using Streamlit that responds to user queries about YouTube trends.

        Features:
        - Displays a welcome message to new users.
        - Captures and displays user input in a chat format.
        - Provides an option to clear the chat session.

        Usage:
        Call this function within a Streamlit app to render a chatbot interface.
    """
    # Display welcome message only once per session
    if "welcome_message" not in st.session_state:
        with st.chat_message("ai"):
            st.write('Hello Buddy! How can I help you?')
        st.session_state.welcome_message = True

    # Capture user input
    user_query = st.chat_input("Ask me about YouTube trends")
    if user_query:
        with st.chat_message("user"):
            input = {"role":"user","content":f"{user_query}"}
            chat = get_chat_history(input)
            for message in chat:
                st.write(message['content'])
            
    # Button to clear chat
    if st._bottom.button("Clear Chat"):
        st.session_state.clear()

        
render_chat_bot()
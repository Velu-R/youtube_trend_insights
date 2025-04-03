import os
import json
from groq import Groq
from dotenv import load_dotenv
from src.backend.database.db import execute_query
from src.backend.utils import logger

load_dotenv()
logger = logger.get_logger()

client = Groq(api_key=os.getenv('groq_api_key'))

MODEL = "llama-3.3-70b-versatile"

tools = [
    {
        "type": "function",
        "function": {
            "name": "execute_query",
            "description": "Executes SQL queries on the youtube_trending_data table based on user requirements.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query string to execute on the database."
                    }
                },
                "required": ["query"]
            }
        }
    }
]

def run_conversation(user_prompt):
    logger.info(f"Received user prompt: {user_prompt}")
    messages = [
        {
            "role": "system",
            "content": """
                You are a helpful assistant providing insights on YouTube trending videos.
                When asked for videos, decide if an SQL query is needed.
                If needed, generate the appropriate SQL query and call `execute_query`.
                If the answer is known or doesn't require a query, respond directly.
                Always include video_id, title, and thumbnail_link when returning results.
                You have access to the following database table schema:
                **Table: youtube_trending_data**
                - video_id (TEXT)
                - title (TEXT)
                - publishedAt (TEXT)
                - channelid (TEXT)
                - channeltitle (TEXT)
                - categoryid (TEXT)
                - trending_date (TEXT)
                - tags (TEXT)
                - view_count (BIGINT)
                - likes (BIGINT)
                - dislikes (BIGINT)
                - comment_count (BIGINT)
                - thumbnail_link (TEXT)
                - comments_disabled (BOOLEAN)
                - ratings_disabled (BOOLEAN)
                - description (TEXT)
                - category_name (TEXT)
            """
        },
        {"role": "user", "content": user_prompt},
    ]

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=tools,
            tool_choice="auto",
            max_completion_tokens=4096
        )
        logger.info("LLM response received successfully.")
    except Exception as e:
        logger.error(f"Error in LLM query processing: {e}")
        return "An error occurred while processing your request."
    
    response_message = response.choices[0].message
    tool_calls = response_message.tool_calls

    if tool_calls:
        logger.info(f"Tool calls detected: {tool_calls}")
        available_functions = {"execute_query": execute_query}
        messages.append(response_message)

        for tool_call in tool_calls:
            function_name = tool_call.function.name
            function_to_call = available_functions.get(function_name)

            if function_to_call:
                function_args = json.loads(tool_call.function.arguments)
                query = function_args.get("query")
                params = function_args.get("params", None)
                fetch = function_args.get("operation") == "SELECT"
                function_response = function_to_call(query)

                messages.append(
                    {
                        "role": "tool",
                        "name": function_name,
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(function_response),
                    }
                )

    # Final response after executing the query
    second_response = client.chat.completions.create(
        model=MODEL,
        messages=messages
    )

    return second_response.choices[0].message.content

# Example usage
user_prompt = "show me the top 5 commented videos"
result = run_conversation(user_prompt)
print(result)
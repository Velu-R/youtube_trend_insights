import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from src.backend.utils import logger
from dotenv import load_dotenv

load_dotenv()
logger = logger.get_logger()

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

def get_youtube_service():
    """Creates and returns a YouTube API service instance with error handling and retries."""

    if not YOUTUBE_API_KEY:
        logger.error("YouTube API key is missing. Please set YOUTUBE_API_KEY.")
        return None

    try:
        youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=YOUTUBE_API_KEY, cache_discovery=False)
        logger.info("YouTube API service initialized successfully.")
        return youtube
    except HttpError as e:
        logger.error(f"An error occurred: {e}")
        return None

def get_video_links(video_ids: list) -> list:
    """Fetches YouTube video links for given video IDs."""
    youtube = get_youtube_service()
    if not youtube:
        return ["Failed to initialize YouTube API service."]
    video_links = []
    for video_id in video_ids:
        try:
            # Fetch video details (title & link)
            video_request = youtube.videos().list(
                part="snippet",
                id=video_id
            )
            video_response = video_request.execute()

            if not video_response["items"]:
                video_links.append(f"Video ID `{video_id}` not found.")
                continue

            video_title = video_response["items"][0]["snippet"]["title"]
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            video_links.append(f"**[{video_title}]({video_url})**")

        except HttpError as e:
            logger.error(f"HTTP error while fetching data for video ID {video_id}: {e}")
            video_links.append(f"Error fetching details for video `{video_id}`: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while fetching video details for {video_id}: {e}")
            video_links.append(f"Unexpected error for video `{video_id}`: {e}")

    return video_links

# video_id = ["Iot0eF6EoNA"]
# result = get_video_links(video_id)
# print(result)
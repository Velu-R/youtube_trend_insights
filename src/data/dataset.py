import os
import pandas as pd
from src.backend.utils.logger import get_logger

logger = get_logger()

# Dataset paths
DATASET_PATH = "src/data/IN_youtube_trending_data.csv"
CATEGORY_PATH = "src/data/IN_category_id.json"
PROCESSED_CSV_PATH = "src/data/processed_dataset.csv"

def get_dataset():
    """
    Loads, processes, and merges the YouTube trending dataset with category metadata.

    This function performs the following steps:

    1. Checks if a previously processed dataset exists and loads it if available.
    2. Validates the existence of the raw dataset CSV and category JSON files.
    3. Loads the dataset from a CSV file and verifies it is not empty.
    4. Loads and normalizes category metadata from a JSON file.
    5. Merges category names into the main dataset based on categoryId.
    6. Handles missing values (NaNs) appropriately.
    7. Cleans the data by removing duplicates and resetting the index.
    8. Saves the processed dataset as a CSV file for future use.

    Returns:
        pd.DataFrame: A processed DataFrame containing the YouTube trending dataset 
                      with category names added. Returns None if an error occurs.

    Raises:
        FileNotFoundError: If the dataset or category metadata file is missing.
        ValueError: If the category JSON file is invalid or lacks the expected structure.
        pd.errors.EmptyDataError: If the dataset file is empty.
        pd.errors.ParserError: If there is an issue parsing the CSV dataset.
    """
    try:

        # Validate dataset existence
        if not os.path.isfile(DATASET_PATH):
            raise FileNotFoundError(f"Dataset file not found: {DATASET_PATH}")

        if not os.path.isfile(CATEGORY_PATH):
            raise FileNotFoundError(f"Category file not found: {CATEGORY_PATH}")

        # Load dataset
        dataset_df = pd.read_csv(DATASET_PATH)
        if dataset_df.empty:
            raise pd.errors.EmptyDataError("Dataset file is empty.")

        # Log missing values before processing
        # missing_values = dataset_df.isnull().sum()
        # logger.info(f"Missing values in dataset before processing:\n{missing_values[missing_values > 0]}")

        # Load and validate category metadata
        category_df = pd.read_json(CATEGORY_PATH)

        # Ensure 'items' column exists
        if "items" not in category_df.columns or category_df["items"].isnull().all():   
            raise ValueError("Invalid category file format: Missing or incorrect 'items' key.")

        # Normalize category metadata
        category_df = pd.json_normalize(category_df["items"])

        if category_df.empty:
            raise ValueError("Category metadata is empty after normalization.")

        # Rename columns for better readability
        category_df = category_df.rename(columns={"id": "categoryId", "snippet.title": "category_name"})

        # Ensure categoryId is string for correct merging
        dataset_df["categoryId"] = dataset_df["categoryId"].astype(str)
        category_df["categoryId"] = category_df["categoryId"].astype(str)

        # Merge dataset with category names
        merged_df = dataset_df.merge(category_df[["categoryId", "category_name"]], on="categoryId", how="left")

        # Handle NaN values in category and other essential columns
        merged_df["channelTitle"] = merged_df["channelTitle"].fillna("Unknown")
        merged_df["description"] = merged_df["description"].fillna("") 

        # # Log missing values after processing
        # missing_values_after = merged_df.isnull().sum()
        # logger.info(f"Missing values after merging:\n{missing_values_after[missing_values_after > 0]}")

        # Remove duplicates, reset index, and save processed dataset
        merged_df.drop_duplicates(inplace=True)
        merged_df.reset_index(drop=True, inplace=True)

        return merged_df

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
    except pd.errors.EmptyDataError:
        logger.error("Dataset file is empty.")
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing dataset file: {e}")
    except ValueError as e:
        logger.error(f"Data validation error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error loading dataset: {e}")

    return None

# Run the function
df = get_dataset()
print(df.head()['trending_date'])
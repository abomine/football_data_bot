# run_pipeline.py
import logging
from pathlib import Path
from datetime import datetime
from pipelines.ingestion import fetch_fixtures, save_raw_data
from pipelines.processing import process_fixtures, save_processed_data
from pipelines.storage import FootballDataStorage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_full_pipeline(league_id: int = 39, season: int = 2023):
    """Run complete pipeline from ingestion to storage"""
    try:
        # 1. INGESTION
        logger.info("Starting data ingestion...")
        api_key = os.getenv("API_KEY")
        if not api_key:
            raise ValueError("API_KEY not found in environment variables")

        fixtures_data = fetch_fixtures(
            league_id=league_id, season=season, api_key=api_key
        )

        # Save raw data with timestamp
        raw_filename = f"fixtures_{league_id}_{season}_{datetime.now().date()}"
        save_raw_data(fixtures_data, raw_filename)
        logger.info(f"Saved raw data to data/raw/{raw_filename}.json")

        # 2. PROCESSING
        logger.info("Processing data...")
        raw_file_path = Path(f"data/raw/{raw_filename}.json")
        processed_df = process_fixtures(raw_file_path)

        # Save processed data
        processed_path = Path("data/processed") / f"processed_{raw_filename}.parquet"
        saved_path = save_processed_data(processed_df, processed_path)
        logger.info(f"Saved processed data to {saved_path}")

        # 3. STORAGE
        logger.info("Loading data into database...")
        storage = FootballDataStorage()
        loaded_count = storage.load_parquet_file(saved_path)

        # Verify results
        total_count = storage.get_fixture_count()
        logger.info(f"Successfully loaded {loaded_count} new fixtures")
        logger.info(f"Total fixtures in database: {total_count}")

        return {
            "raw_file": raw_file_path,
            "processed_file": saved_path,
            "loaded_count": loaded_count,
            "total_count": total_count,
        }

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        if "storage" in locals():
            storage.close()


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()

    # Example: Process Premier League 2023 data
    run_full_pipeline(league_id=39, season=2023)

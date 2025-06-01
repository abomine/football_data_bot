# pipelines/processing.py
from pathlib import Path
import pandas as pd
import json
#import os
from datetime import datetime


def process_fixtures(raw_file_path: str | Path) -> pd.DataFrame:
    """Process raw JSON fixtures into a cleaned DataFrame."""
    input_path = Path(raw_file_path).absolute()

    if not input_path.exists():
        raise FileNotFoundError(f"Fixture file not found at: {input_path}")
    print(f"Opening: {input_path}")


    print("Is file a JSON? First 10 bytes:")
    with open(input_path, "rb") as f:
        print(f.read(10))
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    fixtures = data["response"]
    df = pd.json_normalize(fixtures)

    # Extract and transform relevant fields
    processed_data = []
    for fixture in fixtures:
        processed_data.append(
            {
                "fixture_id": fixture["fixture"]["id"],
                "league_id": fixture["league"]["id"],
                "league_name": fixture["league"]["name"],
                "season": fixture["league"]["season"],
                "home_team_id": fixture["teams"]["home"]["id"],
                "home_team_name": fixture["teams"]["home"]["name"],
                "away_team_id": fixture["teams"]["away"]["id"],
                "away_team_name": fixture["teams"]["away"]["name"],
                "home_goals": fixture["goals"]["home"],
                "away_goals": fixture["goals"]["away"],
                "date": fixture["fixture"]["date"],
                "venue_name": fixture["fixture"]["venue"]["name"],
                "referee": fixture["fixture"].get("referee"),
                "status_short": fixture["fixture"]["status"]["short"],
            }
        )

    return pd.DataFrame(processed_data)

def save_processed_data(
    df: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    """Save processed data to Parquet, creating parent dirs if needed"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)  # This creates the directory

    df.to_parquet(path)
    return path
'''
def save_processed_data(df: pd.DataFrame, filename_prefix: str = "fixtures") -> Path:
    """
    Save processed data to Parquet in the processed directory.
    Returns path to the saved file.
    """
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = processed_dir / f"{filename_prefix}_{timestamp}.parquet"

    df.to_parquet(output_path)
    return output_path
'''

if __name__ == "__main__":
    try:
        # Define input path relative to project root
        raw_path = Path("data/raw/test_fixtures.json")

        # Verify raw data exists
        if not raw_path.exists():
            raise FileNotFoundError(
                f"Raw data file not found at: {raw_path.absolute()}"
            )

        # Process and save data
        print(f"Processing data from: {raw_path}")
        df = process_fixtures(raw_path)
        saved_path = save_processed_data(df, raw_path)

        print(f"✅ Successfully processed {len(df)} records")
        print(f"📁 Output saved to: {saved_path.absolute()}")

    except Exception as e:
        print(f"❌ Processing failed: {str(e)}")
        raise
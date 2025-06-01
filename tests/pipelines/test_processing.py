import pytest
import pandas as pd
import os
#import json
from unittest.mock import patch, mock_open
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from pipelines.processing import process_fixtures, save_processed_data
# Sample raw API data for testing
SAMPLE_RAW_DATA = {
    "response": [
        {
            "fixture": {"id": 1, "timestamp": 1632313800},
            "league": {"name": "Premier League"},
            "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Tottenham"}},
            "goals": {"home": 2, "away": 1},
        }
    ]
}


def test_process_fixtures():
    """Test processing with the fixed test data file"""
    # Use the exact absolute path to your test file
    test_file = Path(
        r"C:\Users\amine\Desktop\football_data_bot\data\raw\test_fixtures.json"
    )

    # Process the file
    result = process_fixtures(test_file)

    # Basic validation
    assert not result.empty
    assert "home_team_name" in result.columns
    assert "away_team_name" in result.columns
    print(f"\nSuccessfully processed {len(result)} fixtures from: {test_file}")


def test_save_processed_data(tmp_path):
    # Create test DataFrame
    df = pd.DataFrame({"fixture_id": [1], "league": ["Premier League"]})

    # Test saving
    output_file = tmp_path / "processed" / "fixtures.parquet"
    save_processed_data(df, output_file)

    # Verify file exists and can be loaded
    assert os.path.exists(output_file)
    loaded_df = pd.read_parquet(output_file)
    assert loaded_df.iloc[0]["league"] == "Premier League"


@patch("pandas.DataFrame.to_parquet")
def test_save_processed_data_error_handling(mock_to_parquet):
    """Test error during file saving."""
    mock_to_parquet.side_effect = Exception("Save failed")
    df = pd.DataFrame({"test": [1]})

    with pytest.raises(Exception, match="Save failed"):
        save_processed_data(df, "dummy_path.parquet")

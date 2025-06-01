import pytest
import requests
from unittest.mock import patch, mock_open
import os
import json
from datetime import datetime
import sys
from pathlib import Path
#idk why this test code doesn't find ingestion.py
sys.path.append(str(Path(__file__).parent.parent.parent))
from pipelines.ingestion import fetch_fixtures, save_raw_data


# ---- Test fetch_fixtures() ----
@patch("pipelines.ingestion.requests.get")
def test_fetch_fixtures_success(mock_get):
    """Test successful API call."""
    mock_response = {
        "response": [
            {
                "fixture": {"id": 1},
                "teams": {"home": {"name": "Team A"}, "away": {"name": "Team B"}},
            }
        ]
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    result = fetch_fixtures(league_id=39, season=2023, api_key="test_key")
    assert result == mock_response

@patch("pipelines.ingestion.requests.get")
def test_fetch_fixtures_failure(mock_get):
    # Simulate a 403 Forbidden response
    mock_get.return_value.status_code = 403
    mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "403 Client Error"
    )

    # Test if the function raises an exception
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_fixtures(league_id=39, season=2023, api_key="invalid_key")

# ---- Test save_raw_data() ----
def test_save_raw_data(tmp_path):
    """Test if data is saved correctly."""
    test_data = {"key": "value"}
    save_dir = tmp_path / "data/raw"
    save_raw_data(test_data, "test_file", save_dir=str(save_dir))

    # Check file exists
    saved_file = save_dir / "test_file.json"
    assert saved_file.exists()

    # Check content
    with open(saved_file, "r") as f:
        assert json.load(f) == test_data


# ---- Integration Test (Optional) ----
@patch("pipelines.ingestion.fetch_fixtures")
@patch("pipelines.ingestion.save_raw_data")
def test_main(mock_save, mock_fetch, tmp_path):
    """Test the main() function."""
    from pipelines.ingestion import main

    mock_fetch.return_value = {"response": []}
    os.environ["API_KEY"] = "test_key"

    main()
    mock_fetch.assert_called_once_with(league_id=39, season=2023, api_key="test_key")
    mock_save.assert_called_once()

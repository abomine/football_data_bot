import pytest
from pathlib import Path
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from pipelines.storage import FootballDataStorage


@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "test.db"
    storage = FootballDataStorage(db_path=str(db_path))
    yield storage
    storage.close()

@pytest.fixture
def sample_parquet(tmp_path):
    """Create a test parquet file with ALL required columns"""
    data = {
        "fixture_id": [1, 2],
        "league_id": [39, 39],
        "league_name": ["Premier League", "Premier League"],
        "season": [2023, 2023],
        "home_team_id": [42, 50],
        "home_team_name": ["Arsenal", "Chelsea"],
        "away_team_id": [66, 55],
        "away_team_name": ["Liverpool", "Man City"],
        "home_goals": [2, 1],
        "away_goals": [2, 0],
        "date": ["2023-08-01", "2023-08-02"],
        "venue_name": ["Emirates", "Stamford Bridge"],
        "referee": ["Ref 1", "Ref 2"],
        "status_short": ["FT", "FT"],
    }
    df = pd.DataFrame(data)
    path = tmp_path / "test.parquet"
    df.to_parquet(path)
    return path


def test_load_parquet(temp_db, sample_parquet):
    # First verify the test file has data
    test_df = pd.read_parquet(sample_parquet)
    assert len(test_df) == 2

    # Test loading
    count = temp_db.load_parquet_file(sample_parquet)
    assert count == 2

    # Verify database content
    db_count = temp_db.conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0]
    assert db_count == 2

def test_invalid_data(temp_db, tmp_path):
    # Create invalid parquet file
    df = pd.DataFrame({"invalid_column": [1, 2]})
    invalid_path = tmp_path / "invalid.parquet"
    df.to_parquet(invalid_path)

    count = temp_db.load_parquet_file(invalid_path)
    assert count == 0  # Should skip invalid files

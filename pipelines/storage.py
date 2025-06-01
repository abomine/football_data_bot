# pipelines/storage.py
import duckdb
import pandas as pd
from pathlib import Path
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FootballDataStorage:
    required_columns = {
        "fixture_id",
        "league_id",
        "league_name",
        "season",
        "home_team_id",
        "home_team_name",
        "away_team_id",
        "away_team_name",
        "home_goals",
        "away_goals",
        "date",
    }
    def __init__(self, db_path: str = "data/football.db"):
        self.db_path = Path(db_path)
        self.conn = self._setup_database()

    def _setup_database(self) -> duckdb.DuckDBPyConnection:
        """Initialize database with schema matching processed data"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(self.db_path))

        conn.execute("""
        CREATE TABLE IF NOT EXISTS fixtures (
            fixture_id BIGINT PRIMARY KEY,
            league_id INTEGER,
            league_name VARCHAR,
            season INTEGER,
            home_team_id INTEGER,
            home_team_name VARCHAR,
            away_team_id INTEGER,
            away_team_name VARCHAR,
            home_goals INTEGER,
            away_goals INTEGER,
            date TIMESTAMP,
            venue_name VARCHAR,
            referee VARCHAR,
            status_short VARCHAR,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Create indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_fixture_date ON fixtures(date);
        CREATE INDEX IF NOT EXISTS idx_league ON fixtures(league_id);
        CREATE INDEX IF NOT EXISTS idx_teams ON fixtures(home_team_id, away_team_id);
        """)

        logger.info(f"Database initialized at {self.db_path.absolute()}")
        return conn

    def _validate_dataframe(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame structure before loading"""
        return self.required_columns.issubset(df.columns)
    def load_parquet_file(self, file_path: Path) -> int:
        """Load a single parquet file into database"""
        try:
            df = pd.read_parquet(file_path)

            if not self._validate_dataframe(df):
                missing = set(self.required_columns) - set(df.columns)
                logger.warning(
                    f"Skipping {file_path.name} - missing columns: {missing}"
                )
                return 0

            # Ensure proper datetime conversion
            df["date"] = pd.to_datetime(df["date"])

            result = self.conn.execute("""
                INSERT OR IGNORE INTO fixtures 
                SELECT 
                    fixture_id, league_id, league_name, season,
                    home_team_id, home_team_name, away_team_id, away_team_name,
                    home_goals, away_goals, date,
                    venue_name, referee, status_short
                FROM df
            """)

            inserted = result.rowcount
            logger.info(f"Loaded {inserted} records from {file_path.name}")
            return inserted

        except Exception as e:
            logger.error(f"Failed to load {file_path}: {str(e)}")
            return 0

    def load_processed_data(self, processed_dir: str = "data/processed") -> int:
        """Load all parquet files from processed directory"""
        processed_path = Path(processed_dir)
        if not processed_path.exists():
            raise FileNotFoundError(
                f"Processed data directory not found: {processed_path}"
            )

        total_loaded = 0
        for parquet_file in sorted(processed_path.glob("*.parquet")):
            total_loaded += self.load_parquet_file(parquet_file)

        logger.info(f"Total records loaded in this batch: {total_loaded}")
        return total_loaded

    def get_fixture_count(self) -> int:
        """Get total number of fixtures in database"""
        return self.conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0]
    def get_upcoming_fixtures(self, team_name: str = None, limit: int = 10):
        """Get upcoming fixtures optionally filtered by team"""
        query = """
            SELECT 
                fixture_id,
                home_team_name as home_team,
                away_team_name as away_team,
                strftime(date, '%Y-%m-%d %H:%M') as date,
                venue_name
            FROM fixtures
            WHERE date > CURRENT_TIMESTAMP
            AND status_short = 'NS'
        """

        if team_name:
            query += f" AND (home_team_name ILIKE '%{team_name}%' OR away_team_name ILIKE '%{team_name}%')"

        query += f" ORDER BY date LIMIT {limit}"

        try:
            return self.conn.execute(query).fetchdf().to_dict("records")
        except Exception as e:
            logger.error(f"Error in get_upcoming_fixtures: {e}")
            return []

    def get_recent_results(self, team_name: str = None, limit: int = 5):
        """Get recent results optionally filtered by team"""
        query = """
            SELECT 
                home_team_name as home_team,
                away_team_name as away_team,
                home_goals,
                away_goals,
                strftime(date, '%Y-%m-%d') as date
            FROM fixtures
            WHERE status_short = 'FT'
        """

        if team_name:
            query += f" AND (home_team_name ILIKE '%{team_name}%' OR away_team_name ILIKE '%{team_name}%')"

        query += f" ORDER BY date DESC LIMIT {limit}"

        try:
            return self.conn.execute(query).fetchdf().to_dict("records")
        except Exception as e:
            logger.error(f"Error in get_recent_results: {e}")
            return []

    def get_league_standings(self, league_id: int = 39):
        """Get league standings (simplified version)"""
        query = f"""
            SELECT 
                team_name,
                points,
                games_played
            FROM (
                SELECT 
                    home_team_name as team_name,
                    SUM(CASE 
                        WHEN home_goals > away_goals THEN 3
                        WHEN home_goals = away_goals THEN 1
                        ELSE 0
                    END) as points,
                    COUNT(*) as games_played
                FROM fixtures
                WHERE league_id = {league_id}
                AND status_short = 'FT'
                GROUP BY home_team_name
                
                UNION ALL
                
                SELECT 
                    away_team_name as team_name,
                    SUM(CASE 
                        WHEN away_goals > home_goals THEN 3
                        WHEN away_goals = home_goals THEN 1
                        ELSE 0
                    END) as points,
                    COUNT(*) as games_played
                FROM fixtures
                WHERE league_id = {league_id}
                AND status_short = 'FT'
                GROUP BY away_team_name
            )
            GROUP BY team_name
            ORDER BY SUM(points) DESC
            LIMIT 20
        """

        try:
            return self.conn.execute(query).fetchdf().to_dict("records")
        except Exception as e:
            logger.error(f"Error in get_league_standings: {e}")
            return []

    def close(self):
        """Clean up database connection"""
        self.conn.close()


def main():
    try:
        storage = FootballDataStorage()

        # Load all available processed data
        loaded_count = storage.load_processed_data()
        total_count = storage.get_fixture_count()

        logger.info(f"Total fixtures in database: {total_count}")
        logger.info(f"New fixtures loaded: {loaded_count}")

    except Exception as e:
        logger.error(f"Storage operation failed: {str(e)}")
        raise
    finally:
        storage.close()


if __name__ == "__main__":
    main()

from typing import List, Dict, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BotQueries:
    @staticmethod
    def get_upcoming_fixtures(
        storage, team_name: Optional[str] = None, limit: int = 10
    ) -> List[Dict]:
        """Query upcoming fixtures from database"""
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
            return storage.conn.execute(query).fetchdf().to_dict("records")
        except Exception as e:
            logger.error(f"Query error in get_upcoming_fixtures: {e}")
            return []

    @staticmethod
    def get_recent_results(
        storage, team_name: Optional[str] = None, limit: int = 5
    ) -> List[Dict]:
        """Query recent results from database"""
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
            return storage.conn.execute(query).fetchdf().to_dict("records")
        except Exception as e:
            logger.error(f"Query error in get_recent_results: {e}")
            return []

    @staticmethod
    def get_league_standings(storage, league_id: int = 39) -> List[Dict]:
        """Query league standings (mock implementation - extend with real logic)"""
        query = f"""
            SELECT DISTINCT
                home_team_name as team_name,
                league_id
            FROM fixtures
            WHERE league_id = {league_id}
            LIMIT 20
        """

        try:
            # This is simplified - you'll need to implement proper standings logic
            teams = storage.conn.execute(query).fetchdf()
            # Mock standings data - replace with actual calculation
            teams["points"] = 50  # Placeholder
            teams["games_played"] = 20  # Placeholder
            return teams.sort_values("team_name").to_dict("records")
        except Exception as e:
            logger.error(f"Query error in get_league_standings: {e}")
            return []

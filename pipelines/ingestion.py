import requests
import os
from dotenv import load_dotenv
import json
from datetime import datetime

load_dotenv()


def fetch_fixtures(league_id: int, season: int, api_key: str):
    url = (
        f"https://v3.football.api-sports.io/fixtures?league={league_id}&season={season}"
    )
    headers = {"x-apisports-key": api_key}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises HTTPError for 4XX/5XX status codes
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"API Error: {e}")
        raise  # Re-raise the exception for the test to catch


def save_raw_data(data: dict, filename: str, save_dir: str = "data/raw"):
    """Saves raw API response to JSON."""
    os.makedirs(save_dir, exist_ok=True)
    with open(f"{save_dir}/{filename}.json", "w") as f:
        json.dump(data, f)


def main():
    """Main function to fetch and save data."""
    api_key = os.getenv("API_KEY")
    data = fetch_fixtures(league_id=39, season=2023, api_key=api_key)
    save_raw_data(data, f"fixtures_39_2023_{datetime.now().date()}")


if __name__ == "__main__":
    main()

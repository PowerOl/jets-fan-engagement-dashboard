
from pathlib import Path
import sqlite3

import requests
import pandas as pd


TEAM = 'WPG'
SEASON = '20242025'

def get_jets_schedule():
    url = f"https://api-web.nhle.com/v1/club-schedule-season/{TEAM}/{SEASON}"
    r = requests.get(url, timeout= 30)
    r.raise_for_status()
    return r.json()

def extract_games(schedule_json):
    games = schedule_json.get("games",[])
    rows = []

    for g in games:
        game_date = g.get("gameDate")
        game_id = g.get("id")
        venue = g.get("venue", {}).get("default")

        away_team = g.get("awayTeam", {}).get("abbrev")
        home_team = g.get("homeTeam", {}).get("abbrev")

        away_score = g.get("awayTeam", {}).get("score")
        home_score = g.get("homeTeam", {}).get("score")

        jets_is_home = home_team == TEAM

        if jets_is_home:
            jets_score = home_score
            opp_team = away_team
            opp_score = away_score
        else:
            jets_score = away_score
            opp_team = home_team
            opp_score = home_score

        result = None
        if jets_score is not None and opp_score is not None:
            result = "W" if jets_score > opp_score else "L"

        rows.append({
            "game_id": game_id,
            "game_date": game_date,
            "venue": venue,
            "home_team": home_team,
            "away_team": away_team,
            "jets_is_home": int(jets_is_home),
            "opponent": opp_team,
            "jets_score": jets_score,
            "opponent_score": opp_score,
            "result": result,
        })

    return pd.DataFrame(rows)

def get_boxscore(game_id):
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def add_boxscore_fields(df):
    extra_rows = []

    for game_id in df["game_id"]:
        try:
            box = get_boxscore(game_id)

            home_team = box.get("homeTeam", {}).get("abbrev")
            away_team = box.get("awayTeam", {}).get("abbrev")

            home_sog = box.get("homeTeam", {}).get("sog")
            away_sog = box.get("awayTeam", {}).get("sog")


            if home_team == TEAM:
                jets_shots = home_sog
                opp_shots = away_sog
            else:
                jets_shots = away_sog
                opp_shots = home_sog

            extra_rows.append({
                "game_id": game_id,
                "jets_shots": jets_shots,
                "opponent_shots": opp_shots,
            })

        except Exception:
            extra_rows.append({
                "game_id": game_id,
                "jets_shots": None,
                "opponent_shots": None,
            } )


    extra_df = pd.DataFrame(extra_rows)
    return df.merge(extra_df, on="game_id", how="left")

def save_outputs(df):
    Path("data/raw").mkdir( exist_ok=True)
    csv_path = "data/raw/jets_games.csv"
    db_path = "data/raw/jets.db"

    df.to_csv(csv_path, index = False)

    conn = sqlite3.connect(db_path)
    df.to_sql("jets_games", conn, if_exists= "replace", index= False)
    conn.close()

def main():
    schedule_json = get_jets_schedule()
    df = extract_games(schedule_json)
    df = add_boxscore_fields(df)
    save_outputs(df)
    print(df.head())

if __name__ == "__main__":
    main()


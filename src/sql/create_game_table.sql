CREATE TABLE nba_basketball.game (
    game_id INTEGER PRIMARY KEY,
    game_date DATE,
    home_team_id INTEGER, -- FKs to team
    home_team_score INTEGER,
    visitor_team_id INTEGER, -- FKs to team
    visitor_team_score INTEGER,
    season INTEGER,
    post_season BOOLEAN,
    status TEXT
)
;

from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import boxscoreplayertrackv2
from pathlib import Path

import pandas as pd
import numpy as np
from tqdm import tqdm
import time as time
from time import sleep
from IPython.display import clear_output
import sqlite3

def add_tracking_boxscores(conn, start_season, end_season, if_exists='append'):
    """
    This function pulls tracking boxscores for players and teams from the NBA_API package 
    and appends (or creates a new table if not exists) it to the table player_tracking_boxscores or 
    team_tracking_boxscores in the sqlite db.
    
    Note: Because of timeout errors and that we have to pull each game's individually, each season takes 1-2 hours.
    If some games were not pulled in certain seasons, you can use the update functions to gather those individual games.
    """
    
    player_table_name = 'player_tracking_boxscores'
    team_table_name = 'team_tracking_boxscores'

    game_ids_not_added = []

    if if_exists == 'replace':
        conn.execute('DROP TABLE IF EXISTS ' + player_table_name)
        conn.execute('VACUUM')
    
    conn.execute('''CREATE TABLE IF NOT EXISTS {} (
                GAME_ID TEXT, TEAM_ID TEXT, TEAM_ABBREVIATION TEXT, 
                TEAM_CITY TEXT, PLAYER_ID TEXT, PLAYER_NAME TEXT,
                START_POSITION TEXT, COMMENT TEXT, MIN INTEGER, SPD FLOAT, 
                DIST FLOAT, ORBC INTEGER, DRBC INTEGER, RBC INTEGER, 
                TCHS INTEGER, SAST INTEGER, FTAST INTEGER, PASS INTEGER,
                AST INTEGER, CFGM INTEGER, CFGA INTEGER, CFG_PCT FLOAT,
                UFGM INTEGER, UFGA INTEGER, UFG_PCT FLOAT, FG_PCT FLOAT, 
                DFGM INTEGER, DFGA INTEGER, DFG_PCT FLOAT)'''.format(player_table_name))
        
    if if_exists == 'replace':
        conn.execute('DROP TABLE IF EXISTS ' + team_table_name)
        conn.execute('VACUUM')
    
    conn.execute('''CREATE TABLE IF NOT EXISTS {} (
                GAME_ID TEXT, TEAM_ID TEXT, TEAM_NAME TEXT, TEAM_ABBREVIATION TEXT, 
                TEAM_CITY TEXT, MIN INTEGER, DIST FLOAT, ORBC INTEGER, DRBC INTEGER, RBC INTEGER, 
                TCHS INTEGER, SAST INTEGER, FTAST INTEGER, PASS INTEGER,
                AST INTEGER, CFGM INTEGER, CFGA INTEGER, CFG_PCT FLOAT,
                UFGM INTEGER, UFGA INTEGER, UFG_PCT FLOAT, FG_PCT FLOAT, 
                DFGM INTEGER, DFGA INTEGER, DFG_PCT FLOAT)'''.format(team_table_name))    
    
    
    for season in range(start_season, end_season+1):

        for season_type in ['Regular Season', 'Playoffs']:
            logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
            game_ids = logs['GAME_ID'].unique()

            for i in range(0, len(game_ids), 100):
                print('games {} to {}'.format(i, i+100))
                for game_id in tqdm(game_ids[i:i+100], desc='progress'):
                    try:
                        tracking_boxscores = boxscoreplayertrackv2.BoxScorePlayerTrackV2(game_id).get_data_frames()
                        
                        team_tracking_boxscores = tracking_boxscores[1]
                        player_tracking_boxscores = tracking_boxscores[0]
                        
                        player_tracking_boxscores.to_sql(player_table_name, conn, if_exists='append', index=False)
                        team_tracking_boxscores.to_sql(team_table_name, conn, if_exists='append', index=False)
    
                    except:
                        game_ids_not_added.append(game_id)
                    sleep(2)
                sleep(120)
                clear_output(wait=True)

        sleep(60)
        
    cur = conn.cursor()
    
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY PLAYER_ID, GAME_ID)'.format(player_table_name, player_table_name))
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(team_table_name, team_table_name))
    
    conn.commit()
    
    return game_ids_not_added


if __name__ == '__main__':
    path_to_db = Path(Path.home(), 'nba_model_v1', 'data', 'nba.db')
    start_season = 2014
    end_season = 2021
    num_seasons = end_season-start_season+1
    connection = sqlite3.connect(path_to_db)

    start = time.time()
    games_not_added = add_tracking_boxscores(connection, start_season, end_season, if_exists='append')
    end = time.time()
    
    print('total mins for {} seasons:'.format(num_seasons), (end-start)/60)
    print('mins per season:', (end-start)/(60*num_seasons))
    connection.close()
    
    print(games_not_added)
    
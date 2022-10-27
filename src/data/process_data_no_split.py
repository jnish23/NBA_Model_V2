from typing import Union, List
import pandas as pd
import sqlite3
from pathlib import Path

def season_to_string(x):
    """Turns a season like 2021 to 2021-22
    Args:
        x (int): season

    Returns:
        str: stringified season
    """       
    
    return str(x) + '-' + str(x+1)[-2:]


def get_data_from_db_all(target, db_filepath, table = 'team_stats_ewa_matchup_prod'):
    """
    Gets features X and targets y from all available data to retrain model
        before making a new prediction

    Args:
        target (Union[str, list]): 'HOME_WL' for classification, ['HOME_TEAM_SCORE', 'AWAY_TEAM_SCORE'] for regression
        db_filepath (str): path to database

    Returns:
        X, y: X and y dataframes
    """        
    
    print(db_filepath)
    connection = sqlite3.connect(db_filepath)

    df = pd.read_sql(f'SELECT * FROM {table}', con=connection)
    df = df.drop(columns=['index'])
    connection.close()

    df = df.sort_values('GAME_DATE')

    df = df.dropna()

    columns_to_drop = ['SEASON', 'HOME_TEAM_ABBREVIATION', 'GAME_DATE', 'GAME_ID', 'MATCHUP',
                        'HOME_HOME_GAME', 'HOME_TEAM_SCORE', 'HOME_ML', 'HOME_SPREAD',
                        'HOME_ATS_DIFF', 'HOME_TEAM_COVERED', 'HOME_POINT_DIFF',
                        'HOME_WL', 'AWAY_ML', 'AWAY_TEAM_SCORE',
                        'HOME_PTS_L5', 'HOME_PTS_L10', 'HOME_PTS_L20',
                        'HOME_PLUS_MINUS_L5', 'HOME_PLUS_MINUS_L10', 'HOME_PLUS_MINUS_L20',
                        'HOME_NET_RATING_L5', 'HOME_NET_RATING_L10', 'HOME_NET_RATING_L20',
                        'HOME_POSS_L5', 'HOME_POSS_L10', 'HOME_POSS_L20',
                        'HOME_PTS_opp_L5', 'HOME_PTS_opp_L10', 'HOME_PTS_opp_L20',
                        'HOME_PLUS_MINUS_opp_L5', 'HOME_PLUS_MINUS_opp_L10', 'HOME_PLUS_MINUS_opp_L20',
                        'HOME_NET_RATING_opp_L5', 'HOME_NET_RATING_opp_L10', 'HOME_NET_RATING_opp_L20',
                        'HOME_POSS_opp_L5', 'HOME_POSS_opp_L10', 'HOME_POSS_opp_L20',
                        'HOME_REB_L5', 'HOME_REB_L10', 'HOME_REB_L20',  
                        'HOME_REB_opp_L5', 'HOME_REB_opp_L10', 'HOME_REB_opp_L20',       
                        'AWAY_PTS_L5', 'AWAY_PTS_L10', 'AWAY_PTS_L20',
                        'AWAY_PLUS_MINUS_L5', 'AWAY_PLUS_MINUS_L10', 'AWAY_PLUS_MINUS_L20',
                        'AWAY_NET_RATING_L5', 'AWAY_NET_RATING_L10', 'AWAY_NET_RATING_L20',
                        'AWAY_POSS_L5', 'AWAY_POSS_L10', 'AWAY_POSS_L20',
                        'AWAY_PTS_opp_L5', 'AWAY_PTS_opp_L10', 'AWAY_PTS_opp_L20',
                        'AWAY_PLUS_MINUS_opp_L5', 'AWAY_PLUS_MINUS_opp_L10', 'AWAY_PLUS_MINUS_opp_L20',
                        'AWAY_NET_RATING_opp_L5', 'AWAY_NET_RATING_opp_L10', 'AWAY_NET_RATING_opp_L20',
                        'AWAY_POSS_opp_L5', 'AWAY_POSS_opp_L10', 'AWAY_POSS_opp_L20',
                        'AWAY_REB_L5', 'AWAY_REB_L10', 'AWAY_REB_L20',
                        'AWAY_REB_opp_L5', 'AWAY_REB_opp_L10', 'AWAY_REB_opp_L20']


    X = df.drop(columns=columns_to_drop)
    y = df[target]

    return X, y, df

if __name__ == '__main__':
    target = 'HOME_WL'
    db_filepath = Path.home().joinpath('NBA_Model_v1', 'data', 'nba.db')
    get_data_from_db_all(target, db_filepath, table = 'team_stats_ewa_matchup_prod')
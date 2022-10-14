import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
import warnings


def load_team_data(conn, start_season, end_season):
    """Loads basic, advanced, and scoring boxscores 
    from sqlite db and merges them into one dataframe"""
    

    basic = pd.read_sql("SELECT * FROM team_basic_boxscores", conn)
    adv = pd.read_sql("SELECT * FROM team_advanced_boxscores", conn)
    scoring = pd.read_sql("SELECT * FROM team_scoring_boxscores", conn)
    tracking = pd.read_sql("SELECT * FROM team_tracking_boxscores", conn)

    basic = basic.loc[basic['SEASON'].between(start_season, end_season)]
    basic[['GAME_ID', 'TEAM_ID']] = basic[['GAME_ID', 'TEAM_ID']].astype(str)
    adv[['GAME_ID', 'TEAM_ID']] = adv[['GAME_ID', 'TEAM_ID']].astype(str)
    scoring[['GAME_ID', 'TEAM_ID']] = scoring[['GAME_ID', 'TEAM_ID']].astype(str)
    tracking[['GAME_ID', 'TEAM_ID']] = tracking[['GAME_ID', 'TEAM_ID']].astype(str)

    df = pd.merge(basic, adv, how='left', on=[
                    'GAME_ID', 'TEAM_ID'], suffixes=['', '_y'])
    df = pd.merge(df, scoring, how='left', on=[
                  'GAME_ID', 'TEAM_ID'], suffixes=['', '_y'])
    
    df = pd.merge(df, tracking, how='left', on=['GAME_ID', 'TEAM_ID'],
                  suffixes=['', '_y'])
    

    df = df.drop(columns=['TEAM_NAME_y', 'TEAM_CITY',
                          'TEAM_ABBREVIATION_y',
                          'TEAM_CITY_y', 'MIN_y',
                          'FG_PCT_y', 'AST_y'])
    
    return df


def clean_team_data(df):
    """This function cleans the team_data
    1) Changes W/L to 1/0 
    2) Changes franchise abbreviations to their most 
    recent abbreviation for consistency
    3) Converts GAME_DATE to datetime object
    4) Creates a binary column 'HOME_GAME'
    5) Removes 3 games where advanced stats were not collected
    """
    df = df.copy()
    df['WL'] = (df['WL'] == 'W').astype(int)

    abbr_mapping = {'NJN': 'BKN',
                    'CHH': 'CHA',
                    'VAN': 'MEM',
                    'NOH': 'NOP',
                    'NOK': 'NOP',
                    'SEA': 'OKC'}

    df['TEAM_ABBREVIATION'] = df['TEAM_ABBREVIATION'].replace(abbr_mapping)
    df['MATCHUP'] = df['MATCHUP'].str.replace('NJN', 'BKN')
    df['MATCHUP'] = df['MATCHUP'].str.replace('CHH', 'CHA')
    df['MATCHUP'] = df['MATCHUP'].str.replace('VAN', 'MEM')
    df['MATCHUP'] = df['MATCHUP'].str.replace('NOH', 'NOP')
    df['MATCHUP'] = df['MATCHUP'].str.replace('NOK', 'NOP')
    df['MATCHUP'] = df['MATCHUP'].str.replace('SEA', 'OKC')

    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])

    df['HOME_GAME'] = df['MATCHUP'].str.contains('vs').astype(int)

    return df


# from src.data.make_team_dataset import prep_for_aggregation

def prep_for_aggregation(df):
    """This function...
    1) Removes categories that are percentages,
    as we will be averaging them and do not want to average 
    percentages. 
    2) Converts shooting percentage stats into raw values"""
    df = df.copy()

    df = df.drop(columns=['FT_PCT', 'FG_PCT', 'FG3_PCT', 'DREB_PCT',
                          'OREB_PCT', 'REB_PCT', 'AST_PCT', 'AST_TOV',
                          'AST_RATIO', 'E_TM_TOV_PCT', 'TM_TOV_PCT',
                          'EFG_PCT', 'TS_PCT', 'USG_PCT', 'E_USG_PCT',
                          'PACE_PER40', 'MIN', 'PIE', 'CFG_PCT', 'UFG_PCT',
                          'DFG_PCT', 'E_OFF_RATING', 'E_DEF_RATING', 'E_NET_RATING'])

    df['FG2M'] = df['FGM'] - df['FG3M']
    df['FG2A'] = df['FGA'] - df['FG3A']
    df['PTS_2PT_MR'] = (df['PTS'] * df['PCT_PTS_2PT_MR']).astype('int8')
    df['PTS_FB'] = (df['PTS'] * df['PCT_PTS_FB']).astype('int8')
    df['PTS_OFF_TOV'] = (df['PTS'] * df['PCT_PTS_OFF_TOV']).astype('int8')
    df['PTS_PAINT'] = (df['PTS'] * df['PCT_PTS_PAINT']).astype('int8')
    df['AST_2PM'] = (df['FG2M'] * df['PCT_AST_2PM']).astype('int8')
    df['AST_3PM'] = (df['FG3M'] * df['PCT_AST_3PM']).astype('int8')
    df['UAST_2PM'] = (df['FG2M'] * df['PCT_UAST_2PM']).astype('int8')
    df['UAST_3PM'] = (df['FG3M'] * df['PCT_UAST_3PM']).astype('int8')

    df['POINT_DIFF'] = df['PLUS_MINUS']
    df['RECORD'] = df['WL']
    df['TEAM_SCORE'] = df['PTS']
    
    df = df.drop(columns = ['PCT_FGA_2PT', 'PCT_FGA_3PT', 'PCT_PTS_2PT',
                          'PCT_PTS_2PT_MR', 'PCT_PTS_3PT', 'PCT_PTS_FB',
                          'PCT_PTS_FT','PCT_PTS_OFF_TOV', 'PCT_PTS_PAINT', 
                          'PCT_AST_2PM', 'PCT_UAST_2PM','PCT_AST_3PM',
                          'PCT_UAST_3PM', 'PCT_AST_FGM', 'PCT_UAST_FGM',
                          'E_PACE'])
    
    ## Reorder Columns
    
    df = df[['SEASON', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID',
       'GAME_DATE', 'MATCHUP', 'TEAM_SCORE', 'WL', 'POINT_DIFF', 'HOME_GAME', 'RECORD',
       'FG2M', 'FG2A', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 
       'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS',
       'PLUS_MINUS', 'OFF_RATING', 'DEF_RATING', 'NET_RATING', 'PACE',
       'POSS', 'DIST', 'ORBC', 'DRBC', 'RBC', 'TCHS', 'SAST', 'FTAST', 'PASS',
       'CFGM', 'CFGA', 'UFGM', 'UFGA', 'DFGM', 'DFGA', 'PTS_2PT_MR', 'PTS_FB', 'PTS_OFF_TOV', 'PTS_PAINT', 'AST_2PM',
       'AST_3PM', 'UAST_2PM', 'UAST_3PM']]

    return df


def load_betting_data(conn):
    spreads = pd.read_sql("SELECT * FROM spreads", conn)
    moneylines = pd.read_sql("SELECT * FROM moneylines", conn)

    return spreads, moneylines


def convert_american_to_decimal(x):
    return np.where(x>0, (100+x)/100, 1+(100.0/-x))          


def clean_moneyline_df(df):
    abbr_mapping = {'Boston': 'BOS', 'Portland': 'POR',
                    'L.A. Lakers': 'LAL', 'Brooklyn': 'BKN',
                    'Cleveland': 'CLE', 'Toronto': 'TOR',
                    'Philadelphia': 'PHI', 'Memphis': 'MEM',
                    'Minnesota': 'MIN', 'New Orleans': 'NOP',
                    'Oklahoma City': 'OKC', 'Dallas': 'DAL',
                    'San Antonio': 'SAS', 'Denver': 'DEN',
                    'Golden State': 'GSW', 'L.A. Clippers': 'LAC',
                    'Orlando': 'ORL', 'Utah': 'UTA',
                    'Charlotte': 'CHA', 'Detroit': 'DET',
                    'Miami': 'MIA', 'Phoenix': 'PHX',
                    'Atlanta': 'ATL', 'New York': 'NYK',
                    'Indiana': 'IND', 'Chicago': 'CHI',
                    'Houston': 'HOU', 'Milwaukee': 'MIL',
                    'Sacramento': 'SAC', 'Washington': 'WAS'}

    df['HOME_TEAM'] = df['HOME_TEAM'].replace(abbr_mapping)
    df['AWAY_TEAM'] = df['AWAY_TEAM'].replace(abbr_mapping)

    away_mls = df['AWAY_ML'].str.split(",", expand=True)
    home_mls = df['HOME_ML'].str.split(",", expand=True)

    away_mls = away_mls.replace('-', np.nan).replace('', np.nan)
    away_mls = away_mls.fillna(value=np.nan)
    away_mls = away_mls.astype(float)

    home_mls = home_mls.replace('-', np.nan).replace('', np.nan)
    home_mls = home_mls.fillna(value=np.nan)
    home_mls = home_mls.astype(float)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
    
        highest_away_ml = away_mls.apply(lambda row: np.nanmax(
            abs(row)) if np.nanmax(row) > 0 else -np.nanmax(abs(row)), axis=1)
        highest_away_ml = convert_american_to_decimal(highest_away_ml)
        highest_away_ml = pd.DataFrame(
            highest_away_ml, columns=['HIGHEST_AWAY_ML'])

        highest_home_ml = home_mls.apply(lambda row: np.nanmax(
            abs(row)) if np.nanmax(row) > 0 else -np.nanmax(abs(row)), axis=1)
        highest_home_ml = convert_american_to_decimal(highest_home_ml)
        highest_home_ml = pd.DataFrame(
            highest_home_ml, columns=['HIGHEST_HOME_ML'])

    moneylines = pd.concat(
        [df.iloc[:, :4], highest_home_ml, highest_away_ml], axis=1)
    
    moneylines['GM_DATE'] = pd.to_datetime(moneylines['GM_DATE'])

    return moneylines

def clean_spreads_df(df):
    abbr_mapping = {'Boston': 'BOS', 'Portland': 'POR',
                    'L.A. Lakers': 'LAL', 'Brooklyn': 'BKN',
                    'Cleveland': 'CLE', 'Toronto': 'TOR',
                    'Philadelphia': 'PHI', 'Memphis': 'MEM',
                    'Minnesota': 'MIN', 'New Orleans': 'NOP',
                    'Oklahoma City': 'OKC', 'Dallas': 'DAL',
                    'San Antonio': 'SAS', 'Denver': 'DEN',
                    'Golden State': 'GSW', 'L.A. Clippers': 'LAC',
                    'Orlando': 'ORL', 'Utah': 'UTA',
                    'Charlotte': 'CHA', 'Detroit': 'DET',
                    'Miami': 'MIA', 'Phoenix': 'PHX',
                    'Atlanta': 'ATL', 'New York': 'NYK',
                    'Indiana': 'IND', 'Chicago': 'CHI',
                    'Houston': 'HOU', 'Milwaukee': 'MIL',
                    'Sacramento': 'SAC', 'Washington': 'WAS'}

    df['HOME_TEAM'] = df['HOME_TEAM'].replace(abbr_mapping)
    df['AWAY_TEAM'] = df['AWAY_TEAM'].replace(abbr_mapping)

    away_spreads = df['AWAY_SPREAD'].str.split(",", expand=True)
    home_spreads = df['HOME_SPREAD'].str.split(",", expand=True)

    for col in away_spreads.columns:
        away_spreads[col] = away_spreads[col].str[:-4]
        away_spreads[col] = away_spreads[col].str.replace('½', '.5')
        away_spreads[col] = away_spreads[col].str.replace('PK', '0')

        away_spreads[col] = away_spreads[col].astype(str).apply(
            lambda x: x if x == '' else (x[:-1] if x[-1] == '-' else x))

    away_spreads = away_spreads.replace('-', np.nan)
    away_spreads = away_spreads.replace('', np.nan)
    away_spreads = away_spreads.replace('None', np.nan)
    away_spreads = away_spreads.fillna(value=np.nan)

    away_spreads = away_spreads.astype(float)

    for col in home_spreads.columns:
        home_spreads[col] = home_spreads[col].str[:-4]
        home_spreads[col] = home_spreads[col].str.replace('½', '.5')
        home_spreads[col] = home_spreads[col].str.replace('PK', '0')

        home_spreads[col] = home_spreads[col].astype(str).apply(
            lambda x: x if x == '' else (x[:-1] if x[-1] == '-' else x))

    home_spreads = home_spreads.replace('-', np.nan).replace('', np.nan).replace('None', np.nan)
    home_spreads = home_spreads.fillna(value=np.nan)

    home_spreads = home_spreads.astype(float)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)

        highest_away_spread = away_spreads.apply(
            lambda row: -np.nanmax(abs(row)) if np.nanmax(row) < 0 else np.nanmax(abs(row)), axis=1)
        highest_away_spread = pd.DataFrame(
            highest_away_spread, columns=['HIGHEST_AWAY_SPREAD'])

        highest_home_spread = home_spreads.apply(
            lambda row: -np.nanmax(abs(row)) if np.nanmax(row) < 0 else np.nanmax(abs(row)), axis=1)
        highest_home_spread = pd.DataFrame(
            highest_home_spread, columns=['HIGHEST_HOME_SPREAD'])

    spreads = pd.concat(
        [df.iloc[:, :4], highest_home_spread, highest_away_spread], axis=1)
    spreads['GM_DATE'] = pd.to_datetime(spreads['GM_DATE'])

    return spreads


def merge_betting_and_boxscore_data(clean_spreads, clean_mls, clean_boxscores):
    clean_boxscores['HOME_TEAM'] = clean_boxscores['MATCHUP'].apply(
        lambda x: x[:3] if 'vs' in x else x[-3:])
    clean_boxscores['AWAY_TEAM'] = clean_boxscores['MATCHUP'].apply(
        lambda x: x[:3] if '@' in x else x[-3:])

    temp = pd.merge(clean_mls, clean_spreads, on=[
                    'SEASON', 'GM_DATE', 'HOME_TEAM', 'AWAY_TEAM'])

    merged_df = pd.merge(clean_boxscores, temp, how='left', 
                         left_on=['SEASON', 'HOME_TEAM', 'AWAY_TEAM', 'GAME_DATE'],
                         right_on=['SEASON', 'HOME_TEAM', 'AWAY_TEAM', 'GM_DATE'])

    merged_df['ML'] = merged_df.apply(lambda row: row['HIGHEST_HOME_ML'] if row['HOME_GAME'] == 1
                                      else row['HIGHEST_AWAY_ML'], axis=1)

    merged_df['SPREAD'] = merged_df.apply(lambda row: row['HIGHEST_HOME_SPREAD'] if row['HOME_GAME'] == 1
                                          else -row['HIGHEST_HOME_SPREAD'], axis=1)

    merged_df = merged_df.drop(columns=['HOME_TEAM', 'AWAY_TEAM', 'GM_DATE',
                                        'HIGHEST_HOME_ML', 'HIGHEST_AWAY_ML',
                                        'HIGHEST_HOME_SPREAD', 'HIGHEST_AWAY_SPREAD'])

    merged_df['ATS_DIFF'] = merged_df['POINT_DIFF'] + merged_df['SPREAD']

    merged_df['TEAM_COVERED'] = (merged_df['ATS_DIFF'] > 0).astype(int)
    

    return merged_df


def normalize_per_100_poss(df):
    df = df.copy(deep=True)
    
    df.iloc[:, 12:27] = 100*df.iloc[:, 12:27].div(df['PACE'], axis=0) 
    df.iloc[:,  34:-4] = 100*df.iloc[:, 34:-4].div(df['PACE'], axis=0) 
    
    return df


def create_matchups(df):
    """This function makes each row a matchup between 
    team and opp"""
    df = df.copy()
    

    matchups = pd.merge(df, df.iloc[:, :-4], on=['GAME_ID'], suffixes=['', '_opp'])
    matchups = matchups.loc[matchups['TEAM_ABBREVIATION'] != matchups['TEAM_ABBREVIATION_opp']]

    matchups = matchups.drop(columns = ['SEASON_opp', 'TEAM_ABBREVIATION_opp', 'GAME_DATE_opp',
                                        'MATCHUP_opp', 'HOME_GAME_opp', 'TEAM_NAME_opp', 
                                        'TEAM_ID_opp', 'WL_opp']
                             )
    
    matchups
    
    return matchups


def build_team_avg_stats_df(df: pd.DataFrame, span = 10) -> pd.DataFrame:    
    """This function finds the average for each team and opp statistic up to (and NOT including) the given date.
    """
    
    df = df.copy(deep=True)

    df = df.sort_values(['TEAM_ABBREVIATION', 'GAME_DATE']).reset_index(drop=True)

    

    drop_cols = ['TEAM_ID', 'TEAM_NAME', 'GAME_ID', 'MATCHUP', 
                 'HOME_GAME', 'TEAM_SCORE', 'ML', 'SPREAD', 
                'GAME_DATE', 'POINT_DIFF', 'WL', 'TEAM_SCORE_opp',
                'POINT_DIFF_opp', 'RECORD', 'RECORD_opp', 'TEAM_COVERED']

    stats = df.drop(columns=drop_cols)

    avg_stat_holder = []

    for stat in stats.columns[2:]:
        avg_stats = stats.groupby(['TEAM_ABBREVIATION'])[stat].ewm(span=span).mean().reset_index(drop=True)
        avg_stat_holder.append(avg_stats)
    
    
    matchup_info = df[['SEASON', 'TEAM_ABBREVIATION', 'GAME_DATE',
                          'GAME_ID', 'MATCHUP', 'HOME_GAME', 'TEAM_SCORE',
                          'ML', 'SPREAD', 'ATS_DIFF', 'RECORD', 'TEAM_COVERED', 
                          'POINT_DIFF', 'WL']]   

    avg_stats = pd.concat(avg_stat_holder, axis=1)
    

    avg_stats = avg_stats.rename(columns={'ATS_DIFF':'AVG_ATS_DIFF'})
    
    avg_stats = pd.concat([matchup_info, avg_stats], axis=1)
    
    avg_stats['WIN_PCT'] = avg_stats.groupby(['TEAM_ABBREVIATION'])['RECORD'].rolling(window=span).mean().values
    avg_stats['COVER_PCT'] = avg_stats.groupby(['TEAM_ABBREVIATION'])['TEAM_COVERED'].rolling(window=span).mean().values

    avg_stats = avg_stats.drop(columns='RECORD')

    avg_stats = avg_stats.sort_values(['TEAM_ABBREVIATION', 'GAME_DATE'])
    avg_stats.iloc[:, 14:] = avg_stats.iloc[:, 14:].shift(1).where(avg_stats['TEAM_ABBREVIATION'].eq(avg_stats['TEAM_ABBREVIATION'].shift()))

    avg_stats = avg_stats.add_suffix('_L{}'.format(span))
    
    avg_stats = avg_stats.rename(columns = {'SEASON_L{}'.format(span):'SEASON',
                                           'TEAM_ABBREVIATION_L{}'.format(span):'TEAM_ABBREVIATION',
                                           'GAME_DATE_L{}'.format(span):'GAME_DATE',
                                           'GAME_ID_L{}'.format(span):'GAME_ID',
                                           'MATCHUP_L{}'.format(span): 'MATCHUP', 
                                           'HOME_GAME_L{}'.format(span): 'HOME_GAME', 
                                           'TEAM_SCORE_L{}'.format(span):'TEAM_SCORE',
                                           'ML_L{}'.format(span):'ML', 
                                           'SPREAD_L{}'.format(span):'SPREAD',
                                           'ATS_DIFF_L{}'.format(span):'ATS_DIFF',
                                           'RECORD_L{}'.format(span):'RECORD', 
                                           'TEAM_COVERED_L{}'.format(span):'TEAM_COVERED',
                                           'POINT_DIFF_L{}'.format(span):'POINT_DIFF',
                                           'WL_L{}'.format(span):'WL'})
    
    return avg_stats


def add_percentage_features(df, span):
    """Add the following features for both team and opp:
    OREB_PCT, DREB_PCT, REB_PCT, TS_PCT, EFG_PCT, AST_RATIO, TOV_PCT, PIE.
    """
    
    df = df.copy()
    
    df['OREB_PCT_L{}'.format(span)] = df['OREB_L{}'.format(span)] / (df['OREB_L{}'.format(span)] + df['DREB_opp_L{}'.format(span)])
    df['OREB_PCT_opp_L{}'.format(span)] = df['OREB_opp_L{}'.format(span)] / (df['OREB_opp_L{}'.format(span)] + df['DREB_L{}'.format(span)])

    df['DREB_PCT_L{}'.format(span)] = df['DREB_L{}'.format(span)] / (df['DREB_L{}'.format(span)] + df['OREB_opp_L{}'.format(span)])
    df['DREB_PCT_opp_L{}'.format(span)] = df['DREB_opp_L{}'.format(span)] / (df['DREB_opp_L{}'.format(span)] + df['OREB_L{}'.format(span)])

    df['REB_PCT_L{}'.format(span)] = df['REB_L{}'.format(span)] / (df['REB_L{}'.format(span)] + df['REB_opp_L{}'.format(span)])
    df['REB_PCT_opp_L{}'.format(span)] = df['REB_opp_L{}'.format(span)] / (df['REB_opp_L{}'.format(span)] + df['REB_L{}'.format(span)])

    df['TS_PCT_L{}'.format(span)] = df['PTS_L{}'.format(span)] / ((2*(df['FG2A_L{}'.format(span)] + df['FG3A_L{}'.format(span)]) + 0.44*df['FTA_L{}'.format(span)]))
    
    df['TS_PCT_opp_L{}'.format(span)] = df['PTS_opp_L{}'.format(span)] / ((2*(df['FG2A_opp_L{}'.format(span)] + df['FG3A_opp_L{}'.format(span)]) + 0.44*df['FTA_opp_L{}'.format(span)]))

    df['EFG_PCT_L{}'.format(span)] = (df['FG2M_L{}'.format(span)] + 1.5*df['FG3M_L{}'.format(span)]) / (df['FG2A_L{}'.format(span)]
                                                                    + df['FG3A_L{}'.format(span)])
    df['EFG_PCT_opp_L{}'.format(span)] = (df['FG2M_opp_L{}'.format(span)] + 1.5*df['FG3M_opp_L{}'.format(span)]) / (df['FG2A_opp_L{}'.format(span)] 
                                                                 + df['FG3A_opp_L{}'.format(span)])

    df['AST_RATIO_L{}'.format(span)] = (df['AST_L{}'.format(span)] * 100) / df['PACE_L{}'.format(span)]
    df['AST_RATIO_opp_L{}'.format(span)] = (df['AST_opp_L{}'.format(span)] * 100) / df['PACE_opp_L{}'.format(span)]

    df['TOV_PCT_L{}'.format(span)] = 100*df['TOV_L{}'.format(span)] / (df['FG2A_L{}'.format(span)] 
                                               + df['FG3A_L{}'.format(span)] 
                                               + 0.44*df['FTA_L{}'.format(span)] 
                                               + df['TOV_L{}'.format(span)])
    
    df['TOV_PCT_opp_L{}'.format(span)] = 100*df['TOV_opp_L{}'.format(span)] / (df['FG2A_opp_L{}'.format(span)] 
                                             + df['FG3A_opp_L{}'.format(span)] 
                                             + 0.44*df['FTA_opp_L{}'.format(span)] 
                                             + df['TOV_opp_L{}'.format(span)])
    
    
    df['PIE_L{}'.format(span)] = ((df['PTS_L{}'.format(span)] + df['FG2M_L{}'.format(span)] + df['FG3M_L{}'.format(span)] + df['FTM_L{}'.format(span)] 
                 - df['FG2A_L{}'.format(span)] - df['FG3A_L{}'.format(span)] - df['FTA_L{}'.format(span)] 
                 + df['DREB_L{}'.format(span)] + df['OREB_L{}'.format(span)]/2
                + df['AST_L{}'.format(span)] + df['STL_L{}'.format(span)] + df['BLK_L{}'.format(span)]/2
                - df['PF_L{}'.format(span)] - df['TOV_L{}'.format(span)]) 
                 / (df['PTS_L{}'.format(span)] + df['PTS_opp_L{}'.format(span)] + df['FG2M_L{}'.format(span)] + df['FG2M_opp_L{}'.format(span)]
                   + df['FG3M_L{}'.format(span)] + df['FG3M_opp_L{}'.format(span)] + df['FTM_L{}'.format(span)] + df['FTM_opp_L{}'.format(span)]
                   - df['FG2A_L{}'.format(span)] - df['FG2A_opp_L{}'.format(span)] - df['FG3A_L{}'.format(span)] - df['FG3A_opp_L{}'.format(span)] 
                    - df['FTA_L{}'.format(span)] - df['FTA_opp_L{}'.format(span)] + df['DREB_L{}'.format(span)] + df['DREB_opp_L{}'.format(span)]
                    + (df['OREB_L{}'.format(span)]+df['OREB_opp_L{}'.format(span)])/2 + df['AST_L{}'.format(span)] + df['AST_opp_L{}'.format(span)]
                    + df['STL_L{}'.format(span)] + df['STL_opp_L{}'.format(span)] + (df['BLK_L{}'.format(span)] + df['BLK_opp_L{}'.format(span)])/2
                    - df['PF_L{}'.format(span)] - df['PF_opp_L{}'.format(span)] - df['TOV_L{}'.format(span)] - df['TOV_opp_L{}'.format(span)]))
        
    return df


def add_rest_days(df):
    
    df['prev_game'] = df.groupby(['SEASON', 'TEAM_ABBREVIATION'])['GAME_DATE'].shift(1)

    df['REST'] = (df['GAME_DATE'] - df['prev_game']) / np.timedelta64(1, 'D')
            
    df.loc[df['REST'] >= 8, 'REST'] = 8
    
    df = df.drop(columns=['prev_game'])
    
    return df


def make_matchups_2(df):
    df = df.copy()
    
    home_teams = df.loc[df['HOME_GAME'] == 1]
    home_teams = home_teams.add_prefix('HOME_')

    away_teams = df.loc[df['HOME_GAME'] == 0]
    away_teams = away_teams.add_prefix('AWAY_')


    away_teams = away_teams.drop(columns=['AWAY_SEASON', 'AWAY_TEAM_ABBREVIATION', 'AWAY_GAME_DATE',
                            'AWAY_MATCHUP', 'AWAY_HOME_GAME', 'AWAY_POINT_DIFF', 'AWAY_WL',
                            'AWAY_ATS_DIFF', 'AWAY_SPREAD', 'AWAY_TEAM_COVERED'])

    full_matchup_ewa = pd.merge(home_teams, away_teams, how = 'inner',
                                left_on = 'HOME_GAME_ID', right_on = 'AWAY_GAME_ID') 
    
    full_matchup_ewa = full_matchup_ewa.rename(columns = {'HOME_SEASON':'SEASON',
                                                        'HOME_GAME_DATE':'GAME_DATE',
                                                        'HOME_GAME_ID':'GAME_ID',
                                                        'HOME_MATCHUP':'MATCHUP'})
    
    full_matchup_ewa = full_matchup_ewa.drop(columns=['AWAY_GAME_ID'])
    
    return full_matchup_ewa

def season_to_string(x):
    return str(x) + '-' + str(x+1)[-2:]


def etl_pipeline(start_season = 2013, end_season = 2021, table_name = 'team_stats_ewa_matchup'):
    start_season = season_to_string(start_season)
    end_season = season_to_string(end_season)

    db_filepath = Path.home().joinpath('NBA_model_v1', 'data', 'nba.db')

    conn = sqlite3.connect(db_filepath)
    
    print("Loading raw team boxscore data from sql database...")
    
    df = load_team_data(conn, start_season, end_season)
    print("Loading betting data from sql database...")
    spreads, moneylines = load_betting_data(conn)
    
    print("Cleaning Data...")
    
    df = clean_team_data(df)
    df = prep_for_aggregation(df)

    clean_mls = clean_moneyline_df(df = moneylines)
    clean_spreads = clean_spreads_df(df = spreads)
    
    
    print("Merging Boxscore and Betting Data...")
    merged_df = merge_betting_and_boxscore_data(
        clean_spreads, clean_mls, clean_boxscores = df)
    
    
    stats_per_100 = normalize_per_100_poss(merged_df)

    print("Aggregating over last 5, 10, and 20 game windows")
    
    matchups = create_matchups(stats_per_100)
    
    team_stats_ewa_5 = build_team_avg_stats_df(matchups, span=5)
    team_stats_ewa_5 = add_percentage_features(team_stats_ewa_5, span=5)

    team_stats_ewa_10 = build_team_avg_stats_df(matchups, span=10)
    team_stats_ewa_10 = add_percentage_features(team_stats_ewa_10, span=10)

    team_stats_ewa_20 = build_team_avg_stats_df(matchups, span=20)
    team_stats_ewa_20 = add_percentage_features(team_stats_ewa_20, span=20)


    temp = pd.merge(team_stats_ewa_5, team_stats_ewa_10, how='inner',
                    on=['SEASON', 'TEAM_ABBREVIATION', 'GAME_DATE',
                        'GAME_ID', 'MATCHUP', 'HOME_GAME', 'TEAM_SCORE',
                        'ML', 'SPREAD', 'ATS_DIFF', 'TEAM_COVERED', 
                        'POINT_DIFF', 'WL'])

    df_full = pd.merge(temp, team_stats_ewa_20, how='inner', 
                       on=['SEASON', 'TEAM_ABBREVIATION', 'GAME_DATE',
                            'GAME_ID', 'MATCHUP', 'HOME_GAME', 'TEAM_SCORE',
                            'ML', 'SPREAD', 'ATS_DIFF', 'TEAM_COVERED', 
                            'POINT_DIFF', 'WL'])

    df_full = df_full.sort_values(['GAME_DATE', 'GAME_ID', 'HOME_GAME'])
    
    
    print("adding rest days")
    df_full = add_rest_days(df_full)

    print("creating matchups between Home and Away team aggregated stats")
    df_full = make_matchups_2(df_full)

    print("Resorting by date")
    df_full = df_full.sort_values(['GAME_DATE', 'GAME_ID', 'HOME_HOME_GAME'])

    print("dropping nulls")
    df_full = df_full.dropna()
    
    print("loading table back into sql db as {}".format(table_name))
    df_full.to_sql(table_name, con = conn, if_exists='append')
    
    cur = conn.cursor()
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY HOME_TEAM_ABBREVIATION, GAME_ID)'.format(table_name, table_name))
    conn.commit()
    conn.close()
    

if __name__ == '__main__':
    start_season = 2013
    end_season = 2021
    processed_data_table_name = 'team_stats_ewa_matchup'
    
    etl_pipeline(start_season = start_season, end_season = end_season, table_name = processed_data_table_name)
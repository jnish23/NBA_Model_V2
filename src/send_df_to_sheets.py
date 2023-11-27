import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
import sqlite3
import numpy as np



def create_results_df():
    s = 2023
    path_to_results = Path().home().joinpath('NBA_Model_v1', 'results', f'betting_predictions_{s}.csv')
    path_to_db = Path.home().joinpath('NBA_Model_v1', 'data', 'nba.db')
    season = '2023-24'
        
    preds = pd.read_csv(path_to_results)
    preds = preds.drop_duplicates(subset=['home_team', 'away_team', 'game_date'], keep='last')

    connection = sqlite3.connect(path_to_db)
    scores = pd.read_sql(f"""SELECT a.SEASON
                            ,a.GAME_DATE
                            ,a.TEAM_ABBREVIATION AS HOME_TEAM_ABBREVIATION
                            ,b.TEAM_ABBREVIATION AS AWAY_TEAM_ABBREVIATION
                            ,a.PTS AS HOME_TEAM_SCORE
                            ,b.PTS AS AWAY_TEAM_SCORE 
                        FROM team_basic_boxscores a
                        JOIN team_basic_boxscores b
                        ON a.GAME_ID = b.GAME_ID
                        WHERE a.MATCHUP like '%vs%'
                        and a.TEAM_ABBREVIATION != b.TEAM_ABBREVIATION
                        and a.SEASON = '{season}'""", con = connection)

    scores['GAME_DATE'] = pd.to_datetime(scores['GAME_DATE']).astype(str)

    merged = pd.merge(scores, preds, how='outer', 
                    left_on = ['HOME_TEAM_ABBREVIATION', 'GAME_DATE'],
                    right_on = ['home_team', 'game_date'])
    
    merged = merged[['home_team', 'away_team', 'game_date', 'home_spread', 'home_moneylines',
       'away_moneylines', 'OU', 'sgd_home_score_pred', 'sgd_away_score_pred',
       'lgb_home_score_pred', 'lgb_away_score_pred', 'home_win_prob_sgd_hinge',
       'home_win_prob_sgd_logloss', 'away_win_prob_sgd_logloss',
       'home_win_prob_lgbc', 'away_win_prob_lgbc', 'SEASON', 'GAME_DATE',
       'HOME_TEAM_ABBREVIATION', 'AWAY_TEAM_ABBREVIATION', 'HOME_TEAM_SCORE',
       'AWAY_TEAM_SCORE']]

    merged['SGD_ATS_DIFF'] = merged['sgd_home_score_pred'] - merged['sgd_away_score_pred'] + merged['home_spread']
    merged['LGB_ATS_DIFF'] = merged['lgb_home_score_pred'] - merged['lgb_away_score_pred'] + merged['home_spread']

    merged['SGD_ATS_BET_HOME'] = (merged['SGD_ATS_DIFF']>0).astype(int)
    merged['LGB_ATS_BET_HOME'] = (merged['LGB_ATS_DIFF']>0).astype(int)

    merged['HOME_SCORE_DIFF'] = merged['HOME_TEAM_SCORE'] - merged['AWAY_TEAM_SCORE']

    merged['HOME_WIN'] = (merged['HOME_SCORE_DIFF']>0).astype(int)

    merged['HOME_COVER'] = ((merged['HOME_SCORE_DIFF'] + merged['home_spread']) > 0).astype(int)
    merged.loc[(merged['HOME_SCORE_DIFF'] + merged['home_spread']) == 0, 'HOME_COVER'] = np.nan

    merged['SGD_ATS_BET_RESULT'] = (merged['HOME_COVER'] == merged['SGD_ATS_BET_HOME']).astype(int)
    merged['LGB_ATS_BET_RESULT'] = (merged['HOME_COVER'] == merged['LGB_ATS_BET_HOME']).astype(int)
    

    merged.loc[merged['HOME_COVER'].isnull(), 'SGD_ATS_BET_RESULT'] = np.nan
    merged.loc[merged['HOME_COVER'].isnull(), 'LGB_ATS_BET_RESULT'] = np.nan

    merged['SGD_HINGE_ML_BET_RESULT'] = (merged['home_win_prob_sgd_hinge'] == merged['HOME_WIN']).astype(int)
    merged['SGD_LOGLOSS_ML_BET_RESULT'] = (merged['home_win_prob_sgd_logloss'].round() == merged['HOME_WIN']).astype(int)
    merged['LGB_ML_BET_RESULT'] = (merged['home_win_prob_lgbc'].round() == merged['HOME_WIN']).astype(int)


    merged.loc[merged['HOME_SCORE_DIFF'].isnull(), ['HOME_WIN', 'HOME_COVER', 'SGD_ATS_BET_RESULT',
                                                    'LGB_ATS_BET_RESULT', 'SGD_HINGE_ML_BET_RESULT',
                                                    'SGD_LOGLOSS_ML_BET_RESULT', 'LGB_ML_BET_RESULT']] = np.nan

    merged['SGD_TOTAL_PRED'] = merged['sgd_home_score_pred'] + merged['sgd_away_score_pred']
    merged['LGB_TOTAL_PRED'] = merged['lgb_home_score_pred'] + merged['lgb_away_score_pred']

    merged['SGD_BET_OVER'] = (merged['SGD_TOTAL_PRED'] > merged['OU']).astype(int)
    merged['LGB_BET_OVER'] = (merged['LGB_TOTAL_PRED'] > merged['OU']).astype(int)
         
    merged['POINT_TOTAL'] = merged['HOME_TEAM_SCORE'] + merged['AWAY_TEAM_SCORE']
    merged['OVER_RESULT'] = (merged['POINT_TOTAL'] > merged['OU']).astype(int)
    merged.loc[merged['POINT_TOTAL'].isnull(), 'OVER_RESULT'] = np.nan
    
    merged.loc[(merged['POINT_TOTAL'] == merged['OU']), 'OVER_RESULT'] = np.nan

    merged['SGD_BET_OVER_RESULT'] = (merged['SGD_BET_OVER'] == merged['OVER_RESULT']).astype(int)
    merged['LGB_BET_OVER_RESULT'] = (merged['LGB_BET_OVER'] == merged['OVER_RESULT']).astype(int)
    
    merged.loc[(merged['POINT_TOTAL'].isnull()), ['OVER_RESULT', 'SGD_BET_OVER_RESULT', 'LGB_BET_OVER_RESULT']] = np.nan
    
    merged.loc[merged['sgd_home_score_pred'].isnull(), ['SGD_ATS_BET_HOME', 'SGD_ATS_BET_RESULT', 'SGD_BET_OVER', 'SGD_BET_OVER_RESULT']] = np.nan
    merged.loc[merged['lgb_home_score_pred'].isnull(), ['LGB_ATS_BET_HOME', 'LGB_ATS_BET_RESULT', 'LGB_BET_OVER', 'LGB_BET_OVER_RESULT']] = np.nan


    merged = merged.drop(columns = ['GAME_DATE', 'SEASON', 'HOME_TEAM_ABBREVIATION'])
    
    return merged

# ########

def send_to_google_sheets(df):
    season_year = 2023
        
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    path_to_key = Path().home().joinpath('NBA_model_v1', 'src', 'nba-model-314520-a7e4b87dbdb6.json')

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        path_to_key, scope)

    gc = gspread.authorize(credentials)


    spreadsheet_key = '1rA6wzNbW2CJwhJ9HI-CI3R_WLw3eNJzcEKBkH9wJpJ4'

    wks_name = f'Model_Predictions_{season_year}'
   

    d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials,
            row_names=True)

def main():
    results = create_results_df()
    send_to_google_sheets(results)
    
    return None

if __name__ == '__main__':
    main()
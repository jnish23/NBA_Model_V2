import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
import sqlite3
import numpy as np

path_to_results = Path().home().joinpath('NBA_Model_v1', 'results', 'betting_predictions_2022.csv')
path_to_db = Path.home().joinpath('NBA_Model_v1', 'data', 'nba.db')
season = '2022-23'



preds = pd.read_csv(path_to_results)
preds = preds.drop_duplicates(subset=['home_team', 'away_team', 'game_date'], keep='last')

connection = sqlite3.connect(path_to_db)
scores = pd.read_sql(f"""SELECT SEASON, GAME_DATE, HOME_TEAM_ABBREVIATION, MATCHUP, HOME_TEAM_SCORE, AWAY_TEAM_SCORE 
                    FROM team_stats_ewa_matchup
                    WHERE SEASON = '{season}'""", con = connection)

scores['GAME_DATE'] = pd.to_datetime(scores['GAME_DATE']).astype(str)

merged = pd.merge(preds, scores, how='left', 
                  left_on = ['home_team', 'game_date'],
                  right_on = ['HOME_TEAM_ABBREVIATION', 'GAME_DATE'])

merged['SGD_ATS_BET_HOME'] = ((merged['sgd_home_score_pred'] - merged['sgd_away_score_pred'] + merged['home_spread'])>0).astype(int)
merged['LGB_ATS_BET_HOME'] = ((merged['lgb_home_score_pred'] - merged['lgb_away_score_pred'] + merged['home_spread'])>0).astype(int)

merged['HOME_SCORE_DIFF'] = merged['HOME_TEAM_SCORE'] - merged['AWAY_TEAM_SCORE']
merged['HOME_WIN'] = (merged['HOME_SCORE_DIFF']>0).astype(int)

merged['HOME_COVER'] = ((merged['HOME_SCORE_DIFF'] + merged['home_spread']) > 0).astype(int)
merged.loc[(merged['HOME_SCORE_DIFF'] + merged['home_spread']) == 0, 'HOME_COVER'] = np.nan

merged['SGD_ATS_BET_RESULT'] = (merged['HOME_COVER'] == merged['SGD_ATS_BET_HOME']).astype(int)
merged['LGB_ATS_BET_RESULT'] = (merged['HOME_COVER'] == merged['LGB_ATS_BET_HOME']).astype(int)


merged['SGD_HINGE_ML_BET_RESULT'] = (merged['home_win_prob_sgd_hinge'] == merged['HOME_WIN']).astype(int)
merged['SGD_LOGLOSS_ML_BET_RESULT'] = (merged['home_win_prob_sgd_logloss'].round() == merged['HOME_WIN']).astype(int)
merged['LGB_ML_BET_RESULT'] = (merged['home_win_prob_lgbc'].round() == merged['HOME_WIN']).astype(int)


merged.loc[merged['HOME_SCORE_DIFF'].isnull(), ['HOME_WIN', 'HOME_COVER', 'SGD_ATS_BET_RESULT',
                                                'LGB_ATS_BET_RESULT', 'SGD_HINGE_ML_BET_RESULT',
                                                'SGD_LOGLOSS_ML_BET_RESULT', 'LGB_ML_BET_RESULT']] = np.nan


merged = merged.drop(columns = ['GAME_DATE', 'SEASON', 'HOME_TEAM_ABBREVIATION', 'MATCHUP'])

########


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

path_to_key = Path().home().joinpath('NBA_model_v1', 'results', 'nba-model-314520-a7e4b87dbdb6.json')

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    path_to_key, scope)

gc = gspread.authorize(credentials)


spreadsheet_key = '15NYLE7EIyZtT4MWpDAInAcnPo5cw4wPj5SsAks512BE'

wks_name = 'Model_Predictions_2022'

d2g.upload(merged, spreadsheet_key, wks_name, credentials=credentials,
           row_names=True)


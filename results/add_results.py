import pandas as pd
import sqlite3
from pathlib import Path

path_to_db = Path.home().joinpath('NBA_Model_v1', 'data', 'nba.db')

season = '2022-23'

connection = sqlite3.connect(path_to_db)

# print(pd.read_sql('''SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name''', con = connection))

df = pd.read_sql(f"""SELECT SEASON, GAME_DATE, HOME_TEAM_ABBREVIATION, MATCHUP, HOME_TEAM_SCORE, AWAY_TEAM_SCORE 
                    FROM team_stats_ewa_matchup
                    WHERE SEASON = '{season}'""", con = connection)

df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE']).astype(str)

path_to_results = Path.home().joinpath('NBA_Model_v1', 'results', 'betting_predictions_2022.csv')
results = pd.read_csv(path_to_results)

merged = pd.merge(results, df, how='left', 
                  left_on = ['home_team', 'game_date'],
                  right_on = ['HOME_TEAM_ABBREVIATION', 'GAME_DATE'])

merged['SGD_ATS_BET_HOME'] = ((merged['sgd_home_score_pred'] - merged['sgd_away_score_pred'] + merged['home_spread'])>0).astype(int)
merged['LGB_ATS_BET_HOME'] = ((merged['lgb_home_score_pred'] - merged['lgb_away_score_pred'] + merged['home_spread'])>0).astype(int)

merged['SCORE_DIFF'] = merged['HOME_TEAM_SCORE'] - merged['AWAY_TEAM_SCORE']
merged['HOME_WIN'] = (merged['SCORE_DIFF']>0).astype(int)

merged['HOME_ATS_COVER'] = ((merged['SCORE_DIFF'] + merged['home_spread']) > 0).astype(int)

merged['SGD_ATS_RESULT'] = (merged['HOME_ATS_COVER'] == merged['SGD_ATS_BET_HOME']).astype(int)
merged['LGB_ATS_RESULT'] = (merged['HOME_ATS_COVER'] == merged['LGB_ATS_BET_HOME']).astype(int)

merged['SGD_HINGE_ML_RESULT'] = (merged['home_win_prob_sgd_hinge'] == merged['HOME_WIN']).astype(int)
merged['SGD_LOGLOSS_ML_RESULT'] = (merged['home_win_prob_sgd_logloss'].round() == merged['HOME_WIN']).astype(int)
merged['LGB_ML_RESULT'] = (merged['home_win_prob_lgbc'].round() == merged['HOME_WIN']).astype(int)


print(merged[['HOME_TEAM_SCORE', 'AWAY_TEAM_SCORE']])
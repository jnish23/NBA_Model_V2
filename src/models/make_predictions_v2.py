from datetime import date
import lightgbm as lgb
import numpy as np
import optuna
import os
import pandas as pd
from pathlib import Path
import sys
sys.path.append('C:/Users/Jordan Nishimura/NBA_Model_v1')

from src.models.model_preparation import get_draftking_lines, clean_draftking_lines, get_odds_data
from src.data.process_data_no_split import get_data_from_db_all

from src.models.Train_LGBMClassifier_WinPredictor import train_and_save_lgbm_classifer
from src.models.Train_LGBMRegressor_ScorePredictor import train_and_save_lgbm_regressor
from src.models.Train_SGDClassifierHinge_WinPredictor import train_and_save_sgd_classifier_hinge
from src.models.Train_SGDClassifierLogLoss_WinPredictor import train_and_save_sgd_classifier_logloss
from src.models.Train_SGDRegressor_ScorePredictor import train_and_save_sgd_regressor

from src import prep_data_for_final_model_new
from src import send_df_to_sheets

from src.etl import etl_pipeline
from src.data.update_data import update_all_data
import joblib
import sqlite3



def make_matchup_row(home_team, away_team, df):
    
    print("creating matchups between Home and Away team aggregated stats")

    matchup_info_cols = ['SEASON', 'TEAM_ABBREVIATION', 'GAME_DATE', 'GAME_ID', 'MATCHUP',
        'HOME_GAME', 'TEAM_SCORE', 'ML', 'SPREAD', 'ATS_DIFF', 'TEAM_COVERED',
        'POINT_DIFF', 'WL']

    most_recent_home_stats = df.loc[df['TEAM_ABBREVIATION'] == home_team].tail(1).drop(columns=matchup_info_cols).reset_index(drop=True)
    most_recent_home_stats = most_recent_home_stats.add_prefix('HOME_')

    most_recent_away_stats = df.loc[df['TEAM_ABBREVIATION'] == away_team].tail(1).drop(columns=matchup_info_cols).reset_index(drop=True)
    most_recent_away_stats = most_recent_away_stats.add_prefix('AWAY_')

    matchup_row = pd.concat([most_recent_home_stats, most_recent_away_stats], axis=1)
        
    return matchup_row




def main():

    update_all_data()
    
    season_year = 2023
    
    print("ETL on updated data to retrain models")
    start_season = 2013
    end_season = 2023
    processed_data_table_name = 'team_stats_ewa_matchup_prod'
    path_to_db = Path.home().joinpath('NBA_Model_v1', 'data', 'nba.db')
    conn = sqlite3.connect(path_to_db)
    etl_pipeline(start_season = start_season, end_season = end_season, table_name = processed_data_table_name)
    
    # print("preparing team statistics")
    # df = prep_data_for_final_model.load_and_process_data(start_season=2013, end_season=2023)
    
    today = date.today()
    print("Getting Odds Data...") 
    
    odds_df = get_odds_data(date=today, sportsbook = 'fanduel')
    
    # dk_lines_df = get_draftking_lines(date=today)
    # dk_lines_clean = clean_draftking_lines(dk_lines_df)
        
    todays_matchup_info, todays_features = prep_data_for_final_model_new.generate_features_for_model(conn, start_season, end_season)
    
    print("retraining models on historical data")
    train_and_save_lgbm_classifer()
    train_and_save_lgbm_regressor()
    train_and_save_sgd_classifier_hinge()
    train_and_save_sgd_classifier_logloss()
    train_and_save_sgd_regressor()
    
    print("loading retrained models")
    lgbr_filepath = Path.home().joinpath('NBA_model_v1', 'models', 'LGBRegressor.sav')
    sgdr_filepath = Path.home().joinpath('NBA_model_v1', 'models', 'SGDRegressor_ScorePredictor.sav')
    lgbc_filepath = Path.home().joinpath('NBA_model_v1', 'models', 'LGBMClassifier.sav')
    sgdc_hinge_filepath = Path.home().joinpath('NBA_model_v1', 'models', 'SGDClassifierHinge_WinPredictor.sav')
    sgdc_logloss_filepath = Path.home().joinpath('NBA_model_v1', 'models', 'SGDClassifierLogLoss_WinPredictor.sav')

    LGBRegressor = joblib.load(lgbr_filepath)
    SGDRegressor = joblib.load(sgdr_filepath)
    LGBClassifier = joblib.load(lgbc_filepath)
    SGDClassifier_Hinge = joblib.load(sgdc_hinge_filepath)
    SGDClassifier_LogLoss = joblib.load(sgdc_logloss_filepath)
    
    
    sgdr_pred = SGDRegressor.predict(todays_features)
    lgbr_pred = LGBRegressor.predict(todays_features)
    
    lgbc_prob = LGBClassifier.predict_proba(todays_features)
    sgd_hinge_pred = SGDClassifier_Hinge.predict(todays_features)
    sgd_logloss_prob = SGDClassifier_LogLoss.predict_proba(todays_features)
    
    

    results = pd.DataFrame({'home_team':todays_matchup_info['HOME_TEAM_ABBREVIATION'],
                            'sgd_home_score_pred':sgdr_pred[:, 0],
                            'sgd_away_score_pred':sgdr_pred[:, 1],
                            'lgb_home_score_pred':lgbr_pred[:, 0],
                            'lgb_away_score_pred':lgbr_pred[:, 1],
                            'home_win_prob_sgd_hinge':sgd_hinge_pred,
                            'home_win_prob_sgd_logloss':sgd_logloss_prob[:, 1],
                            'away_win_prob_sgd_logloss':sgd_logloss_prob[:, 0],
                            'home_win_prob_lgbc':lgbc_prob[:, 1],
                            'away_win_prob_lgbc':lgbc_prob[:, 0],                           
                            })

    results = pd.merge(odds_df, results,
                       left_on = ['home_team'],
                       right_on = ['home_team'])

    # results = pd.merge(dk_lines_clean, results,
    #                    left_on = ['home_team'],
    #                    right_on = ['home_team'])
    
    # results = results.rename(columns={'home_moneyline':'home_moneylines', 
    #                                   'away_moneyline':'away_moneylines'})
    
    results = results[['home_team', 'away_team', 'game_date', 'home_spread', 
                       'home_moneyline', 'away_moneyline', 'OU', 'sgd_home_score_pred',
                       'sgd_away_score_pred','lgb_home_score_pred','lgb_away_score_pred',
                       'home_win_prob_sgd_hinge','home_win_prob_sgd_logloss',
                       'away_win_prob_sgd_logloss','home_win_prob_lgbc','away_win_prob_lgbc']]
    
    
    print('saving to results_df')
    
    
    path_to_results = Path.home().joinpath('NBA_model_v1', 'results', f'betting_predictions_{season_year}.csv')
    
    if not os.path.isfile(path_to_results):
        results.to_csv(path_to_results, index=False, header='column_names') 
    else:
        results.to_csv(path_to_results, index=False, mode='a', header=False)    
        
        
    send_df_to_sheets.main()
    
    return results

if __name__ == '__main__':   

    
    print(main())
    
    
    

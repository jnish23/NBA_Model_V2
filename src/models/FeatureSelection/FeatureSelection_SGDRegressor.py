
import joblib
import optuna
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_absolute_error
from sklearn.multioutput import MultiOutputRegressor
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import RFECV, SequentialFeatureSelector
import sqlite3


def season_to_string(x):
    return str(x) + '-' + str(x+1)[-2:]


def get_data_from_db(target, con, test_season):
    
        
    test_season_str = season_to_string(test_season)
    

    df = pd.read_sql('SELECT * FROM team_stats_ewa_matchup', con=con)
    df = df.drop(columns=['index'])

    df = df.sort_values('GAME_DATE')

    df = df.dropna()

    columns_to_drop = ['SEASON', 'HOME_TEAM_ABBREVIATION', 'GAME_DATE', 'GAME_ID', 'MATCHUP',
                        'HOME_HOME_GAME', 'HOME_TEAM_SCORE', 'HOME_ML', 'HOME_SPREAD',
                        'HOME_ATS_DIFF', 'HOME_TEAM_COVERED', 'HOME_POINT_DIFF',
                        'HOME_WL', 'AWAY_ML', 'AWAY_TEAM_SCORE']

    train_df = df.loc[df['SEASON'] < test_season_str]
    test_df = df.loc[df['SEASON'] >= test_season_str]

    X_train = train_df.drop(columns=columns_to_drop)
    y_train = train_df[target]

    X_test = test_df.drop(columns=columns_to_drop)
    y_test = test_df[target]
    
    return X_train, X_test, y_train, y_test, train_df, test_df


def tscv_by_season(train_df, test_season = 2021):
    earliest_year_with_data = 2013
    min_training_years = 3

    cv_splits = []

    for year in range(earliest_year_with_data + min_training_years, test_season):      
        listTrain = train_df.loc[train_df['SEASON'] < season_to_string(year)].index
        listVal = train_df.loc[train_df['SEASON'] == season_to_string(year)].index        
        cv_splits.append((listTrain, listVal))

    return cv_splits      


if __name__ == '__main__':
    
    
    db_filepath = Path.home().joinpath('NBA_model_v1', 'data', 'nba.db')
    
    # get data
    connection = sqlite3.connect(db_filepath)
    X_train, X_test, y_train, y_test, train_df, test_df = get_data_from_db(target=['HOME_TEAM_SCORE', 'AWAY_TEAM_SCORE'], con=connection, test_season=2021)
    connection.close()
    
    # get best hyperparameters
    model_name = 'SGDRegressor_ScorePredictor'
    study_name = str(Path.home().joinpath('NBA_model_v1', 'models', 'hyperparameter_tuning', model_name))    
    storage_name = "sqlite:///{}.db".format(study_name)
    
    study = optuna.load_study(study_name = study_name, storage = storage_name)
    
    params = study.best_params
    print(params)
    
    # instantiate model with hyperparameters
    model = Pipeline([('scaler', StandardScaler()),
                                ('sgd', MultiOutputRegressor(SGDRegressor(**params,                                                                
                                                                shuffle=False,
                                                                random_state=23)))])

    
    # fit model
    model.fit(X_train, y_train)
    
    # Calculate MAE scores
    
    train_preds = model.predict(X_train)
    test_preds = model.predict(X_test)
    
    print("train_mae before removing correlated features", mean_absolute_error(y_train, train_preds))
    print("test_mae before removing correlated features", mean_absolute_error(y_test, test_preds))
     
       
    # Drop columns with high correlation
    ##
    
    # corr_matrix = X_train.corr().abs()
    # upper_tri = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    # to_drop = [column for column in upper_tri.columns if any(upper_tri[column] > 0.95)]
    
    # print()
    # print(to_drop)    
    
    print('original_num_features:', X_train.shape[1])
    
    # X_train = X_train.drop(columns=to_drop)
    # X_test = X_test.drop(columns=to_drop)
    
    # print('num_features after dropping correlated:', X_train.shape[1])

    # # fit model
    # model.fit(X_train, y_train)
    
    
    # train_preds = model.predict(X_train)
    # test_preds = model.predict(X_test)

    # print("train_mae after removing correlated features", mean_absolute_error(y_train, train_preds))
    # print("test_mae after removing correlated features", mean_absolute_error(y_test, test_preds))
    
    
    cv_splits = tscv_by_season(train_df, test_season = 2021)

    # SFS
    
    sfs = SequentialFeatureSelector(estimator=model, 
                                    cv=cv_splits, 
                                    n_features_to_select=20, 
                                    n_jobs=-1,
                                    scoring='neg_mean_absolute_error'
                                    )
    
    
    sfs.fit(X_train, y_train)

    features = X_train.columns[sfs.support_]
    print(features)
    model.fit(X_train[features], y_train)
    
    train_preds = model.predict(X_train[features])
    test_preds = model.predict(X_test[features])

    print("train_mae after SFS", mean_absolute_error(y_train, train_preds))
    print("test_mae after SFS", mean_absolute_error(y_test, test_preds))
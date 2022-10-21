import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path

path_to_results = Path().home().joinpath('NBA_Model_v1', 'results', 'betting_predictions_2022.csv')

df = pd.read_csv(path_to_results)
df = df.drop_duplicates(subset=['home_team', 'away_team', 'game_date'], keep='last')

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

path_to_key = Path().home().joinpath('NBA_model_v1', 'results', 'nba-model-314520-a7e4b87dbdb6.json')

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    path_to_key, scope)

gc = gspread.authorize(credentials)


spreadsheet_key = '15NYLE7EIyZtT4MWpDAInAcnPo5cw4wPj5SsAks512BE'

wks_name = 'Model_Predictions_2022'

d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials,
           row_names=True)


import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from time import sleep
from datetime import date
from sbrscrape import Scoreboard

options = Options()
options.add_argument('--ignore-certificate-errors')
options.add_experimental_option('excludeSwitches', ['enable-logging'])
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) 

def avg_data_for_model(df, window_size=10, min_periods=5):
    """Computes rolling average for each team's game stats over last n games where n is the window_size (default=10)"""
    df = df.copy()

    df = df.drop(columns = ['SEASON_YEAR_opp', 'SEASON_ID_opp', 
                            'TEAM_ID_opp', 'TEAM_ABBREVIATION_opp',
                            'TEAM_NAME_opp', 'GAME_DATE_opp', 
                            'MATCHUP_opp', 'WL_opp', 'HOME_GAME_opp',
                           'point_diff_opp'])

    team_dfs = []
    for season in df['SEASON_YEAR_team'].unique():
        season_df = df.loc[df['SEASON_YEAR_team'] == season]
        for team in df['TEAM_ABBREVIATION_team'].unique():
            team_df = season_df.loc[season_df['TEAM_ABBREVIATION_team'] == team].sort_values('GAME_DATE_team')
            team_df.iloc[:, 12:] = team_df.iloc[:, 12:].rolling(10, min_periods=min_periods).mean()
            team_dfs.append(team_df)

    new_df = pd.concat(team_dfs)
    new_df = new_df.reset_index(drop=True)
        
    return new_df




def get_days_spreads(date):
    """INPUTS
    date: "yyyy-mm-dd"
    OUPUTS: dataframe with game spreads
    """
    gm_dates = []
    away_teams = []
    home_teams = []
    away_spreads = []
    home_spreads = []
    
    web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/pointspread/?date={}'.format(date)
    path = '../chromedriver.exe'

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(web)
    sleep(2)


    single_row_events = driver.find_elements_by_class_name('eventMarketGridContainer-3QipG')

    num_postponed_events = len(driver.find_elements_by_class_name('eventStatus-3EHqw'))

    num_listed_events = len(single_row_events)
    cutoff = num_listed_events - num_postponed_events

    for event in single_row_events[:cutoff]:

        away_team = event.find_elements_by_class_name('participantBox-3ar9Y')[0].text
        home_team = event.find_elements_by_class_name('participantBox-3ar9Y')[1].text
        away_teams.append(away_team)
        home_teams.append(home_team)
        gm_dates.append(date)

        spreads = event.find_elements_by_class_name('pointer-2j4Dk')
        away_lines = []
        home_lines = []
        for i in range(len(spreads)):    
            if i % 2 == 0:
                away_lines.append(spreads[i].text)
            else:
                home_lines.append(spreads[i].text)

        away_lines = ",".join(away_lines)
        home_lines = ",".join(home_lines)

        away_spreads.append(away_lines)
        home_spreads.append(home_lines)


    driver.quit()

    todays_spreads = pd.DataFrame({'GM_DATE':gm_dates,
                      'AWAY_TEAM':away_teams,
                      'HOME_TEAM':home_teams,
                      'AWAY_SPREAD':away_spreads,
                      'HOME_SPREAD':home_spreads})

    for col in todays_spreads[['AWAY_SPREAD', 'HOME_SPREAD']].columns:
        todays_spreads[col] = todays_spreads[col].astype(str)
        todays_spreads[col] = todays_spreads[col].str.replace("[", "")
        todays_spreads[col] = todays_spreads[col].str.replace("]", "")
        todays_spreads[col] = todays_spreads[col].str.strip()
        
    return todays_spreads


def get_days_moneylines(date):
    """INPUTS
    date: "yyyy-mm-dd"
    OUPUTS: dataframe with game spreads
    """
    gm_dates = []
    away_teams = []
    home_teams = []
    away_mls = []
    home_mls = []
    
    web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line/?date={}'.format(
        date)
    path = '../chromedriver.exe'
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(web)
    sleep(3)
    
    single_row_events = driver.find_elements_by_class_name('eventMarketGridContainer-3QipG')
    
    num_postponed_events = len(
        driver.find_elements_by_class_name('eventStatus-3EHqw'))

    num_listed_events = len(single_row_events)
    cutoff = num_listed_events - num_postponed_events

    for event in single_row_events[:cutoff]:
        away_team = event.find_elements_by_class_name(
            'participantBox-3ar9Y')[0].text
        home_team = event.find_elements_by_class_name(
            'participantBox-3ar9Y')[1].text

        away_teams.append(away_team)
        home_teams.append(home_team)

        gm_dates.append(date)

        mls = event.find_elements_by_class_name('pointer-2j4Dk')

        away_moneyline = []
        home_moneyline = []

        for i, ml in enumerate(mls):
            if i % 2 == 0:
                away_moneyline.append(ml.text)
            else:
                home_moneyline.append(ml.text)

        away_moneyline = ",".join(away_moneyline)
        home_moneyline = ",".join(home_moneyline)

        away_mls.append(away_moneyline)
        home_mls.append(home_moneyline)

    driver.quit()

    todays_moneylines = pd.DataFrame({'GM_DATE': gm_dates,
                       'AWAY_TEAM': away_teams,
                      'HOME_TEAM': home_teams,
                       'AWAY_ML': away_mls,
                       'HOME_ML': home_mls,
                       })

    for col in todays_moneylines[['AWAY_ML', 'HOME_ML']].columns:
        todays_moneylines[col] = todays_moneylines[col].astype(str)
        todays_moneylines[col] = todays_moneylines[col].str.replace("[", "")
        todays_moneylines[col] = todays_moneylines[col].str.replace("]", "")
        todays_moneylines[col] = todays_moneylines[col].str.strip()
        
    return todays_moneylines

def get_odds_data(date, sportsbook = 'fanduel'):
    df_data = []

    sb = Scoreboard(sport='NBA', date=date)
    # if not hasattr(sb, "games"):
    #     continue      
    
    team_abbrev_dict =  {'Atlanta Hawks': 'ATL',
    'Boston Celtics': 'BOS',
    'Brooklyn Nets': 'BKN',
    'Charlotte Hornets': 'CHA',
    'Chicago Bulls': 'CHI',
    'Cleveland Cavaliers': 'CLE',
    'Dallas Mavericks': 'DAL',
    'Denver Nuggets': 'DEN',
    'Detroit Pistons': 'DET',
    'Golden State Warriors': 'GSW',
    'Houston Rockets': 'HOU',
    'Indiana Pacers': 'IND',
    'Los Angeles Clippers': 'LAC',
    'Los Angeles Lakers': 'LAL',
    'Memphis Grizzlies': 'MEM',
    'Miami Heat': 'MIA',
    'Milwaukee Bucks': 'MIL',
    'Minnesota Timberwolves': 'MIN',
    'New Orleans Pelicans': 'NOP',
    'New York Knicks': 'NYK',
    'Oklahoma City Thunder': 'OKC',
    'Orlando Magic': 'ORL',
    'Philadelphia 76ers': 'PHI',
    'Phoenix Suns': 'PHX',
    'Portland Trail Blazers': 'POR',
    'Sacramento Kings': 'SAC',
    'San Antonio Spurs': 'SAS',
    'Toronto Raptors': 'TOR',
    'Utah Jazz': 'UTA',
    'Washington Wizards': 'WAS'
    }

    for game in sb.games:           
        try:
            df_data.append({
                    'away_team': game['away_team'],           
                    'home_team': game['home_team'],
                    'away_spread': game['away_spread'][sportsbook],
                    'home_spread': game['home_spread'][sportsbook],
                    'OU': game['total'][sportsbook],
                    'home_moneyline': game['home_ml'][sportsbook],
                    'away_moneyline': game['away_ml'][sportsbook],
                    'game_date': date,


                    # 'Home_Score': game['home_score'],
                    # 'Away_Score': game['away_score'],
                    # 'Points': game['away_score'] + game['home_score'],
                    # 'Win_Margin': game['home_score'] - game['away_score'],
                })
        except KeyError:
            print(f"No {sportsbook} odds data found for game: {game}")


    df = pd.DataFrame(df_data,)
    
    df['away_moneyline'] = convert_american_to_decimal(df['away_moneyline'].astype(int))
    df['home_moneyline'] = convert_american_to_decimal(df['home_moneyline'].astype(int))
    
    df['home_team'] = df['home_team'].replace(team_abbrev_dict)
    df['away_team'] = df['away_team'].replace(team_abbrev_dict)

    return df


def get_draftking_lines(date):
    """
    INPUTS
    date: "yyyy-mm-dd"
    OUPUTS 
    dataframe with game spreads
    """
    away_teams = []
    home_teams = []
    away_spreads = []
    home_spreads = []
    away_moneylines = []
    home_moneylines = []

    web = 'https://sportsbook.draftkings.com/leagues/basketball/nba'

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(web)

    sleep(2)

    first_heading = driver.find_elements(By.XPATH, '//*[@id="root"]/section/section[2]/section/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/table/thead/tr/th[1]/div/span/span/span')         
    
    if first_heading[0].text == 'TODAY':
        print("Scraping Today's Lines")
        teams = driver.find_elements(By.XPATH, '//*[@id="root"]/section/section[2]/section/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr/th/a/div/div[2]/div/div/div/div/div')
      
        
        # teams = driver.find_elements(By.XPATH, '//*[@id="root"]/section/section[2]/section/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr/th/a/div/div[2]/div/span/div/div')
        spreads = driver.find_elements(By.XPATH, '//*[@id="root"]/section/section[2]/section/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr/td[1]/div/div/div/div[1]/span')
        # spreads = driver.find_elements(By.XPATH, '//*[@id="root"]/section/section[2]/section/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr/td[1]/div/div/div/div[1]/span')
        moneylines = driver.find_elements(By.XPATH, '//*[@id="root"]/section/section[2]/section/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr/td[3]/div/div/div/div/div[2]/span')    
        # moneylines = driver.find_elements(By.XPATH, '//*[@id="root"]/section/section[2]/section/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/table/tbody/tr/td[3]/div/div/div/div/div[2]/span')    

        for i in range(len(teams)):
            if i%2==0:
                away_teams.append(teams[i].text)
                away_spreads.append(spreads[i].text)
                away_moneylines.append(moneylines[i].text)
            else:
                home_teams.append(teams[i].text)
                home_spreads.append(spreads[i].text)
                home_moneylines.append(moneylines[i].text) 
    else:
        print("No Games Today")
        return None
                   

    driver.quit()

    todays_lines = pd.DataFrame({'away_team':away_teams,
                'home_team':home_teams,
                'away_spread':away_spreads,
                'home_spread':home_spreads,
                'away_moneyline':away_moneylines,
                'home_moneyline':home_moneylines})
    
    todays_lines['game_date'] = date
    
    return todays_lines


def clean_draftking_lines(df):
    
    abbr_mapping = {'Celtics': 'BOS',
                    'Trail Blazers': 'POR',
                    'Lakers': 'LAL', 'Nets': 'BKN',
                    'Cavaliers': 'CLE', 'Raptors': 'TOR',
                    '76ers': 'PHI', 'Grizzlies': 'MEM',
                    'Timberwolves': 'MIN', 'Pelicans': 'NOP',
                    'Thunder': 'OKC', 'Mavericks': 'DAL',
                    'Spurs': 'SAS', 'Nuggets': 'DEN',
                    'Warriors': 'GSW', 'Clippers': 'LAC',
                    'Magic': 'ORL', 'Jazz': 'UTA',
                    'Hornets': 'CHA', 'Pistons': 'DET',
                    'Heat': 'MIA', 'Suns': 'PHX',
                    'Hawks': 'ATL', 'Knicks': 'NYK',
                    'Pacers': 'IND', 'Bulls': 'CHI',
                    'Rockets': 'HOU', 'Bucks': 'MIL',
                    'Kings': 'SAC', 'Wizards': 'WAS'}
    
    
    df['away_team'] = df['away_team'].str[3:].str.strip()
    df['home_team'] = df['home_team'].str[3:].str.strip()
    df['away_team'] = df['away_team'].replace(abbr_mapping)
    df['home_team'] = df['home_team'].replace(abbr_mapping)

    df['away_spread'] = df['away_spread'].astype(str).str.replace('pk', '0', regex=False).astype(float)
    df['home_spread'] = df['home_spread'].astype(str).str.replace('pk', '0', regex=False).astype(float)
    
    df['away_moneyline'] = df['away_moneyline'].str.replace('−', '-', regex=False)
    df['home_moneyline'] = df['home_moneyline'].str.replace('−', '-', regex=False)
    
    df['away_moneyline'] = convert_american_to_decimal(df['away_moneyline'].astype(int))
    df['home_moneyline'] = convert_american_to_decimal(df['home_moneyline'].astype(int))
    
    return df

def convert_american_to_decimal(x):
    return np.where(x>0, (100+x)/100, 1+(100.0/-x))      


if __name__ == '__main__':
    
    today = date.today()
    print(today)
    get_draftking_lines(date)
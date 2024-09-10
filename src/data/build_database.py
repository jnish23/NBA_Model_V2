from IPython.display import clear_output
from nba_api.stats.endpoints import leaguegamelog, boxscoreadvancedv2, boxscorescoringv2
import pandas as pd
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sqlite3
from time import sleep
from tqdm import tqdm


def add_basic_boxscores_to_db(conn, season):
    """This function pulls basic team boxscores from the NBA_API package 
    and appends (or creates a new table if not exists) it to the table team_basic_boxscores in the sqlite db"""
    
    table_name = 'team_basic_boxscores'
    season_boxscores = []
    season_str = season_string(season)
    for season_type in ['Regular Season', 'PlayIn', 'Playoffs']:
        boxscores = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
        season_boxscores.append(boxscores)
        sleep(2)
    season_df = pd.concat(season_boxscores)
    season_df['SEASON'] = season_str
    season_df.drop(columns = ['SEASON_ID', 'VIDEO_AVAILABLE'], inplace=True)
    
    season_df.to_sql(table_name, conn, if_exists='append', index=False)
    
    sleep(3)
        
    cur = conn.cursor()
    cur.execute(f'DELETE FROM {table_name} WHERE rowid NOT IN (SELECT max(rowid) FROM {table_name} GROUP BY TEAM_ID, GAME_ID)')
    conn.commit()
    
    return None


def add_advanced_boxscores_to_db(conn, season):
    """
    This function pulls advanced team boxscores from the NBA_API package 
    and appends (or creates a new table if not exists) it to the table team_advanced_boxscores in the sqlite db
    
    Note: Because of timeout errors and that we have to pull each game's individually, each season takes 1-2 hours.
    If some games were not pulled in certain seasons, you can use the update functions to gather those individual games.
    """
    
    table_name = 'team_advanced_boxscores'
    game_ids_not_added = []
    
    game_ids = get_game_ids(season)

    for i in range(0, len(game_ids), 100):
        print(f'games {i} to {i+100}')
        df_holder = []
        for game_id in tqdm(game_ids[i:i+100], desc='progress'):
            try:
                team_boxscores = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id).get_data_frames()[1]    
                df_holder.append(team_boxscores)                
            except:
                game_ids_not_added.append(game_id)
            sleep(2)
         
        df = pd.concat(df_holder)
        df.to_sql(table_name, conn, if_exists='append', index=False)
        sleep(120)
        print("sleeping for 120 seconds to prevent timeout error...")
        clear_output(wait=True)

        
    cur = conn.cursor()
    cur.execute(f'DELETE FROM {table_name} WHERE rowid NOT IN (SELECT max(rowid) FROM {table_name} GROUP BY TEAM_ID, GAME_ID)')
    conn.commit()
    
    return game_ids_not_added


def add_scoring_boxscores_to_db(conn, season):
    """
    This function pulls scoring team boxscores from the NBA_API package 
    and appends (or creates a new table if not exists) it to the table team_scoring_boxscores in the sqlite db.
    
    Note: Because of timeout errors and that we have to pull each game's individually, each season takes 1-2 hours.
    If some games were not pulled in certain seasons, you can use the update functions to gather those individual games.
    """
    
    table_name = 'team_scoring_boxscores'
    game_ids_not_added = []   
    
    game_ids = get_game_ids(season)

    for i in range(0, len(game_ids), 100):
        print(f'games {i} to {i+100}')
        df_holder = []
        for game_id in tqdm(game_ids[i:i+100], desc='progress'):
            try:
                scoring_boxscores = boxscorescoringv2.BoxScoreScoringV2(game_id).get_data_frames()[1]
                df_holder.append(scoring_boxscores)
            except:
                game_ids_not_added.append(game_id)
            sleep(2)
        df = pd.concat(df_holder)
        df.to_sql(table_name, conn, if_exists='append', index=False)

        sleep(120)
        print("sleeping for 120 seconds to prevent timeout error...")        
        clear_output(wait=True)
                
    cur = conn.cursor()
    cur.execute(f'DELETE FROM {table_name} WHERE rowid NOT IN (SELECT max(rowid) FROM {table_name} GROUP BY TEAM_ID, GAME_ID)')
    conn.commit()
    
    return game_ids_not_added


def add_moneylines_to_db(conn, season):
    
    table_name = 'moneylines'
    
    dates_with_no_data = []
    
    seasons = []
    gm_dates = []
    away_teams = []
    home_teams = []
    opening_mls_away = []
    opening_mls_home = []
    away_mls = []
    home_mls = []

    season_str = season_string(season)
    print(f"scraping season: {season_str}")
    dates = get_game_dates(season)
        
    for date in tqdm(dates, desc='progress'):
        web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line/?date={}'.format(date)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.get(web)
        driver.set_window_size(1920, 1024)

        sleep(random.randint(1,2))

        try:
            single_row_events = driver.find_elements(By.XPATH, '//*[@id="tbody-nba"]/div')
        
        except:
            print(f"No Data for {date}")
            dates_with_no_data.append(date)
            continue
                    
        num_postponed_events = len(driver.find_elements(By.CLASS_NAME, 'eventStatus-3EHqw'))

        num_listed_events = len(single_row_events)
        cutoff = num_listed_events - num_postponed_events

        for event in single_row_events[:cutoff]:
            seasons.append(season_string(season))

            away_team = event.find_elements(By.CLASS_NAME, 'GameRows_participantBox__0WCRz')[0].text
            home_team = event.find_elements(By.CLASS_NAME, 'GameRows_participantBox__0WCRz')[1].text

            away_teams.append(away_team)
            home_teams.append(home_team)

            gm_dates.append(date)
            opening_mls_away.append(event.find_elements(By.CLASS_NAME, 'GameRows_opener__NivKJ')[2].text)
            opening_mls_home.append(event.find_elements(By.CLASS_NAME, 'GameRows_opener__NivKJ')[4].text)                

            mls = event.find_elements(By.CLASS_NAME, 'GameRows_columnsContainer__Y94VP')[0].text.split('\n')
            
            away_moneyline = []
            home_moneyline = []

            for i, ml in enumerate(mls):
                if i % 2 == 0:
                    away_moneyline.append(ml)
                else:
                    home_moneyline.append(ml)

            away_moneyline = ",".join(away_moneyline)
            home_moneyline = ",".join(home_moneyline)

            away_mls.append(away_moneyline)
            home_mls.append(home_moneyline)
            
        driver.quit()
        
    clear_output(wait=True)
        
    df = pd.DataFrame({'SEASON': seasons,
                        'GM_DATE': gm_dates,
                        'AWAY_TEAM': away_teams,
                        'HOME_TEAM': home_teams,
                        'AWAY_OPENING_ML':opening_mls_away,
                        'HOME_OPENING_ML':opening_mls_home,
                        'AWAY_ML': away_mls,
                        'HOME_ML': home_mls,
                        })
    
    df = df.sort_values(['GM_DATE']).reset_index(drop=True)
    
    df.to_sql(table_name, conn, if_exists='append', index=False)

    cur = conn.cursor()
    cur.execute('''DELETE FROM moneylines 
                    WHERE rowid NOT IN (SELECT MIN(rowid) FROM moneylines
                                        GROUP BY GM_DATE, AWAY_TEAM, HOME_TEAM)''')
    conn.commit()
    
    return df


def add_spreads_to_db(conn, season):
    
    table_name = 'spreads'
    
    dates_with_no_data = []
    
    seasons = []
    gm_dates = []
    away_teams = []
    home_teams = []
    opening_spreads_away = []
    opening_spreads_home = []
    away_spreads = []
    home_spreads = []
    
    season_str = season_string(season)
    
    print(f"scraping season: {season_str}")
    dates = get_game_dates(season)    
    
    for date in tqdm(dates, desc='progress'):
        web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/?date={}'.format(date)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.get(web)
        sleep(random.randint(1,2))
        driver.set_window_size(1920, 1024)

        try:
            single_row_events = driver.find_elements(By.XPATH, '//*[@id="tbody-nba"]/div')
            
        except:
            print(f"No Data for {date}")
            dates_with_no_data.append(date)
            continue
            
        num_postponed_events = len(driver.find_elements(By.CLASS_NAME, 'eventStatus-3EHqw'))

        num_listed_events = len(single_row_events)
        cutoff = num_listed_events - num_postponed_events

        for event in single_row_events[:cutoff]:
            seasons.append(season_string(season))
            away_team = event.find_elements(By.CLASS_NAME, 'GameRows_participantBox__0WCRz')[0].text
            home_team = event.find_elements(By.CLASS_NAME, 'GameRows_participantBox__0WCRz')[1].text
            away_teams.append(away_team)
            home_teams.append(home_team)
            gm_dates.append(date)
            opening_spreads_away.append(event.find_elements(By.CLASS_NAME, 'GameRows_opener__NivKJ')[1].text)
            opening_spreads_home.append(event.find_elements(By.CLASS_NAME, 'GameRows_opener__NivKJ')[3].text)
            
            spreads = event.find_elements(By.CLASS_NAME, 'GameRows_columnsContainer__Y94VP')[0].text.split('\n')
            away_lines = []
            home_lines = []
            for i in range(len(spreads)):    
                if i % 2 == 0:
                    away_lines.append(spreads[i].text)
                else:
                    home_lines.append(spreads[i].text)
            
            away_lines_str = ",".join(away_lines)
            home_lines_str = ",".join(home_lines)
            
            away_spreads.append(away_lines_str)
            home_spreads.append(home_lines_str)

        driver.quit()
        clear_output(wait=True)

    df = pd.DataFrame({'SEASON':seasons, 
                      'GM_DATE':gm_dates,
                      'AWAY_TEAM':away_teams,
                      'HOME_TEAM':home_teams,
                      'AWAY_OPENING_SPREAD':opening_spreads_away,
                      'HOME_OPENING_SPREAD':opening_spreads_home,
                      'AWAY_SPREAD':away_spreads,
                      'HOME_SPREAD':home_spreads})

    df = df.sort_values(['GM_DATE']).reset_index(drop=True)
    
    df.to_sql(table_name, conn, if_exists='append', index=False)
    
    cur = conn.cursor()
    cur.execute('''DELETE FROM spreads 
                    WHERE rowid NOT IN (SELECT MIN(rowid) FROM spreads 
                                        GROUP BY GM_DATE, AWAY_TEAM, HOME_TEAM)''')
    conn.commit()
    
    return df

    
def season_string(season):
    return str(season) + '-' + str(season+1)[-2:]


def get_game_dates(season):
    season_str = season_string(season)
    dates = []
    for season_type in ['Regular Season', 'Playoffs']:
        games = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
        dates.extend(games['GAME_DATE'].unique())
        sleep(1)
    return dates

def get_game_ids(season):
    game_ids = []
    season_str = season_string(season)
    for season_type in ['Regular Season', 'PlayIn', 'Playoffs']:
        logs = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
        game_ids.extend(logs['GAME_ID'].unique().tolist())
    return game_ids
        

def build_database(conn, season):
    season_str = season_string(season)
    print(f"Gathering basic_boxscores for season: {season_str}")
    add_basic_boxscores_to_db(conn, season)
    
    print(f"Gathering advanced_boxscores for season: {season_str}")
    advanced_boxscores_game_ids_not_added = add_advanced_boxscores_to_db(conn, season)
    print("GAME_IDS not added to advanced boxscores:", advanced_boxscores_game_ids_not_added)

    print(f"Gathering scoring_boxscores for season: {season_str}")
    scoring_boxscores_game_ids_not_added = add_scoring_boxscores_to_db(conn, season)
    
    print("GAME_IDS not added to scoring boxscores:", scoring_boxscores_game_ids_not_added)

    print(f"Scraping moneyline data for season: {season_str}")
    add_moneylines_to_db(conn, season)
    
    print(f"Scraping spread data for season: {season_str}")
    add_spreads_to_db(conn, season)
    
    return None
    
if __name__ == '__main__':
    for season in 
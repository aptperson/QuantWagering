import pandas as pd
from scraping.SportsBookOdds import get_odds

def calc_odds_prob(df, broker = 'PIN'):
    
    
    broker = 'ml_' + broker
    broker_sign = broker + '_sign'
    broker_dec = broker + '_dec'
    broker_int = broker + '_int'
    broker_prob = broker + '_prob'
    
    df[broker_sign] = [s[0] for s in df[broker]]

    df[broker + '_dec'] = 0

    df[broker + '_int'] = [int(s[1:]) for s in df[broker]]

    df.loc[df[broker_sign] == '+', broker_dec] = \
        df.loc[df[broker_sign] == '+', broker_int]/100 + 1

    df.loc[df[broker_sign] == '-', broker_dec] = \
        100 / df.loc[df[broker_sign] == '-', broker_int] + 1

    df[broker_prob] = 1/ df[broker_dec]
    
    return(df)

def calc_agg_odds_prob(df, brokers = ['PIN', 'HER', 'FD', 'BOL', 'BVD']):
    
    # calculate odds and prob
    brokers_with_odds = []
    for b in brokers:
        try:
            print('calculating odds for broker {}'.format(b))
            df = calc_odds_prob(df, b)
            brokers_with_odds.append(b)
        except:
            print('odds missing for broker {}, skipping'.format(b))
    
    # agg odds and prob
    df['ml_AGG_dec'] = df[['ml_' + b + '_dec' for b in brokers_with_odds]].sum(axis = 1) / len(brokers_with_odds)
    df['ml_MAX_dec'] = df[['ml_' + b + '_dec' for b in brokers_with_odds]].max(axis = 1)
    
    df['ml_AGG_prob'] = df[['ml_' + b + '_prob' for b in brokers_with_odds]].sum(axis = 1) / len(brokers_with_odds)
    df['ml_MAX_prob'] = df[['ml_' + b + '_prob' for b in brokers_with_odds]].max(axis = 1)
    
    return(df, brokers_with_odds)


# transform to odds data format

TeamNameDict = {}

TeamNameDict['NewOrleans'] = 'New Orleans'
TeamNameDict['Toronto'] ='Toronto'
TeamNameDict['LALakers'] = 'L.A. Lakers'
TeamNameDict['LAClippers'] = 'L.A. Clippers'
TeamNameDict['Detroit'] = 'Detroit' 
TeamNameDict['Indiana'] = 'Indiana'
TeamNameDict['Cleveland'] = 'Cleveland'
TeamNameDict['Orlando'] = 'Orlando'
TeamNameDict['Chicago'] = 'Chicago'
TeamNameDict['Charlotte'] = 'Charlotte'
TeamNameDict['Boston'] = 'Boston'
TeamNameDict['Philadelphia'] = 'Philadelphia'
TeamNameDict['Memphis'] = 'Memphis'
TeamNameDict['Miami'] = 'Miami'
TeamNameDict['Minnesota'] = 'Minnesota'
TeamNameDict['Brooklyn'] = 'Brooklyn'
TeamNameDict['NewYork'] = 'NewYork'
TeamNameDict['SanAntonio'] = 'SanAntonio'
TeamNameDict['Washington'] = 'Washington'
TeamNameDict['Dallas'] = 'Dallas'
TeamNameDict['OklahomaCity'] = 'Oklahoma City'
TeamNameDict['Utah'] = 'Utah'
TeamNameDict['Sacramento'] = 'Sacramento'
TeamNameDict['Phoenix'] = 'Phoenix'
TeamNameDict['Denver'] = 'Denver'
TeamNameDict['Portland'] = 'Portland'
TeamNameDict['Atlanta'] = 'Atlanta'
TeamNameDict['Milwaukee'] = 'Milwaukee'
TeamNameDict['Houston'] = 'Houston'
TeamNameDict['GoldenState'] = 'GoldenState'

def transform_results_format(df):
    
    df['HomeAway'] = 'home'
    df.loc[df.VH == 'V', 'HomeAway'] = 'away'

    df = pd.merge(left = df,
             left_on = 'Team',
             right = pd.DataFrame.from_dict(TeamNameDict , orient='index', columns=['TeamName']),
             right_index = True,
             how = 'left')

    df['date'] = ['2019' + str(d) for d in df.Date]


    # who won the game

    df['WinLoss'] = 0
    WinLossColId = df.columns.get_loc('WinLoss')
    for i in range(df.shape[0]):
        if i % 2 == 0:
            # check row below
            r = 1
        if i % 2 == 1:
            r = -1
            
        df.iloc[i, WinLossColId] = df.iloc[i]['Final'] > df.iloc[i+r]['Final']

    return(df)
#     #         check row above
#             nbaResults.iloc[i, WinLossColId] = nbaResults.iloc[i]['Final'] > nbaResults.iloc[i-1]['Final']


def get_odds_dates(dates):
    
    df_list = []
    
    for d in dates:
        # get the odds
        df_list.append(get_odds(d))
        
    out = pd.concat(df_list)
    
    return(out)


def merge_odds_results(AllOddsDf, nbaResultsDf):

# convert odds
    AllOddsDf, brokers_with_odds = calc_agg_odds_prob(AllOddsDf)

    # join
    join_cols_left = ['HomeAway', 'TeamName', 'date'] # results
    join_cols_right = ['ml_time', 'team', 'key'] # odds

    nbaResultsDf = pd.merge(left = nbaResultsDf,
            left_on = join_cols_left,
            right = AllOddsDf,
            right_on = join_cols_right,
            how = 'left')
    
    return(nbaResultsDf)
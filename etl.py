import numpy as np
import pandas as pd
import datetime as dt

global df 
df = pd.read_csv(r'src/chess_games.csv')

# 1. Game type dimension
def dimGameType():
    temp_df = df.copy()
    temp_df['Event'] = temp_df['Event'].apply(lambda x: x.strip())
    gameType = dict()
    i = 1
    for type in temp_df['Event'].unique():
        gameType[i] = type
        i = i + 1
    
    res = pd.DataFrame.from_dict({'gameTypeId': gameType.keys(),
                                          'gameType': gameType.values()})
    
    return res

# 2. Date dimension
def dimDate():
    temp_df = df.copy()
    temp_df['UTCDate'] = temp_df['UTCDate'].str.replace('.','-')
    temp_df['UTCDate'] = pd.to_datetime(temp_df['UTCDate'], format='%Y-%m-%d')
    
    res = pd.DataFrame(columns=['dateId','day','month','year','weekday'])
    
    res['day'] = temp_df['UTCDate'].apply(lambda x: x.strftime('%d'))
    res['month'] = temp_df['UTCDate'].apply(lambda x: x.strftime('%m'))
    res['year'] = temp_df['UTCDate'].apply(lambda x: x.strftime('%Y'))
    res['weekday'] = temp_df['UTCDate'].dt.day_name()
    res['dateId'] = res[['day','month','year','weekday']].apply('-'.join, axis=1)
    res = res.drop_duplicates()
    
    return res

#. Time dimension
def dimTime():
    temp_df = df.copy()
    
    res = pd.DataFrame(columns=['timeId','hour','minute','second'])
    temp_df['UTCTime'] = pd.to_datetime(temp_df['UTCTime'], format='%H:%M:%S')
    
    res['hour'] = temp_df['UTCTime'].dt.hour
    res['minute'] = temp_df['UTCTime'].dt.minute
    res['second'] = temp_df['UTCTime'].dt.second
    res = res.applymap(str)
    res['timeId'] = res[['hour','minute','second']].apply(':'.join, axis=1)
    res = res.drop_duplicates()
    res['hour'] = res['hour'].astype('int')
    res['minute'] = res['minute'].astype('int')
    res['second'] = res['second'].astype('int')
    
    return res

# 4. Termination Dimension
def dimTermination():
    temp_df = df.copy()
    termination = dict()
    i = 1
    for type in temp_df['Termination'].unique():
        termination[i] = type
        i = i + 1
    
    res = pd.DataFrame.from_dict({'terminationId': termination.keys(),
                                          'terminationReason': termination.values()})
    
    return res

# 5. Player Dimension
def dimPlayer():
    temp_df = df.copy()
    temp_df['UTCDate'] = temp_df['UTCDate'].str.replace('.','-')
    temp_df['datetime'] = temp_df[['UTCDate','UTCTime']].apply(' '.join, axis=1)
    temp_df['datetime'] = pd.to_datetime(temp_df['datetime'], format='%Y-%m-%d %H:%M:%S')
    temp_df = temp_df.sort_values(by='datetime')
    temp_df.index = range(1,len(temp_df)+1)
    temp_df = temp_df.reset_index()
    
    players = np.hstack((temp_df['White'].unique(), temp_df['Black'].unique()))
    players = np.unique(players)
    players = pd.DataFrame(players)
    players.index = range(1,len(players)+1)
    players = players.reset_index()
    players = players.rename(columns={0:'id'})

    white = temp_df.groupby(['White']).agg(lastrow=('index','max'))
    black = temp_df.groupby(['Black']).agg(lastrow=('index','max'))
    white = white.reset_index().rename(columns={'White':'id'})
    black = black.reset_index().rename(columns={'Black':'id'})
    
    test = pd.concat([white,black])
    res = test.groupby('id').agg(lastrow=('lastrow','max'))
    res = res.reset_index()
    
    result = pd.merge(players, res, how='inner', on=['id'])
    test1 = pd.merge(result,temp_df,left_on='lastrow',right_on='index',how='inner')
    test1 = test1[['id','White','Black','WhiteElo','BlackElo','WhiteRatingDiff','BlackRatingDiff']]
    whiteElo = test1[test1['id'] == test1['White']][['id','WhiteElo','WhiteRatingDiff']]
    blackElo = test1[test1['id'] == test1['Black']][['id','BlackElo','BlackRatingDiff']]
    whiteElo['LatestElo'] = whiteElo['WhiteElo'] + whiteElo['WhiteRatingDiff']
    blackElo['LatestElo'] = whiteElo['WhiteElo'] + whiteElo['WhiteRatingDiff']
    whiteElo = whiteElo[['id','LatestElo']]
    blackElo = blackElo[['id','LatestElo']]
    
    final = pd.concat([whiteElo, blackElo])
    final = final.rename(columns={'id': 'playerId','LatestElo': 'elo'})
    
    return final
    # elo = dict()
    # i = 1
    # for p in players:
    #     last_row = temp_df[(temp_df['White'] == p) | (temp_df['Black'] == p)].iloc[-1:]
            
    #     if (p == last_row['White'].item()):
    #         elo[p] = last_row['WhiteElo'].item() + last_row['WhiteRatingDiff'].item()
    #     else:
    #         elo[p] = last_row['BlackElo'].item() + last_row['BlackRatingDiff'].item()
    #     print(f"Player #{i}: {p}")
    #     i = i + 1
    # res = pd.DataFrame.from_dict({'playerId': elo.keys(),
    #                               'elo': elo.values()})
    
    # return res

# 6. Game Fact Table
def factGame():
    gameType = {
        'Classical': 1,
        'Blitz': 2,
        'Blitz tournament': 3,
        'Correspondence': 4,
        'Classical tournament': 5,
        'Bullet tournament': 6,
        'Bullet': 7
    }
    
    termination = {
        'Time forfeit': 1,
        'Normal': 2,
        'Abandoned': 3,
        'Rules infraction': 4,
        'Unterminated': 5
    }
    temp = df.copy()
    temp['Event'] = temp['Event'].str.strip()
    temp = temp.replace({'Event': gameType, 'Termination': termination})
    temp['UTCDate'] = temp['UTCDate'].str.replace('.','-')
    temp['UTCDate'] = pd.to_datetime(temp['UTCDate'], format='%Y-%m-%d')
    temp['weekday'] = temp['UTCDate'].dt.day_name()
    temp['UTCDate'] = temp['UTCDate'].apply(lambda x: x.strftime('%Y-%m-%d'))
    temp['UTCDate'] = temp[['UTCDate','weekday']].apply('-'.join, axis=1)
    temp = temp.drop('weekday', axis=1)
    
    return temp

def main():

    # gameTypeCsv = dimGameType()
    # dateCsv = dimDate()
    # timeCsv = dimTime()
    # terminationCsv = dimTermination()
    playersCsv = dimPlayer()
    # gameCsv = factGame()
    
    # gameTypeCsv.to_csv(r'dim/dimGameType.csv',index=None)
    # dateCsv.to_csv(r'dim/dimDate.csv',index=None)
    # timeCsv.to_csv(r'dim/dimTime.csv',index=None)
    # terminationCsv.to_csv(r'dim/dimTermination.csv',index=None)
    playersCsv.to_csv(r'dimPlayer.csv',index=None)
    # gameCsv.to_csv(r'fact/factGame.csv',index=None)
    
if __name__ == '__main__':
    main()
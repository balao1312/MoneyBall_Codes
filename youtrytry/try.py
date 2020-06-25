import pandas as pd
import MySQLdb

# 從 mysql 讀 id_reference 的 table
# db = MySQLdb.connect(host='1.tcp.jp.ngrok.io',user='balao1312'\
#                      ,passwd='',db='MoneyBallDatabase', port=23879, charset='utf8')
#
# cursor = db.cursor() #建立游標
#
# sql_str='select * from pitcher_stats_v1'
# cursor.execute(sql_str)
# df = pd.DataFrame(cursor.fetchall())
# df.to_csv('pitcher.csv', encoding='utf8', index=False)
# db.close()

# 已經讀過存檔，直接從檔案讀
#df = pd.read_csv('id3394.csv')

df_batter = pd.read_csv('ttt.csv')
df_game = pd.read_csv('games.csv')
df_pitcher = pd.read_csv('pitcher.csv')
#print(df_game.head())
for id,date,a1,a2,a3,a4,a5,a6,a7,a8,a9 in zip(df_game['home_pitcher'],df_game['date'],df_game['away_1'],df_game['away_2'],df_game['away_3'],df_game['away_4'],df_game['away_5'],df_game['away_6'],df_game['away_7'],df_game['away_8'],df_game['away_9']):
    year = date[0:4]
    i = df_pitcher[df_pitcher['mlb_id'].isin([str(id)]) & df_pitcher['Season'].isin([str(year)])]
    print(i)
    data = i.fillna(0)
    pticher_xFIP = data['xFIP'].values
    FB = data['FB_percentage'].values
    SL = data['SL_percentage'].values
    CT = data['CT_percentage'].values
    CB = data['CB_percentage'].values
    CH = data['CH_percentage'].values
    SF = data['SF_percentage'].values
    KN = data['KN_percentage'].values
    batter = [a1,a2,a3,a4,a5,a6,a7,a8,a9]
    type = ['FB','SL','CT','CB','CH','SF','KN']
    val = [FB,SL,CT,CB,CH,SF,KN]
    # for b in batter:
    #     x = []
    #     for p,q in zip(type,val):
    #         batter2 = df_batter[df_batter['player'].isin([b]) & df_batter['season'].isin([str(s)]) & df_batter['pitch_type'].isin([p])]
    #         print(batter2)
    #         # if batter2['score'] :
    #         #     print(batter)
    #         #     print('!!'*200)
    #         value= batter2['score'].values *q
    #         x.append(value)
    #     y = sum(x)
    #     print('y', y)
    #     print('********')

    break
    # b1 = df_batter[df_batter['mlb_id'].isin(str(a1))& df_batter['Season'].isin([str(s)])]
    # b2 = df_batter[df_batter['mlb_id'].isin(str(a2))& df_batter['Season'].isin([str(s)])]
    # b3 = df_batter[df_batter['mlb_id'].isin(str(a3))& df_batter['Season'].isin([str(s)])]
    # b4 = df_batter[df_batter['mlb_id'].isin(str(a4))& df_batter['Season'].isin([str(s)])]
    # b5 = df_batter[df_batter['mlb_id'].isin(str(a5))& df_batter['Season'].isin([str(s)])]
    # b6 = df_batter[df_batter['mlb_id'].isin(str(a6))& df_batter['Season'].isin([str(s)])]
    # b7 = df_batter[df_batter['mlb_id'].isin(str(a7))& df_batter['Season'].isin([str(s)])]
    # b8 = df_batter[df_batter['mlb_id'].isin(str(a8))& df_batter['Season'].isin([str(s)])]
    # b9 = df_batter[df_batter['mlb_id'].isin(str(a9))& df_batter['Season'].isin([str(s)])]
    #







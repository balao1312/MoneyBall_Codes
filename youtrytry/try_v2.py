import pandas as pd
import MySQLdb

# 從 mysql 讀 id_reference 的 table
# db = MySQLdb.connect(host='1.tcp.jp.ngrok.io',user='balao1312'\
#                      ,passwd='clubgogo',db='MoneyBallDatabase', port=23879, charset='utf8')
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
demo2 = []

for info in df_game.values.tolist():
    try:
        s = info[1][0:4] #比賽日期抓年份

        i_home = df_pitcher[df_pitcher['mlb_id'].isin([str(info[4])]) & df_pitcher['Season'].isin([str(s)])] #game表格home_pitcher欄位搜尋pitcher表格id
        data_home = i_home.fillna(0)

        i_away = df_pitcher[df_pitcher['mlb_id'].isin([str(info[5])]) & df_pitcher['Season'].isin([str(s)])]  # game表格away_pitcher欄位搜尋pitcher表格id
        data_away = i_away.fillna(0)


        #home_pitcher資料
        h_pitcher_xFIP = data_home['xFIP'].values[0]
        h_FB = data_home['FB_percentage'].values[0]
        h_SL = data_home['SL_percentage'].values[0]
        h_CT = data_home['CT_percentage'].values[0]
        h_CB = data_home['CB_percentage'].values[0]
        h_CH = data_home['CH_percentage'].values[0]
        h_SF = data_home['SF_percentage'].values[0]
        h_KN = data_home['KN_percentage'].values[0]
        batter_away = [info[6],info[7],info[8],info[9],info[10],info[11],info[12],info[13],info[14]] #game表格所有客隊打者id
        type = ['FB','SL','CT','CB','CH','SF','KN']
        h_val = [h_FB,h_SL,h_CT,h_CB,h_CH,h_SF,h_KN]


        # away_pitcher資料
        a_pitcher_xFIP = data_home['xFIP'].values[0]
        a_FB = data_home['FB_percentage'].values[0]
        a_SL = data_home['SL_percentage'].values[0]
        a_CT = data_home['CT_percentage'].values[0]
        a_CB = data_home['CB_percentage'].values[0]
        a_CH = data_home['CH_percentage'].values[0]
        a_SF = data_home['SF_percentage'].values[0]
        a_KN = data_home['KN_percentage'].values[0]
        batter_home = [info[15], info[16], info[17], info[18], info[19], info[20], info[21], info[22],info[23]]  # game表格所有客隊打者id
        a_val = [a_FB, a_SL, a_CT, a_CB, a_CH, a_SF, a_KN]



        demo_home = []
        demo_away = []
        #處理客隊打者資料
        for b in batter_away:
            x = []
            for p,q in zip(type,h_val):
                #依照game表格搜尋到的打者id/年份/球種，對應batter表格打者成績
                batter2 = df_batter[df_batter['player'].isin([b]) & df_batter['season'].isin([str(s)]) & df_batter['pitch_type'].isin([p])]
                value= batter2['score'].values *q
                x.append(value)
            df = pd.DataFrame(x).replace('',0.1407142857142857) #空值補中位數
            y = df.sum().values
            demo_away.append(y)

        # 處理主隊打者資料
        for b in batter_home:
            z = []
            for p,q in zip(type,a_val):
                #依照game表格搜尋到的打者id/年份/球種，對應batter表格打者成績
                batter2 = df_batter[df_batter['player'].isin([b]) & df_batter['season'].isin([str(s)]) & df_batter['pitch_type'].isin([p])]
                value= batter2['score'].values *q
                z.append(value)
            df = pd.DataFrame(z).replace('',0.1407142857142857) #空值補中位數
            y = df.sum().values
            demo_home.append(y)
        
        all = [info[1],h_pitcher_xFIP,demo_away,a_pitcher_xFIP,demo_home,info[25],info[24],info[26],info[29]]
        #info[25] = away_total
        #info[24] = home_total
        #info[26] = yard_wOBA
        #info[29] = umpire_K/BB
        demo2.append(all)
        print(len(demo2))






    except Exception as e:
        print(e)
        pass


new_df = pd.DataFrame(demo2,columns=['date','home_pitcher','away_batter1','away_batter2','away_batter3','away_batter4','away_batter5','away_batter6','away_batter7','away_batter8',
                                     'away_batter9','away_pitcher','home_batter1','home_batter2','home_batter3','home_batter4','home_batter5','home_batter6','home_batter7',
                                     'home_batter8','home_batter9','away_total','home_total','yard_wOBA','umpire_K/BB'])
new_df.to_csv('demo.csv',index=False)










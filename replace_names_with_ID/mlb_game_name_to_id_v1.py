import MySQLdb
import pandas as pd

# 從 mysql 讀 id_reference 的 table
# db = MySQLdb.connect(host='192.168.33.10',user='balao1312'\
#                      ,passwd='clubgogo',db='MoneyBallDatabase', port=3306, charset='utf8')
#
# cursor = db.cursor() #建立游標
#
# sql_str='select * from ID_reference'
# cursor.execute(sql_str)
# df = pd.DataFrame(cursor.fetchall())
# df.to_csv('id3394.csv', encoding='utf8', index=False)
# db.close()

# 已經讀過存檔，直接從檔案讀
df = pd.read_csv('id3394.csv')
#print(df)

# 找出重名
player_dd = {}   # 之後用來 name 轉 id 的字典變數
player_repeat = set()   # 找出重名的名字
for i in range(df.shape[0]):
    if df.iat[i,1] in player_dd:
        player_dd[df.iat[i, 1]].append(df.iat[i, 0])     # 這邊用 list 去紀錄對應的 mlb_id 是為了重名時都記下
    else:
        player_dd[df.iat[i, 1]] = [df.iat[i,0]]
    # if i == 100:      # 小規模測試用
    #     break

    if len(player_dd[df.iat[i,1]]) > 1 :    # 如果字典中該名字出現超過一次，加入到 player_repeat 的 list
        #print(df.iat[i, 1])
        player_repeat.add(df.iat[i, 1])

#print(player_dd)
#print(player_repeat)
#print(len(player_repeat))
# for anyy in player_repeat:
#     print(player_dd[anyy])

# 讀入 mlb_game 的 table
df_mlb_game = pd.read_csv('mlb_game.csv')
print('\ntotal games : ', df_mlb_game.shape[0])     # 全部比賽數


# 修正 time 這欄格式
def time_reformat(x):
    return x[8:12]

df_mlb_game['time'] = df_mlb_game['time'].apply(time_reformat)

# 修正 woba 某幾欄小數後位數太多
def woba_format(x):
    return f'{x:.4f}'
df_mlb_game['wOBA'] = df_mlb_game['wOBA'].apply(woba_format)

#修正 K/BB 某幾欄小數後位數太多
def KBB_format(x):
    return f'{x:.5f}'
df_mlb_game['K/BB'] = df_mlb_game['K/BB'].apply(KBB_format)

# 刪掉 Games 這欄  應該是 是裁判的執法過比賽數
df_mlb_game = df_mlb_game.drop('Games',axis=1)

# print(df_mlb_game)


# 計算到底重名的球員影響幾場比賽 會用兩個迴圈去掃所有球員欄位

games_affected = 0 # 最後影響的場次
switch = 0  # 保險起見，為了防止同場有可能出現兩個以上的球員發生重名設的變數
no_id_player_dict = {}   # 沒有ID的球員
repeat_dict = {}   # 重名球員出現次數
def name_to_id(s):
    if s in player_dd and len(player_dd[s]) == 1:   # 如果該欄的值在之前建的字典裡，且該值只有一個對應 id
        return player_dd[s][0]  # 回傳該 id
    elif s in player_dd:    # 如果在字典裡 ， 字典值走到這行一定是大於1，就是發生重名
        global switch
        switch = 1          # 開關先切為1
        if s not in repeat_dict:   # 統計各重名球員出現次數
            repeat_dict[s] = 1
        else:
            repeat_dict[s] += 1
        return s        # 回傳原本的值 = 球員name
    else:
        #print(s)     # 如果真的有球員不在id對照表裡
        if s not in no_id_player_dict:  # 統計各重名球員出現次數
            no_id_player_dict[s] = 1
        else:
            no_id_player_dict[s] += 1
        return 'NO_ID: ' + s


for i in range(df_mlb_game.shape[0]):
    for j in range(3,23):   # 欄位 index 從 3~22 是球員
        df_mlb_game.iat[i,j] = name_to_id(df_mlb_game.iat[i,j])     # 每一欄的值都去 call name_to_id 做轉換

    if switch == 1:     # 掃完一個 row  如果有重名現象 switch會是1，加計到 games_affected 後 再把開關還原回 0
        games_affected += 1
        switch = 0

print('\n影響比賽數 : ', games_affected)
print('\n重名球員名單 : ', player_repeat)
print('\n找不到ID球員名單 : ', no_id_player_dict)
print('\n各重覆球員出現次數 : ', repeat_dict)

df_mlb_game.to_csv('temp.csv', encoding='utf8', index=False)

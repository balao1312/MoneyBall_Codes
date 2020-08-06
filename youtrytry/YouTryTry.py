import pandas as pd

# 投手
df_pitcher = pd.read_csv('pitcher.csv', names=[x for x in range(20)])

df_pitcher_essential = df_pitcher[[2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18]]
# df_pitcher_essential.to_csv('temp1.csv',encoding='utf-8-sig')
# print(df_pitcher_essential.shape)  # (7102,10)

pitcher_dic = {}

# 做一半發現有沒有mlb_id的 所以先跑一次迴圈計算
with_mlb_id = no_mlb_id = 0
for i in range(1, df_pitcher_essential.shape[0]):
    if df_pitcher_essential.iat[i, 1].isnumeric():
        with_mlb_id += 1
    else:
        no_mlb_id += 1
print(f'有轉換成 mlb_id 的有 {with_mlb_id} 個，沒轉換的有 {no_mlb_id} 個')

# 接著繼續做
for i in range(1, df_pitcher_essential.shape[0]):
    if df_pitcher_essential.iat[i, 1].isnumeric():  # 如果有轉換成 mlb_id 的
        mlb_id = int(df_pitcher_essential.iat[i, 1])
        year = int(df_pitcher_essential.iat[i, 0])

        if mlb_id not in pitcher_dic:
            pitcher_dic[mlb_id] = {}

        pitcher_dic[mlb_id].update({year: {}})

        # print(mlb_id,year)

        pitcher_dic[mlb_id][year]['team'] = df_pitcher_essential.iat[i, 2]
        pitcher_dic[mlb_id][year]['xFIP'] = df_pitcher_essential.iat[i, 3]

        pitcher_dic[mlb_id][year]['pitch_type'] = {}

        pitcher_dic[mlb_id][year]['pitch_type']['FB'] = df_pitcher_essential.iat[i, 4]
        pitcher_dic[mlb_id][year]['pitch_type']['SL'] = df_pitcher_essential.iat[i, 5]
        pitcher_dic[mlb_id][year]['pitch_type']['CT'] = df_pitcher_essential.iat[i, 6]
        pitcher_dic[mlb_id][year]['pitch_type']['CB'] = df_pitcher_essential.iat[i, 7]
        pitcher_dic[mlb_id][year]['pitch_type']['CH'] = df_pitcher_essential.iat[i, 8]
        pitcher_dic[mlb_id][year]['pitch_type']['SF'] = df_pitcher_essential.iat[i, 9]
        pitcher_dic[mlb_id][year]['pitch_type']['KN'] = df_pitcher_essential.iat[i, 10]

# print(len(pitcher_dic))
# print(pitcher_dic[477132])  # Clayton Kershow
# print(pitcher_dic[477132][2019])
# print(pitcher_dic[477132][2019]['pitch_type'])


# 打者
df_batter = pd.read_csv('batter.csv')
batter_dic = {}
# print(df_batter)

# 檢查有無球員沒有轉換成 id
# oo = 0
# for i in range(0,df_batter.shape[0]):
#     if str(df_batter.iat[i, 1]).isnumeric():
#         oo += 1
#     #print(df_batter.iat[i, 1])
# print(oo) # 49854 代表全都是 id 了


for i in range(df_batter.shape[0]):

    mlb_id = int(df_batter.iat[i, 1])
    year = int(df_batter.iat[i, 0])
    # print(mlb_id,year)

    if mlb_id not in batter_dic:
        batter_dic[mlb_id] = {}

    if year not in batter_dic[mlb_id]:
        batter_dic[mlb_id][year] = {}

    batter_dic[mlb_id][year][df_batter.iat[i, 2]] = df_batter.iat[i, 9]
    # print(batter_dic)

print(batter_dic[110029][2010])


# 計算球種打擊率總合  先用當年的數據算，若要用去年的就是year-1
def youtrytry(year: int, pitcher_mlb_id: int, batter_mlb_id: int):
    # 確認投手在該年有數據
    try:
        pitcher_dic[pitcher_mlb_id][year]
    except:
        # print(f'投手 {pitcher_mlb_id} 在 {year} 沒有數據')
        return 0.0

    sum = 0.0
    for pitch_type, percentage in pitcher_dic[pitcher_mlb_id][year]['pitch_type'].items():
        # print(type(percentage))
        # print(type(pitch_type))
        if str(percentage) == 'nan':
            continue
        # print(pitch_type, percentage)
        # print(type(percentage))

        # print(batter_dic[batter_mlb_id][year][pitch_type])
        try:
            sum += float(percentage) * batter_dic[batter_mlb_id][year][pitch_type]
            # print(f'球種 {pitch_type} : 投手投出比例 : {percentage}  打者打擊率 : {batter_dic[batter_mlb_id][year][pitch_type]} '
            #      f'小計 : {float(percentage) * batter_dic[batter_mlb_id][year][pitch_type]}')

        except Exception as e:
            print('有空值！用中位數填補')
            sum += float(percentage) * 0.14  # 用 0.14 當中位數
        # print(f'目前總和 : {sum}')

    print(f'對決期望值 : {sum}\n')
    return sum


# print(batter_dic[116539])


# 比賽
df_game = pd.read_csv('games.csv')

# 先改打者欄位型態
for i in range(1, 10):
    df_game = df_game.astype({f'away_{i}': float})
    df_game = df_game.astype({f'home_{i}': float})

# 開始跑每一欄，替換成對決期望值
for i in range(df_game.shape[0]):
    year = int(df_game.iat[i, 1][:4])
    home_pitcher = int(df_game.iat[i, 4])
    away_pitcher = int(df_game.iat[i, 5])
    # print(year,home_pitcher)

    # 確定投手有數據:
    # 主場投手
    try:
        print(f'投手 {home_pitcher} 在 {year} 的 數據 : \n{pitcher_dic[home_pitcher][year]}')
    except KeyError as e:
        print(f'投手 {home_pitcher} 在 {year} 沒有數據 {e}')
        continue  # 先跳過
    # 客場投手
    try:
        print(f'投手 {away_pitcher} 在 {year} 的 數據 : \n{pitcher_dic[home_pitcher][year]}')
    except KeyError as e:
        print(f'投手 {away_pitcher} 在 {year} 沒有數據 {e}')
        continue  # 先跳過

    # 主隊投手 vs 客隊打者 ( index 6 ~ 15)
    for j in range(6, 15):
        batter = int(df_game.iat[i, j])

        # 確定打者有數據
        try:
            print(f'打者 {batter} 在 {year} 的數據 : \n{batter_dic[batter][year]}')
        except KeyError as e:
            print(f'打者 {batter} 在 {year} 沒有數據 {e}')
            continue  # 先跳過

        # 修改回 game csv
        df_game.iat[i, j] = youtrytry(year, home_pitcher, batter)

    # 客隊投手 vs 主隊打者 ( index 15 ~ 23)
    for j in range(15, 23):
        batter = int(df_game.iat[i, j])

        # 確定打者有數據
        try:
            print(f'打者 {batter} 在 {year} 的數據 : \n{batter_dic[batter][year]}')
        except KeyError as e:
            print(f'打者 {batter} 在 {year} 沒有數據 {e}')
            continue  # 先跳過

        # 修改回 game csv

        df_game.iat[i, j] = youtrytry(year, away_pitcher, batter)

    print('==' * 30)

df_game.to_csv('result.csv', encoding='utf-8-sig')

import pandas

df_total = pandas.read_csv('total.csv')
#print(df_total)

df_1B = pandas.read_csv('1B.csv')
#print(df_1B)

df_2B = pandas.read_csv('2B.csv')
df_3B = pandas.read_csv('3B.csv')
df_HR = pandas.read_csv('HR.csv')

df_temp = pandas.merge(df_total, df_1B, on=['year','mlb_id', 'pitch_type'], how='left')
#print(df_temp)
df_temp = pandas.merge(df_temp, df_2B, on=['year','mlb_id', 'pitch_type'], how='left')
df_temp = pandas.merge(df_temp, df_3B, on=['year','mlb_id', 'pitch_type'], how='left')
df_temp = pandas.merge(df_temp, df_HR, on=['year','mlb_id', 'pitch_type'], how='left')

df_temp = df_temp.fillna(0)
df_temp['BA'] = (df_temp['1B_times'] + df_temp['2B_times'] + df_temp['3B_times'] +
                 df_temp['HR_times']) / df_temp['times']

df_temp['BA_weight'] = (df_temp['1B_times']*0.89 + df_temp['2B_times']*1.27 +
                        df_temp['3B_times']*1.62 +
                 df_temp['HR_times']*2.1) / df_temp['times']

afilter = df_temp[(df_temp['year'] == 2008) | (df_temp['year'] == 2009) ].index

df_temp.drop(afilter,inplace=True)
df_temp.to_csv('statistics.csv',encoding='utf8')
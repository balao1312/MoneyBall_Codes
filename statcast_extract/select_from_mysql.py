import MySQLdb, pandas

db = MySQLdb.connect(host='',user='balao1312'\
                     ,passwd='',db='testlala', port=, charset='utf8')

cursor = db.cursor() #建立游標



# 選 total
# try:
#     sql_str='''select year(game_date) year,batter,pitch_type,count(pitch_type)
#             from mbdb_memory.statcast
#             group by year(game_date),batter,pitch_type'''
#
#     cursor.execute(sql_str)
#     df = pandas.DataFrame(cursor.fetchall())
#     print(df)
#     column_list = ['year', 'mlb_id', 'pitch_type', 'times']
#     df.columns = column_list
#     df.to_csv('total.csv',encoding='utf-8',index=False)
# except:
#     print('unable to fetch data from db')

# 選 1B
# try:
#     sql_str='''select year(game_date) year,batter,pitch_type,count(*)
#             from mbdb_memory.statcast
#             where events = 'single'
#             group by year(game_date),batter,pitch_type'''
#
#     cursor.execute(sql_str)
#     df = pandas.DataFrame(cursor.fetchall())
#     column_list = ['year', 'mlb_id', 'pitch_type', '1B_times']
#     df.columns = column_list
#     print(df)
#     df.to_csv('1B.csv',encoding='utf-8',index=False)
# except:
#     print('unable to fetch data from db')

# 選 2B
# try:
#     sql_str='''select year(game_date) year,batter,pitch_type,count(*)
#             from mbdb_memory.statcast
#             where events = 'double'
#             group by year(game_date),batter,pitch_type'''
#
#     cursor.execute(sql_str)
#     df = pandas.DataFrame(cursor.fetchall())
#     column_list = ['year', 'mlb_id', 'pitch_type', '2B_times']
#     df.columns = column_list
#     print(df)
#     df.to_csv('2B.csv',encoding='utf-8',index=False)
# except:
#     print('unable to fetch data from db')

# 選 3B
# try:
#     sql_str='''select year(game_date) year,batter,pitch_type,count(*)
#             from mbdb_memory.statcast
#             where events = 'triple'
#             group by year(game_date),batter,pitch_type'''
#
#     cursor.execute(sql_str)
#     df = pandas.DataFrame(cursor.fetchall())
#     column_list = ['year', 'mlb_id', 'pitch_type', '3B_times']
#     df.columns = column_list
#     print(df)
#     df.to_csv('3B.csv',encoding='utf-8',index=False)
# except:
#     print('unable to fetch data from db')

# 選 home run
try:
    sql_str='''select year(game_date) year,batter,pitch_type,count(*)
            from mbdb_memory.statcast
            where events = 'home_run'
            group by year(game_date),batter,pitch_type'''

    cursor.execute(sql_str)
    df = pandas.DataFrame(cursor.fetchall())
    column_list = ['year', 'mlb_id', 'pitch_type', 'HR_times']
    df.columns = column_list
    print(df)
    df.to_csv('HR.csv',encoding='utf-8',index=False)
except:
    print('unable to fetch data from db')


db.close()

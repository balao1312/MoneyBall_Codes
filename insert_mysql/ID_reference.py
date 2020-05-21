import pandas as pd
import MySQLdb, time

df = pd.read_csv('ID_reference.csv')
#print(df)
f_df = df[['mlb_id', 'mlb_name']]

tablename = 'ID_reference'

print(f_df[0:30])
print(f_df.shape[0])


db = MySQLdb.connect(host='1.tcp.jp.ngrok.io',user='balao1312'
                      ,passwd='clubgogo',db='MoneyBallDatabase', port=23879, charset='utf8')
# 本地
#db = MySQLdb.connect(host='localhost', user='balao1312'
#                     , passwd='clubgogo', db='MoneyBallDatabase', port=3306, charset='utf8')

cursor = db.cursor()  # 建立游標

excepts = 0
success = 0
for index in range(f_df.shape[0]):
    print(index)
    insert_list = []
    for i in range(f_df.shape[1]):
        insert_list.append(f_df.iat[index, i])

    insert_string = ''
    for each in insert_list:
        if type(each) == str:
            insert_string += f'"{each}", '
        else:
            each = str(each)
            if each == 'nan':
                each = 'NULL'
            insert_string += f'{each}, '

    insert_string = insert_string.strip()[:-1]  # 去掉最後的 ,
    try:
        sql_insert = f'''
                     insert into {tablename} (
                     mlb_id, mlb_name
                     ) values ({insert_string})
                     '''
        cursor.execute(sql_insert)
    except MySQLdb._exceptions.OperationalError as e:
        print(f'{e},\noccurs at "{insert_string}"')
        continue
    success += 1
#db.commit()
db.close()
t2 = time.time()
print(f'==> Done with {filename.stem} in {t2 - t1:.2f} seconds :\n')
print(f'\t{filename.stem} got {success} successful wrote to MySQL.')
print(f'\t{filename.stem} got {excepts} excepts.\n')

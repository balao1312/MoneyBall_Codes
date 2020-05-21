import MySQLdb
#建立資料庫連線
db = MySQLdb.connect(host='1.tcp.jp.ngrok.io',user='balao1312'\
                     ,passwd='clubgogo',db='testlala', port=23879, charset='utf8')

cursor = db.cursor() #建立游標
#try:
sql_str='select * from memberlist' #select語法
command = 'insert into memberlist (userid,username,email,salary,words_to_say)' \
          ' values (2,"test","testttt@hotmail.com",66666,"yeahhhhh")'
cursor.execute(command)

db.commit()
cursor.execute(sql_str)

datarows = cursor.fetchall()
for row in datarows:
    print(row)

# except:
#     print('unable to fetch data from db')
db.close()
from kafka import KafkaConsumer
import pymysql

def to_mysql(x):
    data = x.split('|')
    user_name = data[0]
    uid = data[1]
    msg = data[2].replace("'", "''")
    time = data[3]

    try:
        db = pymysql.connect(host='172.105.202.99', user='root'
                             , passwd='rootlala', db='MoneyBallDatabase', port=3306, charset='utf8')

        cursor = db.cursor()  # 建立游標

        sql_str = f"insert into userlog (user_name, uid, msg, time) values('{user_name}','{uid}','{msg}','{time}')"
        cursor.execute(sql_str)
        db.commit()
        db.close()
        print(f'Got 1 record : {x} , \n\tWritten to mysql')
    except Exception as e:
        print(e)


consumer = KafkaConsumer(
    'balaoo',
    bootstrap_servers=['172.105.202.99:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8'))

print('Start consuming ...')

for record in consumer:
    message = record.value
    # print(message)
    to_mysql(message)


# Mongo insert if needed
# from pymongo import MongoClient
# client = MongoClient('localhost:27017')
# collection = client.numtest.numtest
# collection.insert_one(message)        # in for loop


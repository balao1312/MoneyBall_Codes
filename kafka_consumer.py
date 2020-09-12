from confluent_kafka import Consumer, KafkaException, KafkaError
import sys, MySQLdb


# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)

# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None

# 指定要從哪個partition, offset開始讀資料
def my_assign(consumer_instance, partitions):
    for p in partitions:
        p.offset = 0
    print('assign', partitions)
    consumer_instance.assign(partitions)

def to_mysql(x):
    data = x.split('|')
    user_name = data[0]
    uid = data[1]
    msg = data[2].replace("'","''")
    time = data[3]

    try:
        db = MySQLdb.connect(host='34.80.186.211', user='balao1312' \
                             , passwd='clubgogo', db='MoneyBallDatabase', port=3306, charset='utf8')

        cursor = db.cursor()  # 建立游標

        sql_str = f"insert into userlog (user_name, uid, msg, time) values('{user_name}','{uid}','{msg}','{time}')"
        cursor.execute(sql_str)
        db.commit()
        db.close()
        print(f'\t\tWritten to mysql')
    except:
        print('syntax error')



if __name__ == '__main__':
    # 步驟1.設定要連線到Kafka集群的相關設定
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    props = {
        'bootstrap.servers': '35.194.137.72:9092',       # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'STUDENT_ID',                     # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'earliest',             # Offset從最前面開始
        'session.timeout.ms': 6000,                  # consumer超過6000ms沒有與kafka連線，會被認為掛掉了
        'error_cb': error_cb                         # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = "balaoo"
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName]) #  , on_assign=my_assign)
    # 步驟5. 持續的拉取Kafka有進來的訊息
    print('start consuming ...')
    try:
        while True:
            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('error')

                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    # 秀出metadata與msgKey & msgValue訊息
                    print(f'{topic}-{partition}-{offset} : \n\t{msgValue}')

                    to_mysql(msgValue)


    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(e)

    finally:
        # 步驟6.關掉Consumer實例的連線
        consumer.close()

import pandas as pd
from sqlalchemy import create_engine

host= '1.tcp.jp.ngrok.io'
port= 23879
user= 'balao1312'
password= ''
db = 'MoneyBallDatabase'


engine = create_engine(f'mysql+mysqldb://{user}:{password}@{host}:{port}/{db}')

try:
    df = pd.read_csv('statistics.csv', encoding='utf8')
    #df.to_sql('statistics',engine)
    print("Write to MySQL successfully!")
except Exception as e:
    print(e)
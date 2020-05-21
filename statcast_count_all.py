import pathlib, time
import pandas as pd
import concurrent.futures

t1 = time.time()
path = pathlib.Path('/Users/balao1312/sync/MoneyBallData/MLB_statcast_data')
print(path)

all_csvs = list(path.iterdir())
#print(all_csvs)

def count(csv_file):
    print('processing 1 file')
    df = pd.read_csv(csv_file)
    return df.shape[0]


with concurrent.futures.ProcessPoolExecutor() as executor:
    results = executor.map(count, all_csvs)

    c=0
    for result in results:
       c+=result

t2= time.time()
print(f'this task take {t2-t1:.2f} seconds')
print(f'\nTotal : \n{c}')
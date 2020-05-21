from pybaseball import batting_stats_range
import pandas as pd
import pathlib, time

# data = batting_stats_range('2019-05-01', '2019-05-31')
# print(data.shape)

years = [2019-x for x in range(20)]
years = list(map(str,years))
#print(years)
months = [ f'{x:02}' for x in range(1,13)]
#print(months)
days = [ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ]
days = list(map(str,days))
#print(len(days))
months_days = dict(zip(months,days))
#print(months_days)

folder_path = pathlib.Path.cwd().joinpath('MLB_batting_stats_range')
if not folder_path.exists():
    folder_path.mkdir()

for year in years:
    for month in months:
        print(f'Now Downloading : start_dt = {year}-{month}-01, end_dt={year}-{month}-{months_days[month]}')
        start_dt = f'{year}-{month}-01'
        end_dt = f'{year}-{month}-{months_days[month]}'
        filename = f'{year}-{month}_batting.csv'
        file_path = folder_path.joinpath(filename)

        if file_path.exists():
            print('-' * 40)
            print(f'{filename} already dowaloaded !')
            print('-' * 40)
            continue
        try:
            print('-' * 40)
            print(f'Start with {filename}')
            print('-' * 40)
            data = batting_stats_range(start_dt=start_dt, end_dt=end_dt)
            data.to_csv(file_path, encoding='utf_8_sig')
            print('-'*40)
            print(f'Done with {filename}')
            print('-' * 40)
        except Exception as e:
            print(f'\nError\n{e}')
            with open(folder_path.joinpath('errorlog.txt'),'a') as logfile:
                logfile.write(f'\noccur at {start_dt} ~ {end_dt}')
            continue

        time.sleep(10)

from pybaseball import pitching_stats
import pathlib, time

years = [2019-x for x in range(50)]
years = list(map(str,years))

folder_path = pathlib.Path.cwd().joinpath('pitching_status')
if not folder_path.exists():
    folder_path.mkdir()

for year in years:
    print(f'Start with {year}')
    filename = f'{year}_pitching_status.csv'
    file_path = folder_path.joinpath(filename)

    if file_path.exists():
        print(f'{year} already downloaded !')
        continue

    data = pitching_stats(year)
    data.to_csv(file_path, encoding='utf_8_sig')
    print(f'Done with {year}')

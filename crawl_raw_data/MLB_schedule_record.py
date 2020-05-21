from pybaseball import schedule_and_record
import pathlib, time

teams = [
        'NYY', 'ATL', 'MIA', 'NYM', 'PHI',
        'WSH', 'CHC', 'CIN', 'MIL', 'PIT',
        'STL', 'ARI', 'COL', 'LAD', 'SD',
        'SF',  'BAL', 'BOS', 'TB',  'TOR',
        'CWS', 'CLE', 'DET', 'KC',  'MIN',
        'HOU', 'LAA', 'OAK', 'SEA', 'TEX'
         ]
#print(teams)

years = [x for x in range(2019,1902,-1)]
#print(years)

folder_path = pathlib.Path.cwd().joinpath('MLB_schedule_records')
if not folder_path.exists():
    folder_path.mkdir()

for team in teams:
    for year in years:
        print(f'Now processing : {team} in {year}')
        team_folder_path = folder_path.joinpath(team)
        if not team_folder_path.exists():
            team_folder_path.mkdir()

        file_path = team_folder_path.joinpath(f'{team}_{year}.csv')
        if file_path.exists():
            print('Already Downloaded')
            continue
        try:
            data = schedule_and_record(year, team)
            data.to_csv(file_path, encoding='utf_8_sig')
            print('Done')
        except ValueError as e :
            print(f'{e}\ncontinue next')
        #time.sleep(5)


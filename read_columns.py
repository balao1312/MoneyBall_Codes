import pandas

df = pandas.read_csv('/Users/balao1312/sync/MoneyBallData/MLB_pitching_status/pitcher_requirement.csv')

print(df.columns)

req_column = list(df.columns)[1:]

print(req_column)
import pandas as pd


def simulate_ratio(msg):
    year = msg.split(',')[0][1:].strip()
    bet_amount = msg.split(',')[1].strip()
    # print(year, bet_amount)
    max_bet = 10000
    bet_ratio = 0.1
    win_rate = 0.9

    df = pd.read_csv('prediction_0722.csv')
    # print(df)

    win_or_lose_list = list(df[df['season'] == int(year)]['final_result']) if year != 'all' else \
        list(df['final_result'])
    # print(win_or_lose_list)

    total_games = len(win_or_lose_list)
    total_win = len([x for x in win_or_lose_list if x == 'win'])
    total_lose = len([x for x in win_or_lose_list if x == 'lose'])
    print(total_games, total_win, total_lose)
    print()

    now_amount = float(bet_amount)
    money_line = []
    for each in win_or_lose_list:
        if each == 'win':
            now_amount = now_amount * (1 + bet_ratio * win_rate) if now_amount * bet_ratio < max_bet else \
                now_amount + (max_bet * (1 + bet_ratio))
        elif each == 'lose':
            now_amount = now_amount * 0.9 if now_amount * bet_ratio < max_bet else now_amount - max_bet

        money_line.append(round(now_amount, 4))

    for each in money_line:
        print(each)

    print(total_games, total_win, total_lose)

    return


if __name__ == '__main__':
    simulate_ratio('$2019,1')

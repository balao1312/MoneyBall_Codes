import pandas as pd
import concurrent.futures
import time

def set_price(id):
    print(id)

    id_filter = (df['PRODUCT_ID'] == id)
    # print(f'the product id : {id} got {len(df[id_filter])} records')

    if len(df[id_filter]) <= 1:
        return
    else:
        id_price_dict = {}
        # print(df[id_filter]['Product_single_price'])
        for anyy in df[id_filter]['Product_single_price']:
            if anyy not in id_price_dict:
                id_price_dict[anyy] = 1
            else:
                id_price_dict[anyy] += 1
    # print(id_price_dict)

    id_price_dict_list_sorted = sorted(id_price_dict.items(), key=lambda x: x[1], reverse=True)
    # print(id_price_dict_list_sorted)
    max_count_price = id_price_dict_list_sorted[0][0]
    df.loc[id_filter, 'Product_single_price'] = max_count_price
    # print(df[id_filter]['Product_single_price'])

    return


if __name__ == "__main__":
    t1 = time.time()

    df = pd.read_csv('original.csv')
    pid_list = list(set(df['PRODUCT_ID']))
    pid_list.sort()

    # 多工會出錯
    # with concurrent.futures.ProcessPoolExecutor() as ex :
    #     ex.map(set_price,pid_list)

    for each_id in pid_list:
        set_price(each_id)

    df.to_csv('result.csv', encoding='utf-8-sig', index=False)
    t2 = time.time()
    print(f'\ntake {t2 - t1:.4f} seconds')

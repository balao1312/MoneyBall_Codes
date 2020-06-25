import pandas
import MySQLdb, time, pathlib
import concurrent.futures

def csv_to_mysql(filename):
    print(f'==> Now processing : {filename}')

    try:
        df = pandas.read_csv(filename, low_memory=False)
    except UnicodeDecodeError :
        print(f'{filename} may have problems.')
        return
    except pandas.errors.ParserError :
        print(f'{filename} may have problems.')
        return
    except KeyError :
        print(f'{filename} may have problems.')
        return
    except pandas.errors.EmptyDataError:
        print(f'{filename} may have problems.')
        return

    # print(f'==> Start processing {filename.stem} ....\n')
    t1 = time.time()

    column_list = ['Season', 'Name', 'Team', 'xFIP', 'FB%', 'FBv', 'SL%', 'SLv', 'CT%', 'CTv', 'CB%', 'CBv', 'CH%', 'CHv', 'SF%', 'SFv', 'KN%', 'KNv']


    # 讀到不是要求的csv的話 會選不到所需欄位
    try:
        f_df = df[column_list]
    except KeyError:
        print(f'{filename} may have problems.')
        return

    # 把 % 改成 _percentage
    transformed_column_list = []
    for each in column_list:
        if '%' in each:
            each = each.replace('%', '_percentage')
        transformed_column_list.append(each)
    #print('f :  ',transformed_column_list)
    column_list = transformed_column_list

    # mysql 建立資料庫連線
    # 教室
    db = MySQLdb.connect(host='1.tcp.jp.ngrok.io',user='balao1312'
                         ,passwd='',db='MoneyBallDatabase', port=23879, charset='utf8')
    # 本地
    # db= MySQLdb.connect(host='192.168.33.10', port=3306 ,user='balao1312'
    #                     ,passwd='',db='MoneyBallDatabase', charset='utf8')

    cursor = db.cursor() # 建立游標

    excepts = 0
    success = 0
    for index in range(f_df.shape[0]):

        insert_list = []
        for i in range(f_df.shape[1]):
            insert_list.append(f_df.iat[index,i])

        # 要送入 values ( aa, bb, 'cc', cc, dd, 'ee')
        insert_string = ''
        for each in insert_list:
            if type(each) == str:
                insert_string += f'"{each}", '
            else:
                each = str(each)
                if each == 'nan':
                    each = 'NULL'
                insert_string += f'{each}, '

        insert_string = insert_string.strip()[:-1] # 去掉最後的 ,

        columns_to_string = ''
        for each in column_list:
            columns_to_string += f'{each}, '

        columns_to_string = columns_to_string.strip()[:-1]  # 去掉最後的 ,
        try:
            sql_insert = f'''
                        insert into pitcher_stats (
                        {columns_to_string}
                        ) values ({insert_string})
                        '''
            #print(sql_insert)
            cursor.execute(sql_insert)
        except MySQLdb._exceptions.OperationalError as e :
            print(f'{e},\noccurs at query : \n"{sql_insert}"')
            continue
        success += 1
    # db.commit()
    db.close()
    t2 =time.time()
    print(f'==> Done with {filename.stem} in {t2-t1:.2f} seconds :\n')
    print(f'\t{filename.stem} got {success} successful wrote to MySQL.')
    print(f'\t{filename.stem} got {excepts} excepts.\n')
    return success  # 回傳成功寫入的筆數



if __name__ == '__main__':

    start = time.time()
    filelist = list(pathlib.Path.cwd().iterdir())

    if str(filelist[0])[-9:] == '.DS_Store':     # mac 系統檔
        del filelist[0]

    print(f'Total files : {len(filelist)}\n\n')
    #
    # for each_file in filelist:
    #     csv_to_mysql(each_file)

    # 多工
    with concurrent.futures.ProcessPoolExecutor() as excutor:
        results = excutor.map(csv_to_mysql,filelist)

        total_writen = 0
        for result in results:
            total_writen += result if result is not None else 0
            print(f'---------Successfully wrote {total_writen} records to MySQL.')

    end = time.time()
    print(f'\nAll tasks completed in {end-start:.2f} seconds.')
# 争取将所有操作都放到数据库中处理，减少本地内存的使用

from datetime import datetime, timedelta

# 为了兼容本地调试，要在所有代码钱调用此方法
def init():
    global execSql
    global dayStr
    global days

    if 'o' in globals():
        print('this is online version')

        from odps import options
        # UTC+0
        options.sql.settings = {'odps.sql.timezone':'Africa/Accra'}

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']
        days = args['days']
    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        dayStr = '20231110'
        days = '15'

    # 如果days不是整数，转成整数
    days = int(days)
    print('dayStr:', dayStr)
    print('days:', days)

# 处理skan数据第一步
def skanDataStep1(dayStr, days):
    # 创建临时表skanDataStep1
    # 
    pass
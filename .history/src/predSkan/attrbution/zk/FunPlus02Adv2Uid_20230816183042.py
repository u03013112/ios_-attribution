# FunPlus02Adv uid版 不再使用af id，而是使用uid
# 但是由于使用归因方式sql获得的数据有较大偏差，又没有时间去研究，所以暂时不用这个版本
# 还是先采用af id的方式，然后再用af id与uid的映射表，将af id转换为uid
# 映射部分单独做，本质上这个映射是多对多的，但是为了简化，简化成一对一的，即一个af id只对应一个uid，一个uid只对应一个af id
# 本任务要在FunPlus02Adv2完成后调用

import io

import numpy as np
import pandas as pd
from tqdm import tqdm

from datetime import datetime, timedelta

# 参数dayStr，是当前的日期，即${yyyymmdd-1}，格式为'20230301'
# 生成安装日期是dayStr - 7的各媒体7日回收金额

# 为了兼容本地调试，要在所有代码钱调用此方法
def init():
    global execSql
    global dayStr
    if 'o' in globals():
        print('this is online version')

        def execSql_online(sql):
            o.execute_sql(sql)

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']

    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql2 as execSql_local

        execSql = execSql_local

        dayStr = '20230404'
    
    print('dayStr:', dayStr)

def delTable(dayStr):
    sql = f'''ALTER TABLE topwar_ios_funplus02_adv_uid DROP PARTITION (day = '{dayStr}');'''
    print(sql)
    execSql(sql)
    return

def main(dayStr):
    before7days = datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=7)
    before7daysStr = before7days.strftime('%Y%m%d')
    sql = f'''
        INSERT INTO
            rg_bi.topwar_ios_funplus02_adv_uid
        SELECT
            e.customer_user_id,
            a.install_date,
            a.`facebook ads rate`,
            a.`googleadwords_int rate`,
            a.`bytedanceglobal_int rate`,
            a.`day`
        FROM
            rg_bi.topwar_ios_funplus02_adv a
            JOIN rg_bi.ods_platform_appsflyer_events e ON a.appsflyer_id = e.appsflyer_id
        WHERE
            a.`day` = '{dayStr}'
            AND e.`day` >= '{before7daysStr}' and e.`day` <= '{dayStr}'
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

init()
delTable(dayStr)
main(dayStr)


# 继续处理FunPlus02的结果

# FunPlus02得到的结果是一个数据库表
# 表名topwar_ios_funplus02_raw
# 表结构 
# 列1 appsflyer_id string
# 列2 install_date string 类似 ‘2023-05-31’
# 列3 day string 类似 ‘20230531’
# 列4 ‘facebook ads count’ double 
# 列5 ‘googleadwords_int count’ double
# 列6 ‘bytedanceglobal_int count’ double

# 表名ods_platform_appsflyer_events
# 表结构 
# 列1 appsflyer_id string
# 列2 install_timestamp bigint unix时间戳，单位秒
# 列3 event_timestamp bigint unix时间戳，单位秒
# 列4 event_revenue_usd double 事件收入，单位美元

# topwar_ios_funplus02_raw与ods_platform_appsflyer_events合并，计算上面3个媒体每日(install_date)的7日（7*24小时）回收金额
# 其中 ods_platform_appsflyer_events 与 topwar_ios_funplus02_raw 的合并条件是：ods_platform_appsflyer_events.appsflyer_id = topwar_ios_funplus02_raw.appsflyer_id
# 但是获取媒体count的时候要先将topwar_ios_funplus02_raw按照appsflyer_id分组，媒体count要求和
# 其中媒体的7日回收金额 计算方式为：ods_platform_appsflyer_events中event_timestamp在install_timestamp的7天内的event_revenue_usd的和 * 媒体 count
# 比如：媒体facebook的7日回收金额 计算方式为：ods_platform_appsflyer_events中event_timestamp在install_timestamp的7天内的event_revenue_usd的和 * facebook ads count


import numpy as np
import pandas as pd
from datetime import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def getAfDataFromMC():
    # 将minValidInstallTimestamp和maxValidInstallTimestamp转换为字符串
    minValidInstallTimestampStr = '2023-04-01'
    maxValidInstallTimestampStr = '2023-07-01'
    
    minValidInstallTimestampDayStr = '20230401'
    maxValidInstallTimestampDayStr = '20230701'

    # 修改后的SQL语句，r1usd用来计算cv，r2usd可能可以用来计算48小时cv，暂时不用r7usd，因为这个时间7日应该还没有完整。
    sql = f'''
        SELECT
            appsflyer_id,
            SUM(CASE WHEN event_timestamp <= install_timestamp + 168 * 3600 THEN event_revenue_usd ELSE 0 END) as r7usd
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND zone = 0
            AND day BETWEEN '{minValidInstallTimestampDayStr}' AND '{maxValidInstallTimestampDayStr}'
            AND install_time BETWEEN '{minValidInstallTimestampStr}' AND '{maxValidInstallTimestampStr}'
        GROUP BY
            appsflyer_id
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df


def getFunPlus02Ret():
    sql = '''
        select *
        from topwar_ios_funplus02_raw
        where day > 0
    '''
    print(sql)
    df = execSql(sql)
    return df

def main():
    # df = getAfDataFromMC()
    # df.to_csv(getFilename('FunPlus02t1'), index=False)

    # df2 = getFunPlus02Ret()
    # df2.to_csv(getFilename('FunPlus02t2'), index=False)

    mediaList = [
        'facebook ads',
        'googleadwords_int',
        'bytedanceglobal_int',
    ]

    df = pd.read_csv(getFilename('FunPlus02t1'))
    df2 = pd.read_csv(getFilename('FunPlus02t2'))

    print(df2[df2['appsflyer_id'] == '1579604536781-2607914'])
    return

    df2.drop(['day'], axis=1, inplace=True)
    df2 = df2.groupby(['appsflyer_id','install_date']).sum().reset_index()
    print(df2.head())
    # 计算media count 的sum 大于1的行的占比
    df2['total'] = df2[['%s count'%media for media in mediaList]].sum(axis=1)
    l1 = len(df2[df2['total'] > 1])
    l2 = len(df2)
    print('media count 的sum 大于1的行的占比: %d / %d = %.2f'%(l1,l2,l1/l2))

    mergeDf = df2.merge(df, on='appsflyer_id')

    
    for media in mediaList:
        mergeDf[media] = mergeDf['%s count'%media] * mergeDf['r7usd']

    mergeDf.groupby(['install_date']).sum().to_csv(getFilename('FunPlus02t3'), index=True)

if __name__ == '__main__':
    main()



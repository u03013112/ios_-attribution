# 计算 skan 4 所需要的分档
# 主要需要一套 64 分档
# 3套3分档
# 其中 64 分档为 24 或 48 小时付费，可以出一套 32 档位的版本
# 3分档为 7天，35天
 
import time
import datetime
import numpy as np
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def getDataFromMC():
    sql = '''
        WITH purchases AS (
            SELECT
                appsflyer_id AS uid,
                to_char(
                    to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                    "yyyy-mm-dd"
                ) AS install_date,
                install_timestamp,
                event_timestamp,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                event_name in ('af_purchase','af_purchase_oldusers')
                AND zone = 0
                AND day >= '20230101'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
                AND to_date('2023-05-01', "yyyy-mm-dd")
        )
        SELECT
            uid,
            install_date,
            COALESCE(
                sum(event_revenue_usd) FILTER (
                    WHERE
                        event_timestamp <= install_timestamp + 86400
                ),
                0
            ) AS r1usd,
            COALESCE(
                sum(event_revenue_usd) FILTER (
                    WHERE
                        event_timestamp <= install_timestamp + 2 * 86400
                ),
                0
            ) AS r2usd,
            COALESCE(
                sum(event_revenue_usd) FILTER (
                    WHERE
                        event_timestamp <= install_timestamp + 7 * 86400
                ),
                0
            ) AS r7usd,
            COALESCE(
                sum(event_revenue_usd) FILTER (
                    WHERE
                        event_timestamp <= install_timestamp + 30 * 86400
                ),
                0
            ) AS r30usd,
            COALESCE(
                sum(event_revenue_usd) FILTER (
                    WHERE
                        event_timestamp <= install_timestamp + 35 * 86400
                ),
                0
            ) AS r35usd
        FROM
            purchases
        GROUP BY
            uid,
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('skan4_20230101_20230501'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('skan4_20230101_20230501'))
    # 过滤 uid 为空的数据
    df = df.loc[df['uid'].notnull()]
    # 过滤 r35usd 为 0 的数据
    df = df.loc[df['r35usd'] > 0]
    return df

def makeLevels1(userDf, usd='r1usd', N=32):
    # `makeLevels1`函数接受一个包含用户数据的DataFrame（`userDf`），一个表示用户收入的列名（`usd`，默认为'r1usd'），以及分组的数量（`N`，默认为8）。
    # 其中第0组特殊处理，第0组是收入等于0的用户。
    # 过滤收入大于0的用户进行后续分组，分为N-1组，每组的总收入大致相等。
    # 根据收入列（`usd`）对用户DataFrame（`userDf`）进行排序。
    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值。
    # 计算所有这些用户的总收入。
    # 计算每组的目标收入（总收入除以分组数量）。
    # 初始化当前收入（`current_usd`）和组索引（`group_index`）。
    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入。然后，将该用户的收入值存储到`levels`数组中，并将当前收入重置为0，组索引加1。当组索引达到N-1时，停止遍历。
    # 返回`levels`数组。
    
    # 过滤收入大于0的用户
    filtered_df = userDf[userDf[usd] > 0]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df[usd].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N)

    # 初始化当前收入（`current_usd`）和组索引（`group_index`）
    current_usd = 0
    group_index = 0

    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入
    for index, row in df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            # 将该用户的收入值存储到`levels`数组中
            levels[group_index] = row[usd]
            # 将当前收入重置为0，组索引加1
            current_usd = 0
            group_index += 1
            # 当组索引达到N-1时，停止遍历
            if group_index == N - 1:
                break

    return levels

def addCv(userDf,cvMapDf,usd='hour24price',cv='cv1',usdp='hour24priceP'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf.loc[cvMapDf['cv']==cv1,'min_event_revenue'].values[0]
        max = cvMapDf.loc[cvMapDf['cv']==cv1,'max_event_revenue'].values[0]

        avg = cvMapDf['avg'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = cv1
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),usdp
        ] = avg
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = cv1
    userDfCopy.loc[userDfCopy[usd]>max,usdp] = avg

    # return userDfCopy[['uid',cv,usdp]]
    return userDfCopy

def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0],
        'avg':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        min = mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1]
        max = levels[i]
        mapData['min_event_revenue'].append(min)
        mapData['max_event_revenue'].append(max)
        mapData['avg'].append((min+max)/2)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def main():
    df = loadData()

    l1_32 = makeLevels1(df, 'r1usd', 32)
    makeCvMap(l1_32).to_csv(getFilename('cvMap1_32'), index=False)
    df1_32 = addCv(df,makeCvMap(l1_32),'r1usd','cv','r1usdP')
    df1_32Group = df1_32.groupby('install_date').agg({'r1usd':'sum','r1usdP':'sum'})
    mape = np.mean(np.abs((df1_32Group['r1usd'] - df1_32Group['r1usdP']) / df1_32Group['r1usd'])) * 100
    print('24小时 32档 mape: %.2f%%'%mape)

    l1_64 = makeLevels1(df, 'r1usd', 64)
    makeCvMap(l1_64).to_csv(getFilename('cvMap1_64'), index=False)
    df1_64 = addCv(df,makeCvMap(l1_64),'r1usd','cv','r1usdP')
    df1_64Group = df1_64.groupby('install_date').agg({'r1usd':'sum','r1usdP':'sum'})
    mape = np.mean(np.abs((df1_64Group['r1usd'] - df1_64Group['r1usdP']) / df1_64Group['r1usd'])) * 100
    print('24小时 64档 mape: %.2f%%'%mape)

    l2_32 = makeLevels1(df, 'r2usd', 32)
    makeCvMap(l2_32).to_csv(getFilename('cvMap2_32'), index=False)
    df2_32 = addCv(df,makeCvMap(l2_32),'r2usd','cv','r2usdP')
    df2_32Group = df2_32.groupby('install_date').agg({'r2usd':'sum','r2usdP':'sum'})
    mape = np.mean(np.abs((df2_32Group['r2usd'] - df2_32Group['r2usdP']) / df2_32Group['r2usd'])) * 100
    print('48小时 32档 mape: %.2f%%'%mape)
    
    l2_64 = makeLevels1(df, 'r2usd', 64)
    makeCvMap(l2_64).to_csv(getFilename('cvMap2_64'), index=False)
    df2_64 = addCv(df,makeCvMap(l2_64),'r2usd','cv','r2usdP')
    df2_64Group = df2_64.groupby('install_date').agg({'r2usd':'sum','r2usdP':'sum'})
    mape = np.mean(np.abs((df2_64Group['r2usd'] - df2_64Group['r2usdP']) / df2_64Group['r2usd'])) * 100
    print('48小时 64档 mape: %.2f%%'%mape)

    l1_4 = makeLevels1(df, 'r1usd', 4)
    makeCvMap(l1_4).to_csv(getFilename('cvMap1_4'), index=False)
    df1_4 = addCv(df,makeCvMap(l1_4),'r1usd','cv','r1usdP')
    df1_4Group = df1_4.groupby('install_date').agg({'r1usd':'sum','r1usdP':'sum'})
    mape = np.mean(np.abs((df1_4Group['r1usd'] - df1_4Group['r1usdP']) / df1_4Group['r1usd'])) * 100
    print('24小时 4档 mape: %.2f%%'%mape)

    l2_4 = makeLevels1(df, 'r2usd', 4)
    makeCvMap(l2_4).to_csv(getFilename('cvMap2_4'), index=False)
    df2_4 = addCv(df,makeCvMap(l2_4),'r2usd','cv','r2usdP')
    df2_4Group = df2_4.groupby('install_date').agg({'r2usd':'sum','r2usdP':'sum'})
    mape = np.mean(np.abs((df2_4Group['r2usd'] - df2_4Group['r2usdP']) / df2_4Group['r2usd'])) * 100
    print('48小时 4档 mape: %.2f%%'%mape)

    l7_4 = makeLevels1(df, 'r7usd', 4)
    makeCvMap(l7_4).to_csv(getFilename('cvMap7_4'), index=False)
    df7_4 = addCv(df,makeCvMap(l7_4),'r7usd','cv','r7usdP')
    df7_4Group = df7_4.groupby('install_date').agg({'r7usd':'sum','r7usdP':'sum'})
    mape = np.mean(np.abs((df7_4Group['r7usd'] - df7_4Group['r7usdP']) / df7_4Group['r7usd'])) * 100
    print('7天 4档 mape: %.2f%%'%mape)

    l35_4 = makeLevels1(df, 'r35usd', 4)
    makeCvMap(l35_4).to_csv(getFilename('cvMap35_4'), index=False)
    df35_4 = addCv(df,makeCvMap(l35_4),'r35usd','cv','r35usdP')
    df35_4Group = df35_4.groupby('install_date').agg({'r35usd':'sum','r35usdP':'sum'})
    mape = np.mean(np.abs((df35_4Group['r35usd'] - df35_4Group['r35usdP']) / df35_4Group['r35usd'])) * 100
    print('35天 4档 mape: %.2f%%'%mape)

if __name__ == '__main__':
    # getDataFromMC()

    main()
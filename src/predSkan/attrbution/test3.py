# 可以算一下2.1-2.08这段时间，按照af的建议区间算出来的整体roi、分fb、tt、gg三个渠道的每日skan roi吗？

import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

def getDataFromMC():
    sql = '''
        select
            customer_user_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                    when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r1usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= 20230201
        group by
            install_date,
            customer_user_id
    '''

    df = execSql(sql)
    return df

def mapeFunc(df,map):
    # 先按照map给每个人加上cv
    df.loc[:,'cv'] = 0
    for i in range(len(map)):
        min_event_revenue = map.min_event_revenue[i]
        max_event_revenue = map.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        df.loc[
            (df.r1usd > min_event_revenue) & (df.r1usd <= max_event_revenue),
            'cv'
        ] = i
    df.loc[
        (df.r1usd > max_event_revenue),
        'cv'
    ] = len(map)-1
    
    # for debug
    df.to_csv(getFilename('iosCv_user_cv'))
    # 然后按照install date进行汇总
    df.loc[:,'count'] = 1
    df = df.groupby(['install_date','cv'],as_index=False).agg({'r1usd':'sum','count':'sum'})
    df = df.sort_values(['install_date','cv'])
    
    # 对install date进行cv转usd的计算
    # print(df)
    df.loc[:,'cv_usd'] = 0
    for i in range(len(map)):
        min_event_revenue = map.min_event_revenue[i]
        max_event_revenue = map.max_event_revenue[i]
        avg = (min_event_revenue + max_event_revenue)/2
        if pd.isna(max_event_revenue):
            avg = 0
        if avg < 0:
            avg = 0
        count = df.loc[df.cv==i,'count']
        df.loc[df.cv==i,'cv_usd'] = count * avg
    # print(df)
    df.to_csv(getFilename('tmp_01'))
    # 计算mape
    df = df.groupby('install_date',as_index=False).agg({'r1usd':'sum','cv_usd':'sum'})
    df.loc[df.r1usd >= df.cv_usd,'mape']=(df.r1usd - df.cv_usd)/df.r1usd
    df.loc[df.r1usd < df.cv_usd,'mape']=(df.cv_usd - df.r1usd)/df.r1usd
    # print(df)
    df.to_csv(getFilename('iosCv_user_cv2usd'))

    return df

from sklearn.metrics import r2_score
# 这个要在mapeFunc之后调用
def r2Func(df):
    return r2_score(df.r1usd,df.cv_usd)



if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getDataFromMC()
        df.to_csv(getFilename('iOS_user_20230201'))

    df = pd.read_csv(getFilename('iOS_user_20230201'))
    df = df.loc[(df.install_date >= '2023-02-01') & (df.install_date <='2023-02-08')]

    df = df.sort_values(['install_date','r1usd'])
    df.to_csv(getFilename('iosCv_user_sort'))

    # levels = [
    #     1.6969,3.5726,6.0103,8.8024,12.3524,16.7171,21.701,27.2172,33.2045,40.5426,49.1994,58.9601,70.2909,83.6891,97.1486,
    #     114.7633,132.0923,153.4342,184.3833,211.8377,242.5758,271.612,303.1287,335.7322,382.8155,440.3342,526.2914,783.6587,973.4348,1152.2711
    # ]
    levels = [
        1,2,3,4,5,6,7,8,9,10,11,12,13,15,17,19,24,29,34,44,55,65,75,85,95,115,135,155,175,195,215
    ]

    # 根据levels制作map
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0]
    }

    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
        mapData['max_event_revenue'].append(levels[i])

    # 最后将最后一个固定档位插入
    mapData['cv'].append(len(mapData['cv']))
    mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
    # mapData['max_event_revenue'].append(1243.25)
    mapData['max_event_revenue'].append(235)

    mapDf = pd.DataFrame(data=mapData)
    mapDf.to_csv(getFilename('iosCv_map2'))

    mapeDf = mapeFunc(df, mapDf)

    mape = mapeDf.mape.mean()

    r2 = r2Func(mapeDf)

    print(mapDf)
    print('mape:',mape)
    print('r2:',r2)

    mapeDf.to_csv(getFilename('iosCv_f2'))
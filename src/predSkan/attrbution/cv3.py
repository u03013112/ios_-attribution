# 尝试对目前所有iOS首日付费用户进行cv重排列，找到效果更好的cv map
# 穷举数量太大，不适合，所以考虑换一种算法
# 尝试在范围内进行插入测试，每插入一次进行一次测试，直到插入到足够多的点
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename
from src.predSkan.tools.ai import purgeRetCsv
# 从mc找到最近半年的iOS用户首24小时付费金额
# 格式 install_date，customer_user_id，r1usd
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
            and day >= 20220701
            and day <= 20230201
        group by
            install_date,
            customer_user_id
    '''

    df = execSql(sql)
    return df

# 按照指定cv map进行计算后获得准确度，暂定按天算Mape
# df 是需要测试数据，map是映射区间
# map 格式：df格式，里面至少要包括min_event_revenue和max_event_revenue，行号就是cv值
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
        count = df.loc[df.cv==i,'count']
        df.loc[df.cv==i,'cv_usd'] = count * avg
    # print(df)
    # 计算mape
    df = df.groupby('install_date',as_index=False).agg({'r1usd':'sum','cv_usd':'sum'})
    df.loc[df.r1usd >= df.cv_usd,'mape']=(df.r1usd - df.cv_usd)/df.r1usd
    df.loc[df.r1usd < df.cv_usd,'mape']=(df.cv_usd - df.r1usd)/df.r1usd
    # print(df)
    df.to_csv(getFilename('iosCv_user_cv2usd'))

    return df

# 记录报告
def report(df,mapPath,message):
    mape = df.mape.mean()
    print('mape:',mape)
    path = '/src/data/doc/cv'
    os.makedirs(path,exist_ok=True)

    logFilename = os.path.join(path,'log2.csv')
    if os.path.exists(logFilename):
        logDf = pd.read_csv(logFilename)
    else:
        logDf = pd.DataFrame(data = {
            'map':[],
            'mape':[],
            'message':[]
        })
    logData = {
        'map':[mapPath],
        'mape':[mape],
        'message':[message]
    }

    logDf = logDf.append(pd.DataFrame(data=logData))
    logDf.to_csv(logFilename)
    purgeRetCsv(logFilename)

# 加速版本
def report2(df,mapPath,message):
    mape = df.mape.mean()
    print('mape:',mape)
    path = '/src/data/doc/cv'
    logFilename = os.path.join(path,'log3.csv')
    with open(logFilename,'a') as f:
        f.write('%s,%f,%s\n'%(mapPath,mape,message))

import copy

# 插入算法
def insert(df,level=32):
    
    levels = []

    # 收末档位固定
    for _ in range(level-2):
        # 尝试进行所有可用cv的尝试
        minMape = 10000
        needAppendLevel = -1

        for v in range(1,216):
            if v in levels:
                continue
            levelsTmp = copy.deepcopy(levels)
            levelsTmp.append(v)
            levelsTmp.sort()
            
            # 根据levels制作map
            mapData = {
                'cv':[0],
                'min_event_revenue':[-1],
                'max_event_revenue':[0]
            }

            for i in range(len(levelsTmp)):
                mapData['cv'].append(len(mapData['cv']))
                mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
                mapData['max_event_revenue'].append(levelsTmp[i])

            # 最后将最后一个固定档位插入
            mapData['cv'].append(len(mapData['cv']))
            mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
            mapData['max_event_revenue'].append(216)

            # print(mapData)

            mapeDf = mapeFunc(df, pd.DataFrame(data=mapData))
            mape = mapeDf.mape.mean()

            print(levelsTmp,':',mape)

            if mape < minMape:
                minMape = mape
                needAppendLevel = v

        levels.append(needAppendLevel)
        levels.sort()
        mapName = '0'
        for l in levels:
            mapName += '_%d'%l
        mapName += '_216'
        path = '/src/data/doc/cv'
        logFilename = os.path.join(path,'log4.csv')
        with open(logFilename,'a') as f:
            f.write('%s,%f,%s\n'%(mapName,minMape,'avg'))


    

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getDataFromMC()
        df.to_csv(getFilename('iosCv20220701_20230201'))

    df = pd.read_csv(getFilename('iosCv20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    
    # insert(df)

    df = df.sort_values(['install_date','r1usd'])
    df.to_csv(getFilename('iosCv_user_sort'))
    levels = [4,10,20,42,60,105,136,204]

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
    mapData['max_event_revenue'].append(216)

    mapDf = pd.DataFrame(data=mapData)
    mapDf.to_csv(getFilename('iosCv_map'))

    mapeDf = mapeFunc(df, mapDf)

    mape = mapeDf.mape.mean()
    print(mape)
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

from sklearn.metrics import r2_score
# 这个要在mapeFunc之后调用
def r2Func(df):
    return r2_score(df.r1usd,df.cv_usd)

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
    
    levels = [4,20]

    # 收末档位固定
    for _ in range(level-2):
        # 尝试进行所有可用cv的尝试
        minMape = 10000
        maxR2 = -100000
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
            r2 = r2Func(mapeDf)

            print(levelsTmp,'Mape:',mape,'R2:',r2)

            # 不再依据mape选方案，而是通过R2

            # if mape < minMape:
            #     minMape = mape
            #     needAppendLevel = v

            if r2 > maxR2:
                maxR2 = r2
                minMape = mape
                needAppendLevel = v

        levels.append(needAppendLevel)
        levels.sort()
        mapName = '0'
        for l in levels:
            mapName += '_%d'%l
        mapName += '_216'
        path = '/src/data/doc/cv'
        logFilename = os.path.join(path,'log5.csv')

        if os.path.exists(logFilename) == False:
            with open(logFilename,'w') as f:
                f.write('name,mape,r2,message\n')
        with open(logFilename,'a') as f:
            f.write('%s,%f,%f,%s\n'%(mapName,minMape,maxR2,'avg'))


    

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
    # levels = [
    #     1.6969,3.5726,6.0103,8.8024,12.3524,16.7171,21.701,27.2172,33.2045,40.5426,49.1994,58.9601,70.2909,83.6891,97.1486,
    #     114.7633,132.0923,153.4342,184.3833,211.8377,242.5758,271.612,303.1287,335.7322,382.8155,440.3342,526.2914,783.6587,973.4348,1152.2711
    # ]

    # levels = [
    #     1,2,3,4,5,6,7,8,9,10,11,12,13,15,17,19,24,29,34,44,55,65,75,85,95,115,135,155,175,195,215
    # ]


    # 31
    levels = [
        1.6448,3.2418,5.3475,7.7988,10.7114,14.465,18.992,24.2942,31.0778,40.2628,51.5247,61.2463,70.1597,82.5565,97.3848,111.5657,125.2677,142.6695,161.6619,184.4217,204.8459,239.7421,264.9677,306.9067,355.154,405.6538,458.3643,512.6867,817.0817,1819.0253,2544.7372
    ]

    # 20
    # levels = [
    #     1.5243,4.8268,7.3777,11.0212,17.0397,25.765,32.9272,43.8799,53.0345,66.4053,82.305,102.7762,128.0474,155.7262,196.9415,294.3326,414.014,686.8542,1455.6964,2544.7372
    # ]

    # 15
    levels = [
        2.6208,2.8701,11.8094,22.0189,33.9542,48.5706,64.8133,86.3302,119.2499,141.4811,187.3731,232.9296,277.9331,348.4363,420.0994
    ]

    # 7
    levels = [
        2.4707,5.2468,18.9076,47.7314,94.9377,193.1167,234.78
    ]

    # 4
    levels = [
        2.2711,11.4428,35.9047,134.9
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

    # # 最后将最后一个固定档位插入
    # mapData['cv'].append(len(mapData['cv']))
    # mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
    # mapData['max_event_revenue'].append(235)

    mapDf = pd.DataFrame(data=mapData)
    # mapDf.to_csv(getFilename('iosCv_map'))

    mapeDf = mapeFunc(df, mapDf)

    mape = mapeDf.mape.mean()

    r2 = r2Func(mapeDf)

    print(mapDf)
    print('mape:',mape)
    print('r2:',r2)
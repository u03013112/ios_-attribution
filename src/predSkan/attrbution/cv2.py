# 尝试对目前所有iOS首日付费用户进行cv重排列，找到效果更好的cv map

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
def mape(df,map):
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
    # print(df)
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
# 递归函数，用于穷举所有map
def dangerousFunc(df,mapData,level,filename,min,max):
    # print(mapData,level,filename,min,max)
    nowCv = len(mapData['cv'])
    # 还剩leftCv个档位没有分配
    leftCv = level - nowCv
    # print(nowCv,leftCv)
    if leftCv < 0:
        # 存csv
        # fullPath = '/src/data/doc/cv/map/%s.csv'%filename
        # print(mapDataCopy)
        print('finish',filename)
        # pd.DataFrame(data=mapData).to_csv(fullPath)
        mapeDf = mape(df, pd.DataFrame(data=mapData))
        report2(mapeDf,filename,'avg')
        
    else:
        # 目前到第nowCv个档位了
        for v in range(min+2,max-leftCv+1,2):
            mapDataCopy = copy.deepcopy(mapData)
            
            filenameNew = filename+'_%d'%(v)
            mapDataCopy['cv'].append(nowCv)
            mapDataCopy['min_event_revenue'].append(min)
            mapDataCopy['max_event_revenue'].append(v)
            dangerousFunc(df,mapDataCopy,level,filenameNew,v,max)
            
    

# 正式流程，穷举32个档位
def qj(df,level=31):
    # 第0档位固定是0
    mapData = {
        'cv':[0,1],
        'min_event_revenue':[-1,0],
        'max_event_revenue':[0,1]
    }
    # 穷举
    # 这里可能用递归会容易一些
    # 216是原有map的上限
    max = 216
    min = 0
    dangerousFunc(df,mapData,level,'map_0_1',min,max)

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getDataFromMC()
        df.to_csv(getFilename('iosCv20220701_20230201'))

    df = pd.read_csv(getFilename('iosCv20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    # afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
    # df = mape(df, afCvMapDataFrame)
    # report(df,'/src/afCvMap.csv','avg')
    qj(df)
    
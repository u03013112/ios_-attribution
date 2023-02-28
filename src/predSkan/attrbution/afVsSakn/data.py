# AF 数据与 SKAN 数据对比
# 与之前的区别，
# 重新推算安装时间区间，并将安装时间按日然日计算几率（精确到小时）
# 然后按照安装日几率将 count，r1usd，r7usd拆开
# 先把上述值算出来，然后再进行准确度校验

import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame

# 用idfv做主键，获得iOS AF数据
def getIOSDataFromAF():
    sql = '''
        select
            idfv,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                else 0
                end
            ) as r1usd,
            media_source as media,
            idfa
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and zone = 0
            and day >= 20221001
            and day <= 20230205
            and install_time >= '2022-10-01'
            and install_time < '2023-02-01'
        group by
            install_date,
            idfv,
            media,
            idfa
        ;
    '''
    df = execSql(sql)
    return df

# SKAN
def getDataFromSkan():
    sql = '''
        select
            install_date,
            media_source as media,
            skad_conversion_value as cv,
            timestamp,
            ad_network_timestamp
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = 'id1479198816'
            and day >= 20221001 and day < 20230205
            and install_date >= '2022-10-01'
            and install_date < '2023-02-01'
            and event_name in ('af_skad_install','af_skad_redownload')
        ;
    '''
    df = execSql(sql)
    return df

# 将首日付费按照目前的地图映射到CV
def addCV(userDf,mapDf = None):
    userDf.loc[:,'cv'] = 0
    if mapDf is None:
        map = afCvMapDataFrame
    else:
        map = mapDf
    for i in range(len(map)):
        min_event_revenue = map.min_event_revenue[i]
        max_event_revenue = map.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        userDf.loc[
            (userDf.r1usd > min_event_revenue) & (userDf.r1usd <= max_event_revenue),
            'cv'
        ] = i
    userDf.loc[
        (userDf.r1usd > max_event_revenue),
        'cv'
    ] = len(map)-1

    return userDf

# 
def getAndroidDataFromAF():
    sql = '''
        select
            appsflyer_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                else 0
                end
            ) as r1usd,
            sum(
                case
                when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
                else 0
                end
            ) as r7usd,
            install_timestamp,
            media_source as media
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp'
            and zone = 0
            and day >= 20221001
            and day <= 20230205
            and install_time >= '2022-10-01'
            and install_time < '2023-02-01'
        group by
            install_date,
            appsflyer_id,
            install_timestamp,
            media
        ;
    '''
    df = execSql(sql)
    return df

# 为了用 android 数据添加postback 时间戳
# 添加 postback时间戳的字段
# 为了减少计算量，按用户随机之后，汇总成小时
def addSkanHour(df):
    # 按照苹果文档，skan推迟时间 
    # 转化为0的用户 为 24~48小时
    # 转化大于0的用户 为 24~72小时，并且可能超过48小时的可能性很大（因为大概率不会激活后立即付款）

    df.loc[df.cv == 0,'rand_delay'] = np.random.randint(24*3600,48*3600,len(df.loc[df.cv == 0]))
    df.loc[df.cv > 0,'rand_delay'] = np.random.randint(24*3600,72*3600,len(df.loc[df.cv > 0]))

    # df.loc[:,'install_timestamp'] = pd.to_datetime(df['install_hour'],format='%Y-%m-%d %H').astype(int)/ 10**9

    # print(df)
    # skan的时间戳直接就叫做 timestamp，和真的skan报告保持一致
    df.loc[:,'timestamp'] = df['install_timestamp'] + df['rand_delay']
    
    df.loc[:,'skan_hour'] = pd.to_datetime(df['timestamp'],unit='s').dt.strftime('%Y-%m-%d %H')
    return df

# 添加安装日期，按照可能区间分概率
# 输入是getAndroidDataFromAF的返回值，最原始数据
def addInstallDateByJVStep1(df0):
    df = df0.copy(deep=True)
    df = addCV(df)
    df = addMediaGroup(df)
    df = addSkanHour(df)
    
    df.loc[:,'count'] = 1
    # 按小时进行汇总
    df = df.groupby(by=['media_group','skan_hour','cv'],as_index=False).agg({
        'count':'sum',
        'r1usd':'sum',
        'r7usd':'sum',
    })
    df.loc[:,'timestamp'] = pd.to_datetime(df['skan_hour'],format='%Y-%m-%d %H').astype(int)/ 10**9
    # 按照苹果文档，安装日期区间推算
    # 未付费用户 介于 -48~-24小时
    df.loc[df.cv == 0,'install_timestamp_min'] = df['timestamp'] - 48*3600
    df.loc[:,'install_timestamp_max'] = df['timestamp'] - 24*3600
    # 付费用户 介于 -72~-24小时
    df.loc[df.cv > 0,'install_timestamp_min'] = df['timestamp'] - 72*3600

    # 计算此用户在每个可能安装日的几率
    # 计算方式是根据这个用户在此安装日的可能时间长度与24小时的比值，0~1之间

    # 将时间戳变成日期，这个不会写，从网上抄一份，不知道效率如何
    df['install_date_min'] = pd.to_datetime(df['install_timestamp_min'],unit='s').dt.strftime('%Y-%m-%d %H')
    df['install_date_max'] = pd.to_datetime(df['install_timestamp_max'],unit='s').dt.strftime('%Y-%m-%d %H')

    df.loc[:,'h0'] = 24 - df['install_timestamp_min'] % (24 * 3600)/3600
    df.loc[:,'h1'] = df['install_timestamp_max']% (24 * 3600)/3600
    df.loc[:,'h2'] = 0
    df.loc[df.cv > 0,'h2'] = 24
    return df


import datetime
# 拆分安装日期可能性
# 逐行处理，这个步骤会很慢
# 将count，r1usd 和 r7usd 按照概率拆分
def addInstallDateByJVStep2(df):
    spliteRetDf = pd.DataFrame()

    df.loc[:,'install_date_min'] = pd.to_datetime(df['install_date_min'],format='%Y-%m-%d %H').dt.strftime('%Y-%m-%d')
    df.loc[:,'install_date_max'] = pd.to_datetime(df['install_date_max'],format='%Y-%m-%d %H').dt.strftime('%Y-%m-%d')

    for _,row in df.iterrows():

        install_date_min = row['install_date_min']
        install_date_max = row['install_date_max']
        
        media_group = row['media_group']
        cv = row['cv']
        count = row['count']
        r1usd = row['r1usd']
        r7usd = row['r7usd']
        h0 = row['h0']
        h1 = row['h1']
        h2 = row['h2']
        hSum = row['h0'] + row['h1'] + row['h2'] 

        tmpData = {}

        # print(install_date_min,install_date_max)
        installDateMin = datetime.datetime.strptime(install_date_min,'%Y-%m-%d')
        installDateMax = datetime.datetime.strptime(install_date_max,'%Y-%m-%d')
        if (installDateMax - installDateMin).days == 1:
            # 间隔只一天
            tmpData['media_group'] = [media_group,media_group]
            tmpData['cv'] = [cv,cv]
            tmpData['install_date'] = [install_date_min,install_date_max]
            tmpData['count'] = [count * (h0/hSum),count * (h1/hSum)]
            tmpData['r1usd'] = [r1usd * (h0/hSum),r1usd * (h1/hSum)]
            tmpData['r7usd'] = [r7usd * (h0/hSum),r7usd * (h1/hSum)]
        else:
            # 间隔两天
            tmpData['media_group'] = [media_group,media_group,media_group]
            tmpData['cv'] = [cv,cv,cv]
            dayStr = (installDateMin + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            tmpData['install_date'] = [install_date_min,dayStr,install_date_max]
            tmpData['count'] = [count * (h0/hSum),count * (h2/hSum),count * (h1/hSum)]
            tmpData['r1usd'] = [r1usd * (h0/hSum),r1usd * (h2/hSum),r1usd * (h1/hSum)]
            tmpData['r7usd'] = [r7usd * (h0/hSum),r7usd * (h2/hSum),r7usd * (h1/hSum)]
        
        # print(tmpData)
        tmpDf = pd.DataFrame(tmpData)
        spliteRetDf = spliteRetDf.append(tmpDf,ignore_index = True)

        # 拆开之后直接汇总
        spliteRetDf = spliteRetDf.groupby(by=['media_group','install_date','cv'],as_index=False).agg({
            'count':'sum',
            'r1usd':'sum',
            'r7usd':'sum',
        })
    return spliteRetDf
# 暂时就先关注3个主要媒体
mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads']},
    {'name':'unknown','codeList':[]}
]
def addMediaGroup(df):
    # 所有不属于mediaList的都算是unknown，和自然量一起处理
    df.insert(df.shape[1],'media_group','unknown')
    for media in mediaList:
        name = media['name']
        for code in media['codeList']:
            df.loc[df.media == code,'media_group'] = name
    return df

# 准确度校验方案一：安卓校验
# 将安卓用户按照SKAN的方式进行模拟，计算（随机）出SKAN上报时间
# 再按照上述方法用上报时间将 count，r1usd，r7usd拆开
# 计算r1usd和r7usd的差异
def androidCheck():
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df0 = pd.read_csv(getFilename('androidUserData_20221001_20230201'))
        df1 = addInstallDateByJVStep1(df0)
        df1.to_csv(getFilename('androidUserDataHours1'))
        # df = pd.read_csv(getFilename('androidUserDataHours1'))
        df2 = addInstallDateByJVStep2(df1)
        df2.to_csv(getFilename('androidUserDataHours2'))
        

        # 给原始数据汇总
        df0 = addCV(df0)
        df0 = addMediaGroup(df0)
        df0.loc[:,'count'] = 1
        df0 = df0.groupby(by=['media_group','install_date','cv'],as_index=False).agg({
            'count':'sum',
            'r1usd':'sum',
            'r7usd':'sum',
        })
        df0.to_csv(getFilename('androidUserDataHours0'))

    df0 = pd.read_csv(getFilename('androidUserDataHours0'))
    df2 = pd.read_csv(getFilename('androidUserDataHours2'))
    retDf = androidCheckFinal(df0,df2)
    retDf.to_csv(getFilename('androidUserDataHours3'))
            
    
# 和准确数据做对比
# 输入应该是原始数据和算法生成数据，两个都应该是汇总完成的数据
def androidCheckFinal(df0,df4):
    
    df0 = df0.groupby(by=['media_group','install_date'],as_index=False).agg({
    # df0 = df0.groupby(by=['install_date'],as_index=False).agg({
        'count':'sum',
        'r1usd':'sum',
        'r7usd':'sum',
    })

    df4 = df4.groupby(by=['media_group','install_date'],as_index=False).agg({
    # df4 = df4.groupby(by=['install_date'],as_index=False).agg({
        'count':'sum',
        'r1usd':'sum',
        'r7usd':'sum',
    })

    mergeDf = df0.merge(df4,how = 'left',on = ['media_group','install_date'],suffixes=('_0','_4'))
    mergeDf = mergeDf.sort_values(by = ['install_date','media_group'],ignore_index= True)

    mergeDf.loc[:,'mape1'] = (mergeDf['r1usd_0'] - mergeDf['r1usd_4'])/mergeDf['r1usd_0']
    mergeDf.loc[mergeDf.mape1 < 0,'mape1'] *= -1

    mergeDf.loc[:,'mape7'] = (mergeDf['r7usd_0'] - mergeDf['r7usd_4'])/mergeDf['r7usd_0']
    mergeDf.loc[mergeDf.mape7 < 0,'mape7'] *= -1

    mergeDf.replace([np.inf, -np.inf], 0, inplace=True)

    for media in mediaList:
        name = media['name']
        df = mergeDf.loc[mergeDf.media_group == name]
        
        print(name,'mape1',df['mape1'].mean())
        print(name,'mape7',df['mape7'].mean())

    # mergeDf.to_csv(getFilename('androidUserDataHours5_20221001_20230201'))
    return mergeDf


# 对比实验：用AF的方案进行安装日期的计算，然后重复上述验算，
# 计算r1usd和r7usd的差异

# 准确度校验方案二：iOS真是数据校验
# 每天 AF CV - SKAN CV 一定大于0，小于0的部分认为是差异值
# 计算差异用户数差异金额。这个只能算差异，没法校验

# 对比实验：用AF的方案进行对比

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    # else:
        # df = getAndroidDataFromAF()
        # df.to_csv(getFilename('androidUserData_20221001_20230201'))

    androidCheck()
    
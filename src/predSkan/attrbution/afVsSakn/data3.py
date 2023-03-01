# 进行多天的汇总，
# 既然一天不能对齐，尝试多天，看看是否可以对齐

import os
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

# 添加一个模拟的skan报告时间，并记录在timestamp字段
# 里面有随机的情况
def addTimestamp(df0):
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
    return df

# 添加安装日期，按照可能区间分概率
# 输入是getAndroidDataFromAF的返回值，最原始数据
def addInstallDateByJVStep1(df):
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
# 这里面是一个重新拆表
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
def androidCheck(dfRandomSum,df0Sum,i):
    df = dfRandomSum.copy(deep = True)
    df1 = addInstallDateByJVStep1(df)
    # df1.to_csv(getFilename('androidUserDataHours1'))
    
    df2 = addInstallDateByJVStep2(df1)
    # df2.to_csv(getFilename('androidUserDataHours2'))
    
    retDf = androidCheckFinal(df0Sum,df2,'Self',i)
    # retDf.to_csv(getFilename('androidUserDataHours3'))
            
    
# 和准确数据做对比
# 输入应该是原始数据和算法生成数据，两个都应该是汇总完成的数据
def androidCheckFinal(df0,df4,message,i=0):
    dateDf = pd.DataFrame({'install_date':df0['install_date'].unique()})
    dateDf = dateDf.sort_values(by = ['install_date'],ignore_index=True)
    dateDf.loc[:,'i0'] = np.arange(len(dateDf))
    

    for d in (1,3,7,14,30):
        dateDfCopy = dateDf.copy(deep = True)
        dateDfCopy.loc[:,'i1'] = dateDfCopy['i0']%(d)
        dateDfCopy.loc[:,'install_date_group'] = pd.to_datetime(
            (pd.to_datetime(dateDfCopy['install_date'],format='%Y-%m-%d').astype(int)/ 10**9 - dateDfCopy['i1']*24*3600),
            unit='s'
        ).dt.strftime('%Y-%m-%d')

        df0Group = df0.merge(dateDfCopy,how='left',on=['install_date'],suffixes = ('',''))
        df0Group2 = df0Group.groupby(by=['media_group','install_date_group'],as_index=False).agg({
            'count':'sum',
            'r1usd':'sum',
            'r7usd':'sum',
        })
        
        df4Group = df4.merge(dateDfCopy,how='left',on=['install_date'],suffixes = ('',''))
        df4Group2 = df4Group.groupby(by=['media_group','install_date_group'],as_index=False).agg({
            'count':'sum',
            'r1usd':'sum',
            'r7usd':'sum',
        })
        
        mergeDf = df0Group2.merge(df4Group2,how = 'left',on = ['media_group','install_date_group'],suffixes=('_0','_4'))
        mergeDf = mergeDf.sort_values(by = ['media_group','install_date_group'],ignore_index= True)

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

        print('total mape1',mergeDf['mape1'].mean())
        print('total mape7',mergeDf['mape7'].mean())
        # mergeDf.to_csv(getFilename('androidUserDataHours5_20221001_20230201'))

        # 记录日志
        report(mergeDf,message,i,d)
    return mergeDf

import matplotlib.pyplot as plt
def report(mergeDf,message,i = 0,d = 1):
    logFile = '/src/data/doc/ds/afVsSkanDays.csv'

    if os.path.exists(logFile) == False:
        with open(logFile, 'w') as f:
            f.write('media,mape1,mape7,message,days,i\n')
    
    
    with open(logFile, 'a') as f:
        for media in mediaList:
            name = media['name']
            df = mergeDf.loc[mergeDf.media_group == name]
            
            line = '%s,%.3f,%.3f,%s,%d,%d\n'%(name,df['mape1'].mean(),df['mape7'].mean(),message,d,i)
            f.write(line)    

        line = '%s,%.3f,%.3f,%s,%d,%d\n'%('total',mergeDf['mape1'].mean(),mergeDf['mape7'].mean(),message,d,i)
        f.write(line)

    
    # mergeDf.to_csv('/src/data/tmp.csv')
    # mergeDf = pd.read_csv('/src/data/tmp.csv')
    # print(mergeDf)

    # mergeDf.set_index(["install_date"], inplace=True)

    # for media in mediaList:
    #     name = media['name']

    #     df = mergeDf.loc[mergeDf.media_group == name]

    #     plt.title("r1usd %s"%message)
    #     df['r1usd_0'].plot(label = 'real')
    #     df['r1usd_4'].plot(label = 'pred')
    #     plt.xticks(rotation=45)
    #     plt.legend()
    #     plt.tight_layout()
    #     filename = os.path.join('/src/data/doc/ds/', '%s_%s_%d_%d_r1usd.png'%(name,message,d,i))
    #     plt.savefig(filename)
    #     print('save pic',filename)
    #     plt.clf()

    #     plt.title("r7usd %s"%message)
    #     df['r7usd_0'].plot(label = 'real')
    #     df['r7usd_4'].plot(label = 'pred')
    #     plt.xticks(rotation=45)
    #     plt.legend()
    #     plt.tight_layout()
    #     filename = os.path.join('/src/data/doc/ds/', '%s_%s_%d_%d_r7usd.png'%(name,message,d,i))
    #     plt.savefig(filename)
    #     print('save pic',filename)
    #     plt.clf()

# 对比实验：用AF的方案进行安装日期的计算，然后重复上述验算，
# 计算r1usd和r7usd的差异

def androidCheck2(dfRandomSum,df0Sum,i):
    df = dfRandomSum.copy(deep = True)
    # 按照AF规则
    # 激活日期是基于回传接收日期推算的，具体方法如下：回传接收日期 - 36小时 - [末次互动范围平均小时数]。默认[末次互动范围平均小时数]为12小时，但如果转化值为0，则末次互动范围平均小时数也为0。
    df.loc[df.cv == 0,'install_timestamp'] = df['timestamp'] - 36*3600
    df.loc[df.cv > 0,'install_timestamp'] = df['timestamp'] - 48*3600

    df['install_date'] = pd.to_datetime(df['install_timestamp'],unit='s').dt.strftime('%Y-%m-%d')

    df2 = df.groupby(by=['media_group','install_date','cv'],as_index=False).agg({
        'count':'sum',
        'r1usd':'sum',
        'r7usd':'sum',
    })

    retDf = androidCheckFinal(df0Sum,df2,'AF',i)
    # retDf.to_csv(getFilename('androidUserDataHours3AF'))






# 准确度校验方案二：iOS真是数据校验
# 每天 AF CV - SKAN CV 一定大于0，小于0的部分认为是差异值
# 计算差异用户数差异金额。这个只能算差异，没法校验

# 对比实验：用AF的方案进行对比

def test():
    df0Sum = pd.read_csv(getFilename('androidUserDataHours0'))
    df = pd.DataFrame({'install_date':df0Sum['install_date'].unique()})
    df.loc[:,'i0'] = np.arange(len(df))

    df.loc[:,'i1'] = df['i0']%3
    df.loc[:,'install_date_group'] = pd.to_datetime(
        (pd.to_datetime(df['install_date'],format='%Y-%m-%d').astype(int)/ 10**9 - df['i1']*24*3600),
        unit='s'
    ).dt.strftime('%Y-%m-%d')
    print(df)

if __name__ == '__main__':
    df0 = pd.read_csv(getFilename('androidUserData_20221001_20230201'))
    df0Sum = pd.read_csv(getFilename('androidUserDataHours0'))

    # df0 = df0.loc[df0.install_date < '2022-10-10']
    # df0Sum = df0Sum.loc[df0Sum.install_date < '2022-10-10']


    for i in range(10):
        dfRandomSum = addTimestamp(df0)
        
        androidCheck(dfRandomSum,df0Sum,i)
        androidCheck2(dfRandomSum,df0Sum,i)
    
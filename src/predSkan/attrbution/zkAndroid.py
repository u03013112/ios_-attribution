# 撞库实践，用安卓进行尝试
import copy
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame


# 暂时只看着3个媒体
mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    # 'unknown'
]

# 获得安卓用户信息
# afid + media + campaign + r1usd + r7usd
# 暂时可以忽略campaign，先尝试到分媒体就好。
def getDataFromAF():
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
            media
        ;
    '''
    df = execSql(sql)
    return df

# 随机的忽略掉80%的用户ID信息，这里改为给这些用户一个标记，标记为需要预测用户
# frac 是可归因的用户占比
def emuSKAN(df,frac = .2):
    userDf = copy.deepcopy(df)
    userDf.loc[:,'idfa'] = 0
    sampleDf = userDf.sample(frac = frac)
    userDf.loc[sampleDf.index,'idfa'] = 1
    return userDf

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

# 撞库尝试
def zk(userDf):
    # 先将用户按照安装日期、媒体和cv进行分组
    # 然后将没有idfa的用户逐一进行分配，将用户标记成不同的媒体成分，包括自然量
    userDf.loc[:,'count'] = 1

    for media in mediaList:
        userDf.loc[:,media] = 0


    # 只针对 idfa == 0 的用户
    notHaveIdfaUserDf = userDf.loc[userDf.idfa == 0]

    # 安装日 + cv 求和
    groupDf0 = notHaveIdfaUserDf.groupby(['install_date','cv'],as_index=False).agg({
        'count':'sum'
    })

    for media in mediaList:
        # 这里的统计是将拥有idfa的排除掉，iOS之后也要这样做。
        mediaUserDf = notHaveIdfaUserDf.loc[notHaveIdfaUserDf.media == media]

        # 安装日 + cv + 媒体 求和
        mediaGroupDf = mediaUserDf.groupby(['install_date','cv'],as_index=False).agg({
            'count':'sum'
        })

        groupDf0 = pd.merge(groupDf0,mediaGroupDf,how='left',on=['install_date','cv'],suffixes=('','_%s'%media))

    mergeDf0 = pd.merge(notHaveIdfaUserDf,groupDf0,how='left',on=['install_date','cv'],suffixes=('','_total'))
    

    # print(mergeDf0)

    # 将所有idfa == 0 的用户进行
    for media in mediaList:
        mergeDf0[media] = mergeDf0['count_%s'%(media)]/mergeDf0['count_total']
    return mergeDf0


# 将zk结论转化成更好处理的格式
# 原格式是 安装日期、媒体1金额、媒体2金额
# 改为 安装日期、媒体、金额
def zkRetDataSwitch(zkRetDf):
    # 将7日回收算出来，并填到媒体收入中去
    for media in mediaList:
        zkRetDf['r7usd_%s'%(media)] = zkRetDf['r7usd']*zkRetDf[media]

    # print(zkRetDf)
    # 将媒体7日回收进行汇总
    zkRetSumDf = zkRetDf.groupby(['install_date'],as_index=False).agg({
        # 'r7usd':'sum',
        'r7usd_googleadwords_int':'sum',
        'r7usd_Facebook Ads':'sum',
        'r7usd_bytedanceglobal_int':'sum',
    })
    zkRetSumDf = zkRetSumDf.rename(columns={
        'r7usd_googleadwords_int':'googleadwords_int',
        'r7usd_Facebook Ads':'Facebook Ads',
        'r7usd_bytedanceglobal_int':'bytedanceglobal_int'
    })
    # print(zkRetSumDf)

    # 列转行
    return pd.melt(zkRetSumDf,id_vars='install_date',var_name='media',value_name='r7usd')

# zkRetDf 只撞库的结论，df是全部的原始数据，里面包括了idfa == 1的值
# message 是扩展记录的信息，比如idfa比例+随机次数，暂时想到只有这
def check(zkRetDf,df):
    zkDf = zkRetDataSwitch(zkRetDf)
    # print(zkDf.loc[(zkDf.install_date == '2022-10-01') & (zkDf.media == 'Facebook Ads')])
    # 再加上idfa == 1的数据，算出预测媒体7日回收
    idfaDf = df.loc[
        (df.idfa == 1) 
        & (
        df.media.isin(mediaList)
        #     (df.media == 'googleadwords_int') |
        #     (df.media == 'Facebook Ads') |
        #     (df.media == 'bytedanceglobal_int') 
        )
    ]
    idfaSumDf = idfaDf.groupby(['install_date','media'],as_index=False).agg({
        'r7usd':'sum'
    })

    sumDf = zkDf.append(idfaSumDf,ignore_index=True)

    # 与真实回收进行比对，计算
    sumDf = sumDf.groupby(['install_date','media'],as_index=False).agg({
        'r7usd':'sum'
    })

    sumRealDf = df.loc[df.media.isin(mediaList)]
    sumRealDf = sumRealDf.groupby(['install_date','media'],as_index=False).agg({
        'r7usd':'sum'
    })

    mergeDf = pd.merge(sumRealDf,sumDf,on = ['install_date','media'],suffixes=('_real','_predict'))
    
    mergeDf.loc[:,'mape'] = 0
    mergeDf['mape'] = (mergeDf['r7usd_real'] - mergeDf['r7usd_predict'])/mergeDf['r7usd_real']
    mergeDf.loc[mergeDf.mape <0,'mape'] *= -1
    
    return mergeDf

# 主要流程
def main():
    df = pd.read_csv(getFilename('androidUserData20221001_20230201'))

    # 补0，防止之后计算的时候警告信息
    df.loc[pd.isna(df.r1usd),'r1usd'] = 0
    df.loc[pd.isna(df.r7usd),'r7usd'] = 0

    df = addCV(df)

    for idfa in (0.2,0.3):
        # 由于划定idfa是随机的，所以这里多尝试几次，之后是取平均还是均中位数再看数据吧
        for i in range(20):
            df = emuSKAN(df,idfa)

            zkRetDf = zk(df)
            zkRetDf.to_csv(getFilename('zkRet'))
            
            zkRetDf = pd.read_csv(getFilename('zkRet'))

            ret = check(zkRetDf,df)

            ret.to_csv(getFilename('zkr%f_%d'%(idfa,i)))

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getDataFromAF()
        df.to_csv(getFilename('androidUserData20221001_20230201'))
    
    # df = pd.read_csv(getFilename('androidUserData20221001_20230201'))

    # # 补0，防止之后计算的时候警告信息
    # df.loc[pd.isna(df.r1usd),'r1usd'] = 0
    # df.loc[pd.isna(df.r7usd),'r7usd'] = 0

    # df = addCV(df)
    # df = emuSKAN(df)


    # zkRetDf = zk(df)
    # zkRetDf.to_csv(getFilename('zkRet'))
    
    # zkRetDf = pd.read_csv(getFilename('zkRet'))

    # ret = check(zkRetDf,df)

    # ret.to_csv(getFilename('zkr01'))
    main()
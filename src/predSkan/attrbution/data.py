# 对数
# 对数大致分两个部分
# AF event 与 skan ，主要对这个部分。
# 数数 与 AF event ，这两个主要需要针对uid进行比对，主要差异是AF按照设备算新用户，数数按照uid算。

# 用2022年10月 到 2023年1月 3个月按照天和月分别做统计，能够匹配的比率最好超过99%，付费用户超过99%
# 用户数据，其中包括 注册时间（UTC-0），uid，首日（24小时）付费金额，7日（168小时）付费金额，广告信息（暂时精确到媒体）

# 其他信息比如：国家暂时不获取，大概率匹配不上。

import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame


# 数数
def getDataFromSS():
    data = {}
    return data

# AF event 24小时内的付费数据，直接按照目前的map转成cv
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
            appsflyer_id,
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
            count(*) as count
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = 'id1479198816'
            and day >= 20221001 and day < 20230205
            and install_date >= '2022-10-01'
            and install_date < '2023-02-01'
        group by
            install_date,
            media,
            cv
    ;
    '''
    df = execSql(sql)
    return df

# 对数据做基础处理
def dataStep3(dataDf2):
    # 每天补充满64组数据，没有的补0
    install_date_list = dataDf2['install_date'].unique()
    for install_date in install_date_list:
        df = dataDf2.loc[(dataDf2.install_date == install_date)]
        for i in range(64):
            if df.loc[df.cv == i,'count'].sum() == 0:
                dataDf2 = dataDf2.append(pd.DataFrame(data={
                    'install_date':[install_date],
                    'count':[0],
                    # 'sumr1usd':[0],
                    # 'sumr7usd':[0],
                    'cv':[i]
                }),ignore_index=True)
                print('补充：',install_date,i)
    return dataDf2


# AF数据应该可以完全包括SKAN数据
# 粒度限定在一天
# 暂时先不分媒体，应该是每一天 每一种CV中都是AF >= SKAN
# 需要制定一个标准来计算偏差程度，简单的分两个指标
# 第一个是人数偏差程度，(SKAN人数 - AF人数)/AF人数
# 第二个是CV偏差程度，(SKAN CV - AF CV)/AF CV
def afMustIncludeSkan(afDf,skanDf):
    # 给AF添加CV
    afDf.loc[:,'cv'] = 0
    map = afCvMapDataFrame
    for i in range(len(map)):
        min_event_revenue = map.min_event_revenue[i]
        max_event_revenue = map.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        afDf.loc[
            (afDf.r1usd > min_event_revenue) & (afDf.r1usd <= max_event_revenue),
            'cv'
        ] = i
    afDf.loc[
        (afDf.r1usd > max_event_revenue),
        'cv'
    ] = len(map)-1

    afDf.to_csv(getFilename('afSkanMergeData20221001_20230_01'))
    # 按天汇总
    afDf.loc[:,'count'] = 1
    afDf = afDf.groupby(['install_date','cv'],as_index=False).agg({
        'count':'sum'
    }).sort_values(by = ['install_date','cv'])

    skanDf = skanDf.groupby(['install_date','cv'],as_index=False).agg({
        'count':'sum'
    }).sort_values(by = ['install_date','cv'])

    afDf = dataStep3(afDf)
    skanDf = dataStep3(skanDf)

    afDf = afDf.sort_values(by = ['install_date','cv'])
    
    mergeDf = pd.merge(afDf,skanDf,on = ['install_date','cv'],suffixes = ('_af','_skan'))
    print(mergeDf)
    return mergeDf

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        # df = getDataFromSkan()
        # df.to_csv(getFilename('skanData20221001_20230201'))

        df = getDataFromAF()
        df.to_csv(getFilename('afData20221001_20230201'))

    afDf = pd.read_csv(getFilename('afData20221001_20230201'))
    skanDf = pd.read_csv(getFilename('skanData20221001_20230201'))

    mergeDf = afMustIncludeSkan(afDf, skanDf)
    mergeDf.to_csv(getFilename('afSkanMergeData20221001_20230'))
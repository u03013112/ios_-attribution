# AF event 与 skan 对数

# 用2022年10月 到 2023年1月 3个月按照天和月分别做统计，能够匹配的比率最好超过99%，付费用户超过99%

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame

# AF event 24小时内的付费数据，直接按照目前的map转成cv
# 用idfv来做唯一键
# 
def getDataFromAF():
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
            count(*) as count
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = 'id1479198816'
            and day >= 20221001 and day < 20230205
            and install_date >= '2022-10-01'
            and install_date < '2023-02-01'
            and event_name in ('af_skad_install','af_skad_redownload')
        group by
            install_date,
            media,
            cv
        ;
    '''
    df = execSql(sql)
    return df

def getDataFromSkanInstallOnly():
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
            and event_name in ('af_skad_install')
        group by
            install_date,
            media,
            cv
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


# 怎样才算是对数能对得上
# AF数据要完全包括SKAN数据，每个CV都应该是
# 评判标准，不能覆盖的用户数/所有用户
# 评判标准2，不能覆盖的用户数/付费用户
def check1(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    afDf.loc[:,'count'] = 1
    afGroupbyDf = afDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    # afGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(afGroupbyDf,skanGroupbyDf,how='outer',on=['install_date','cv'],suffixes=('_af','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_date','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['af_skan'] = mergeDf['count_af'] - mergeDf['count_skan']

    # 计算评判标准
    mergeDf.loc[mergeDf.af_skan > 0,'af_skan'] = 0
    mergeDf.loc[mergeDf.af_skan < 0,'af_skan'] *= -1

    mergeDf.to_csv(getFilename('afVsSkanCheck01'))
    mergeDf = pd.read_csv(getFilename('afVsSkanCheck01'))

    errorCount = mergeDf['af_skan'].sum()
    totalCount = mergeDf['count_skan'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    # print(mergeDf)
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['count_skan'].sum()
    error2 = errorCount/totalCount2

    return error,error2

# SKAN数据应该包含所有IDFA数据
# 标准与上面一致
def check2(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    idfaDf = afDf.loc[pd.isna(afDf.idfa) == False]

    idfaDf.loc[:,'count'] = 1
    idfaGroupbyDf = idfaDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    # idfaGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(idfaGroupbyDf,skanGroupbyDf,how='outer',on=['install_date','cv'],suffixes=('_idfa','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_date','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['skan_idfa'] = mergeDf['count_skan'] - mergeDf['count_idfa']

    # 计算评判标准
    mergeDf.loc[mergeDf.skan_idfa > 0,'skan_idfa'] = 0
    mergeDf.loc[mergeDf.skan_idfa < 0,'skan_idfa'] *= -1

    mergeDf.to_csv(getFilename('afVsSkanCheck02'))
    mergeDf = pd.read_csv(getFilename('afVsSkanCheck02'))

    errorCount = mergeDf['skan_idfa'].sum()
    totalCount = mergeDf['count_skan'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    # print(mergeDf)
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['count_skan'].sum()
    error2 = errorCount/totalCount2

    return error,error2

# 按月做检查1
def checkByMonth1(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    afDf['install_month'] = afDf['install_date'].str[0:7]
    skanDf['install_month'] = skanDf['install_date'].str[0:7]

    afDf.loc[:,'count'] = 1
    afGroupbyDf = afDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    # afGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(afGroupbyDf,skanGroupbyDf,how='outer',on=['install_month','cv'],suffixes=('_af','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_month','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['af_skan'] = mergeDf['count_af'] - mergeDf['count_skan']

    # 计算评判标准
    mergeDf.loc[mergeDf.af_skan > 0,'af_skan'] = 0
    mergeDf.loc[mergeDf.af_skan < 0,'af_skan'] *= -1

    mergeDf.to_csv(getFilename('afVsSkanMonthCheck01'))
    mergeDf = pd.read_csv(getFilename('afVsSkanMonthCheck01'))

    errorCount = mergeDf['af_skan'].sum()
    totalCount = mergeDf['count_skan'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    # print(mergeDf)
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['count_skan'].sum()
    error2 = errorCount/totalCount2

    return error,error2

def checkByMonth2(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    idfaDf = afDf.loc[pd.isna(afDf.idfa) == False]

    idfaDf['install_month'] = idfaDf['install_date'].str[0:7]
    skanDf['install_month'] = skanDf['install_date'].str[0:7]


    idfaDf.loc[:,'count'] = 1
    idfaGroupbyDf = idfaDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    # idfaGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(idfaGroupbyDf,skanGroupbyDf,how='outer',on=['install_month','cv'],suffixes=('_idfa','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_month','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['skan_idfa'] = mergeDf['count_skan'] - mergeDf['count_idfa']

    # 计算评判标准
    mergeDf.loc[mergeDf.skan_idfa > 0,'skan_idfa'] = 0
    mergeDf.loc[mergeDf.skan_idfa < 0,'skan_idfa'] *= -1

    mergeDf.to_csv(getFilename('afVsSkanMonthCheck02'))
    mergeDf = pd.read_csv(getFilename('afVsSkanMonthCheck02'))

    errorCount = mergeDf['skan_idfa'].sum()
    totalCount = mergeDf['count_skan'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    # print(mergeDf)
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['count_skan'].sum()
    error2 = errorCount/totalCount2

    return error,error2

# 为表格添加 cv 对应的 usd，为了可以按照金额计算偏差
def addCvToUsd(retDf):
    retDf.insert(retDf.shape[1],'cv2usd',0)
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        avg = (min_event_revenue + max_event_revenue)/2
        if pd.isna(max_event_revenue):
            avg = 0
        retDf.loc[retDf.cv==i,'cv2usd'] = avg
    return retDf

def checkUsd1(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    afDf.loc[:,'count'] = 1
    afGroupbyDf = afDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    # afGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(afGroupbyDf,skanGroupbyDf,how='outer',on=['install_date','cv'],suffixes=('_af','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_date','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['af_skan'] = mergeDf['count_af'] - mergeDf['count_skan']

    # 计算评判标准
    mergeDf.loc[mergeDf.af_skan > 0,'af_skan'] = 0
    mergeDf.loc[mergeDf.af_skan < 0,'af_skan'] *= -1

    mergeDf.to_csv(getFilename('dataAfVsSkanDebug'))
    mergeDf = pd.read_csv(getFilename('dataAfVsSkanDebug'))

    mergeDf = addCvToUsd(mergeDf)
    # mergeDf['af_usd'] = mergeDf['count_af'] * mergeDf['cv2usd']
    mergeDf['skan_usd'] = mergeDf['count_skan'] * mergeDf['cv2usd']
    mergeDf['af_skan_usd'] = mergeDf['af_skan'] * mergeDf['cv2usd']

    mergeDf.to_csv(getFilename('afVsSkanUsdCheck01'))
    mergeDf = pd.read_csv(getFilename('afVsSkanUsdCheck01'))

    errorCount = mergeDf['af_skan_usd'].sum()
    totalCount = mergeDf['skan_usd'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    # print(mergeDf)
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['skan_usd'].sum()
    error2 = errorCount/totalCount2

    return error,error2

def checkUsd2(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    idfaDf = afDf.loc[pd.isna(afDf.idfa) == False]

    idfaDf.loc[:,'count'] = 1
    idfaGroupbyDf = idfaDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_date','cv']).agg({
        'count':'sum'
    })

    # idfaGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(idfaGroupbyDf,skanGroupbyDf,how='outer',on=['install_date','cv'],suffixes=('_idfa','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_date','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['skan_idfa'] = mergeDf['count_skan'] - mergeDf['count_idfa']

    # 计算评判标准
    mergeDf.loc[mergeDf.skan_idfa > 0,'skan_idfa'] = 0
    mergeDf.loc[mergeDf.skan_idfa < 0,'skan_idfa'] *= -1

    mergeDf.to_csv(getFilename('dataAfVsSkanIdfaDebug'))
    mergeDf = pd.read_csv(getFilename('dataAfVsSkanIdfaDebug'))

    mergeDf = addCvToUsd(mergeDf)
    # mergeDf['idfa_usd'] = mergeDf['count_idfa'] * mergeDf['cv2usd']
    mergeDf['skan_usd'] = mergeDf['count_skan'] * mergeDf['cv2usd']
    mergeDf['skan_idfa_usd'] = mergeDf['skan_idfa'] * mergeDf['cv2usd']

    mergeDf.to_csv(getFilename('afVsSkanUsdCheck02'))
    mergeDf = pd.read_csv(getFilename('afVsSkanUsdCheck02'))

    errorCount = mergeDf['skan_idfa_usd'].sum()
    totalCount = mergeDf['skan_usd'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    # print(mergeDf)
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['skan_usd'].sum()
    error2 = errorCount/totalCount2

    return error,error2

def checkUsdByMonth1(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    afDf['install_month'] = afDf['install_date'].str[0:7]
    skanDf['install_month'] = skanDf['install_date'].str[0:7]

    afDf.loc[:,'count'] = 1
    afGroupbyDf = afDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    # afGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(afGroupbyDf,skanGroupbyDf,how='outer',on=['install_month','cv'],suffixes=('_af','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_month','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['af_skan'] = mergeDf['count_af'] - mergeDf['count_skan']

    # 计算评判标准
    mergeDf.loc[mergeDf.af_skan > 0,'af_skan'] = 0
    mergeDf.loc[mergeDf.af_skan < 0,'af_skan'] *= -1

    mergeDf.to_csv(getFilename('dataAfVsSkanDebug'))
    mergeDf = pd.read_csv(getFilename('dataAfVsSkanDebug'))

    mergeDf = addCvToUsd(mergeDf)
    mergeDf['skan_usd'] = mergeDf['count_skan'] * mergeDf['cv2usd']
    mergeDf['af_skan_usd'] = mergeDf['af_skan'] * mergeDf['cv2usd']

    mergeDf.to_csv(getFilename('afVsSkanMonthUsdCheck01'))
    mergeDf = pd.read_csv(getFilename('afVsSkanMonthUsdCheck01'))

    errorCount = mergeDf['af_skan_usd'].sum()
    totalCount = mergeDf['skan_usd'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['skan_usd'].sum()
    error2 = errorCount/totalCount2

    return error,error2

def checkUsdByMonth2(afDf,skanDf):
    # 按照 install_date cv 进行汇总
    idfaDf = afDf.loc[pd.isna(afDf.idfa) == False]

    idfaDf['install_month'] = idfaDf['install_date'].str[0:7]
    skanDf['install_month'] = skanDf['install_date'].str[0:7]


    idfaDf.loc[:,'count'] = 1
    idfaGroupbyDf = idfaDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    skanGroupbyDf = skanDf.groupby(['install_month','cv']).agg({
        'count':'sum'
    })

    # idfaGroupbyDf.to_csv(getFilename('dataAfVsSkanDebug01'))
    # skanDf.to_csv(getFilename('dataAfVsSkanDebug02'))
    # 将两个表merge到一起
    mergeDf = pd.merge(idfaGroupbyDf,skanGroupbyDf,how='outer',on=['install_month','cv'],suffixes=('_idfa','_skan'))
    mergeDf = mergeDf.fillna(0)
    mergeDf = mergeDf.sort_values(by = ['install_month','cv'])

    # 尝试count 相减，找到count小于0的部分
    mergeDf['skan_idfa'] = mergeDf['count_skan'] - mergeDf['count_idfa']

    # 计算评判标准
    mergeDf.loc[mergeDf.skan_idfa > 0,'skan_idfa'] = 0
    mergeDf.loc[mergeDf.skan_idfa < 0,'skan_idfa'] *= -1

    mergeDf.to_csv(getFilename('dataAfVsSkanIdfaDebug'))
    mergeDf = pd.read_csv(getFilename('dataAfVsSkanIdfaDebug'))

    mergeDf = addCvToUsd(mergeDf)
    mergeDf['skan_usd'] = mergeDf['count_skan'] * mergeDf['cv2usd']
    mergeDf['skan_idfa_usd'] = mergeDf['skan_idfa'] * mergeDf['cv2usd']

    mergeDf.to_csv(getFilename('afVsSkanMonthUsdCheck02'))
    mergeDf = pd.read_csv(getFilename('afVsSkanMonthUsdCheck02'))

    errorCount = mergeDf['skan_idfa_usd'].sum()
    totalCount = mergeDf['skan_usd'].sum()
    error = errorCount/totalCount

    # 计算评判标准2
    totalCount2 = mergeDf.loc[mergeDf.cv > 0]['skan_usd'].sum()
    error2 = errorCount/totalCount2

    return error,error2


if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getDataFromSkan()
        df.to_csv(getFilename('skanDataAll20221001_20230201'))

        # df = getDataFromSkanInstallOnly()
        # df.to_csv(getFilename('skanDataInstall20221001_20230201'))

        # df = getDataFromAF()
        # df.to_csv(getFilename('afDataIdfv20221001_20230201'))

    afDf = pd.read_csv(getFilename('afDataIdfv20221001_20230201'))
    afDf = addCV(afDf)

    skanDf = pd.read_csv(getFilename('skanDataAll20221001_20230201'))

    print('check1',check1(afDf,skanDf))
    print('check2',check2(afDf,skanDf))

    print('checkByMonth1',checkByMonth1(afDf,skanDf))
    print('checkByMonth2',checkByMonth2(afDf,skanDf))

    print('checkUsd1',checkUsd1(afDf,skanDf))
    print('checkUsd2',checkUsd2(afDf,skanDf))

    print('checkUsdByMonth1',checkUsdByMonth1(afDf,skanDf))
    print('checkUsdByMonth2',checkUsdByMonth2(afDf,skanDf))


# 总体结论是AF与SKAN的每日差距很大，但是合并到月就可以忽略不计。
# 可能就是安装时间导致的，这个偏差是否可以忽略。

# AF到BI的数据仍然需要进行比对。
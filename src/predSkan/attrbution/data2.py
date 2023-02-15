# 尝试对比af付费率与skan付费率差距

import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame

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

# SKAN 全部用户
def getDataFromSkan0():
    sql = '''
        select
            install_date,
            count(*) as count
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = 'id1479198816'
            and day >= 20221001 and day < 20230205
            and install_date >= '2022-10-01'
            and install_date < '2023-02-01'
        group by
            install_date
        ;
    '''
    df = execSql(sql)
    return df

# SKAN 付费用户
def getDataFromSkan1():
    sql = '''
        select
            install_date,
            count(*) as count
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = 'id1479198816'
            and day >= 20221001 and day < 20230205
            and install_date >= '2022-10-01'
            and install_date < '2023-02-01'
            and skad_conversion_value > 0
        group by
            install_date
    ;
    '''
    df = execSql(sql)
    return df


# SKAN 全部用户，排除redownload
def getDataFromSkan0New():
    sql = '''
        select
            install_date,
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
            install_date
        ;
    '''
    # and event_name in ('af_skad_install','af_skad_redownload')
    df = execSql(sql)
    return df

# SKAN 付费用户，排除redownload
def getDataFromSkan1New():
    sql = '''
        select
            install_date,
            count(*) as count
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = 'id1479198816'
            and day >= 20221001 and day < 20230205
            and install_date >= '2022-10-01'
            and install_date < '2023-02-01'
            and skad_conversion_value > 0
            and event_name in ('af_skad_install')
        group by
            install_date
    ;
    '''
    df = execSql(sql)
    return df




if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df0 = getDataFromSkan0New()
        df1 = getDataFromSkan1New()

        df0.to_csv(getFilename('data2_0'))
        df1.to_csv(getFilename('data2_1'))
    
        skan0 = pd.read_csv(getFilename('data2_0'))
        skan1 = pd.read_csv(getFilename('data2_1'))

        skanDf = pd.merge(skan0,skan1,on = ['install_date'],suffixes = ('_skan_user','_skan_pay_user'))
        skanDf.loc[:,'skan_pr'] = skanDf['count_skan_pay_user']/skanDf['count_skan_user']
        skanDf.to_csv(getFilename('skan'))

    afDf = pd.read_csv(getFilename('afData20221001_20230201'))
    afDf.loc[:,'pay_count'] = 0
    
    afDf.loc[
        (afDf.r1usd > 0),
        'pay_count'
    ] = 1
    afDf.loc[:,'count'] = 1
    afSumDf = afDf.groupby('install_date',as_index=False).agg({
        'count':'sum',
        'pay_count':'sum'
    }).sort_values(by = ['install_date'])

    afSumDf.loc[:,'af_pr'] = afSumDf['pay_count']/afSumDf['count']

    skanDf = pd.read_csv(getFilename('skan'))

    mergeDf = pd.merge(afSumDf,skanDf,on = ['install_date'],suffixes = ('_af','_skan'))

    mergeDf.to_csv(getFilename('afVsSkan'))
        


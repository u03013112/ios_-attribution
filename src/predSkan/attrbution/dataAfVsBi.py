# AF 与 BI 对数

# 用2022年10月 到 2023年1月 3个月按照天和月分别做统计

# 首先需要确认的是AF中的数据在BI中都有，这步可以省略

# 用AF中的IDFV作为索引，和BI中二次归因结论进行比对
# 比对内容：
# 差异用户数所占比例，即AF中按照idfv+安装时间统计到的uid和BI中不一致的用户BI用户数的比例，具体用户差异可以先忽略
# 差异金额占比
# 以及上述两个对比中按天对比与按月对比

# 在有了结论之后看看是否有机会进行校正

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame

# 这里没有获取idfv对应的uid，有了uid也无法一一比对
# 直接获取安装用户数、付费用户数（首日）、付费金额（首日），其中首日均按照激活24小时计算，方便之后与SKAN再次对数

# 获得安装用户数，用idfv做索引
def getUserCountFromAF():
    sql = '''
        select
            count(distinct idfv) as count,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date
        from 
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and zone = 0
            and event_name = 'install'
            and day >= 20221001
            and day <= 20230205
            and install_time >= '2022-10-01'
            and install_time < '2023-02-01'
        group by 
            install_date
        ;
    '''
    df = execSql(sql)
    return df

def getPayUserCountFromAF():
    sql = '''
        select
            count(distinct idfv) as count,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date
        from 
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and zone = 0
            and event_name = 'af_purchase'
            and day >= 20221001
            and day <= 20230205
            and install_time >= '2022-10-01'
            and install_time < '2023-02-01'
            and event_timestamp - install_timestamp <= 1*24*3600
        group by
            install_date
        ;
    '''
    df = execSql(sql)
    return df

def getPayUsdFromAF():
    sql = '''
        select
            sum(
                event_revenue_usd
            ) as r1usd,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date
        from 
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and zone = 0
            and event_name = 'af_purchase'
            and day >= 20221001
            and day <= 20230205
            and install_time >= '2022-10-01'
            and install_time < '2023-02-01'
            and event_timestamp - install_timestamp <= 1*24*3600
        group by
            install_date
        ;
    '''
    df = execSql(sql)
    return df

def mergeDataAF(ucDf,pucDf,usdDf):
    afDf = pd.merge(ucDf,pucDf,on = ['install_date'],suffixes=('','_pay'))
    afDf = pd.merge(afDf,usdDf,on = ['install_date'],suffixes=('',''))
    return afDf

def getPayUserCountFromBI():
    sql = '''
       select
            to_char(
                to_date(install_day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            count(distinct game_uid) as count
        from
            dwd_base_event_purchase_afattribution
        where
            app_package = "id1479198816"
            and app = 102
            and zone = 0
            and window_cycle = 9999
            and time - install_timestamp <= 1*24*3600
            and day >= 20221001
            and day <= 20230205
            and install_day >= '20221001'
            and install_day < '20230201'
        group by
            install_date
        ; 
    '''

    print(sql)
    pd_df = execSql(sql)
    return pd_df

def getPayUsdFromBI():
    sql = '''
       select
            to_char(
                to_date(install_day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(revenue_value_usd) as r1usd
        from
            dwd_base_event_purchase_afattribution
        where
            app_package = "id1479198816"
            and app = 102
            and zone = 0
            and window_cycle = 9999
            and time - install_timestamp <= 1*24*3600
            and day >= 20221001
            and day <= 20230205
            and install_day >= '20221001'
            and install_day < '20230201'
        group by
            install_date
        ; 
    '''

    print(sql)
    pd_df = execSql(sql)
    return pd_df

def mergeDataBI(pucDf,usdDf):
    biDf = pd.merge(pucDf,usdDf,on = ['install_date'],suffixes=('',''))
    return biDf

# 按天进行付费用户差异/ BI付费用户数
# 以及付费金额差异 / BI付费金额
def check(afVsBiDataDf):
    afVsBiDataDf['pay_count_diff'] = afVsBiDataDf['count_pay'] - afVsBiDataDf['count_bi']
    afVsBiDataDf.loc[afVsBiDataDf.pay_count_diff < 0 ,'pay_count_diff'] *= -1

    afVsBiDataDf['pay_usd_diff'] = afVsBiDataDf['r1usd_af'] - afVsBiDataDf['r1usd_bi']
    afVsBiDataDf.loc[afVsBiDataDf.pay_usd_diff < 0,'pay_usd_diff'] *= -1

    payCountDiff = afVsBiDataDf['pay_count_diff'].sum()
    payCountBi = afVsBiDataDf['count_bi'].sum()
    payCountError = payCountDiff/payCountBi

    payUsdDiff = afVsBiDataDf['pay_usd_diff'].sum()
    payUsdBi = afVsBiDataDf['r1usd_bi'].sum()
    payUsdError = payUsdDiff/payUsdBi

    return payCountError,payUsdError

# 按月进行预测
def check2(afVsBiDataDf):
    afVsBiDataDf['install_month'] = afVsBiDataDf['install_date'].str[0:7]
    afVsBiDataDf = afVsBiDataDf.groupby('install_month').agg({
        'count_pay':'sum',
        'count_bi':'sum',
        'r1usd_af':'sum',
        'r1usd_bi':'sum',
    })

    afVsBiDataDf['pay_count_diff'] = afVsBiDataDf['count_pay'] - afVsBiDataDf['count_bi']
    afVsBiDataDf.loc[afVsBiDataDf.pay_count_diff < 0 ,'pay_count_diff'] *= -1

    afVsBiDataDf['pay_usd_diff'] = afVsBiDataDf['r1usd_af'] - afVsBiDataDf['r1usd_bi']
    afVsBiDataDf.loc[afVsBiDataDf.pay_usd_diff < 0,'pay_usd_diff'] *= -1

    payCountDiff = afVsBiDataDf['pay_count_diff'].sum()
    payCountBi = afVsBiDataDf['count_bi'].sum()
    payCountError = payCountDiff/payCountBi

    payUsdDiff = afVsBiDataDf['pay_usd_diff'].sum()
    payUsdBi = afVsBiDataDf['r1usd_bi'].sum()
    payUsdError = payUsdDiff/payUsdBi

    return payCountError,payUsdError

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        # af
        ucDf = getUserCountFromAF()
        ucDf.to_csv(getFilename('afUcData'))

        pucDf = getPayUserCountFromAF()
        pucDf.to_csv(getFilename('afPucData'))

        usdDf = getPayUsdFromAF()
        usdDf.to_csv(getFilename('afUsdData'))

        ucDf = pd.read_csv(getFilename('afUcData'))
        ucDf = ucDf.loc[:,~ucDf.columns.str.match('Unnamed')]

        pucDf = pd.read_csv(getFilename('afPucData'))
        pucDf = pucDf.loc[:,~pucDf.columns.str.match('Unnamed')]
        
        usdDf = pd.read_csv(getFilename('afUsdData'))
        usdDf = usdDf.loc[:,~usdDf.columns.str.match('Unnamed')]

        afDf = mergeDataAF(ucDf,pucDf,usdDf)
        afDf.to_csv(getFilename('afData'))

        # bi
        pucDf = getPayUserCountFromBI()
        pucDf.to_csv(getFilename('biPucData'))

        usdDf = getPayUsdFromBI()
        usdDf.to_csv(getFilename('biUsdData'))

        pucDf = pd.read_csv(getFilename('biPucData'))
        pucDf = pucDf.loc[:,~pucDf.columns.str.match('Unnamed')]
        
        usdDf = pd.read_csv(getFilename('biUsdData'))
        usdDf = usdDf.loc[:,~usdDf.columns.str.match('Unnamed')]
        
        biDf = mergeDataBI(pucDf,usdDf)
        biDf.to_csv(getFilename('biData'))

        afDf = pd.read_csv(getFilename('afData'))
        afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]
        biDf = pd.read_csv(getFilename('biData'))
        biDf = biDf.loc[:,~biDf.columns.str.match('Unnamed')]

        afVsBiDataDf = pd.merge(afDf,biDf,on=['install_date'],suffixes=('_af','_bi'))

        afVsBiDataDf = afVsBiDataDf.drop(['count_af'],axis=1)
        afVsBiDataDf = afVsBiDataDf.rename(columns={'count_pay':'count_af'})
        afVsBiDataDf = afVsBiDataDf.loc[:,~afVsBiDataDf.columns.str.match('Unnamed')]
        afVsBiDataDf = afVsBiDataDf.to_csv(getFilename('afVsBiData1'))
        
        # afVsBiDataDf.to_csv(getFilename('afVsBiData'))

    afVsBiDataDf = pd.read_csv(getFilename('afVsBiData'))
    
    

    # payCountError,payUsdError = check(afVsBiDataDf)
    # print('payCountError',payCountError)
    # print('payUsdError',payUsdError)

    # payCountError,payUsdError = check2(afVsBiDataDf)
    # print('payCountError2',payCountError)
    # print('payUsdError2',payUsdError)

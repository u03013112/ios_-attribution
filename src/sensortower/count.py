# 对数，尝试将sensortower的数据与BI数据对比

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.sensortower.intel import getAndroidDownloadAndRevenue

# 对数1
# 全球 海外安卓，下载量对数，2023全年，按照月份对
def log1():
    df = getAndroidDownloadAndRevenue('com.topwar.gp',startDate='2023-01-01',endDate='2023-12-31')
    
    sql = '''
        select
            count(distinct customer_user_id) as installs,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm"
            ) as install_date
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp'
            and event_name = 'install'
            and zone = 0
            and day >= 20230101
            and day <= 20231231
        group by
            install_date;
    '''
    df2 = execSql(sql)
    df2.to_csv('/src/data/log1.csv',index=False)

    df.rename(columns={
        'date':'install_date'
    },inplace=True)
    df = df.merge(df2,on=['install_date'],how='left')

    df.rename(columns={
        'downloads':'sensortower_downloads',
        'installs':'af_installs'
    },inplace=True)

    df = df[['install_date','af_installs','sensortower_downloads']]
    df.to_csv('/src/data/log2.csv',index=False)
    print(df)

    return df

def log1_corr():
    df = pd.read_csv('/src/data/log2.csv')
    r = df['sensortower_downloads'].sum() / df['af_installs'].sum()
    print('r:',r)

    df0 = df[['af_installs','sensortower_downloads']]

    print(df0.corr())

def log2():
    df = getAndroidDownloadAndRevenue('com.topwar.gp',countries = 'US',startDate='2023-01-01',endDate='2023-12-31')
    
    sql = '''
        select
            count(distinct customer_user_id) as installs,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm"
            ) as install_date
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp'
            and country_code = 'US'
            and event_name = 'install'
            and zone = 0
            and day >= 20230101
            and day <= 20231231
        group by
            install_date;
    '''
    df2 = execSql(sql)
    df2.to_csv('/src/data/log2_1.csv',index=False)

    df.rename(columns={
        'date':'install_date'
    },inplace=True)
    df = df.merge(df2,on=['install_date'],how='left')

    df.rename(columns={
        'downloads':'sensortower_downloads',
        'installs':'af_installs'
    },inplace=True)

    df = df[['install_date','af_installs','sensortower_downloads']]
    df.to_csv('/src/data/log2_2.csv',index=False)
    print(df)

    return df

def log2_corr():
    df = pd.read_csv('/src/data/log2_2.csv')
    r = df['sensortower_downloads'].sum() / df['af_installs'].sum()
    print('r:',r)

    df0 = df[['af_installs','sensortower_downloads']]

    print(df0.corr())

if __name__ == '__main__':
    log1()
    log1_corr()

    log2()
    log2_corr()
    
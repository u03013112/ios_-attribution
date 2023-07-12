# 继续处理FunPlus02_redownload的结果

import numpy as np
import pandas as pd
from datetime import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def getDataFromMC():
    sql = '''
        WITH grouped_raw_table AS (
        SELECT
            appsflyer_id,
            install_date,
            SUM(`facebook ads count`) AS total_facebook_ads_count,
            SUM(`googleadwords_int count`) AS total_googleadwords_int_count,
            SUM(`bytedanceglobal_int count`) AS total_bytedanceglobal_int_count
        FROM
            topwar_ios_funplus02_redownload_raw
        where
            day > '20230401'
        GROUP BY
            appsflyer_id,
            install_date
        ),
        grouped_events_table AS (
        SELECT
            appsflyer_id,
            install_timestamp,
            SUM(
            CASE
                WHEN event_timestamp BETWEEN install_timestamp
                AND install_timestamp + 7 * 24 * 3600 THEN event_revenue_usd
                ELSE 0
            END
            ) AS revenue_7_days
        FROM
            ods_platform_appsflyer_events
        where
            day > '20230401'
        GROUP BY
            appsflyer_id,
            install_timestamp
        ),
        joined_table AS (
        SELECT
            g1.appsflyer_id,
            g1.install_date,
            g1.total_facebook_ads_count,
            g1.total_googleadwords_int_count,
            g1.total_bytedanceglobal_int_count,
            g2.revenue_7_days
        FROM
            grouped_raw_table g1
            JOIN grouped_events_table g2 ON g1.appsflyer_id = g2.appsflyer_id
        )
        SELECT
        install_date,
        SUM(revenue_7_days * total_facebook_ads_count) AS facebook_7_days_revenue,
        SUM(revenue_7_days * total_googleadwords_int_count) AS googleadwords_7_days_revenue,
        SUM(revenue_7_days * total_bytedanceglobal_int_count) AS bytedanceglobal_7_days_revenue
        FROM
        joined_table
        GROUP BY
        install_date;
    '''
    print(sql)
    df = execSql(sql)
    return df

def getAdCost():
    sql = '''
        select
            sum(cost) as cost,
            media_source as media,
            to_char(to_date(day, "yyyymmdd"), "yyyy-mm-dd") as install_date
        from
            ods_platform_appsflyer_masters
        where
            app_id = 'id1479198816'
            and day >= '20230401'
            and media_source in ('Facebook Ads', 'googleadwords_int', 'bytedanceglobal_int')
        group by
            media_source,
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

def main():
    df = getDataFromMC()
    df.to_csv(getFilename('funplus02t1redownload'), index=False)

    # df = getAdCost()
    # df.to_csv(getFilename('funplus02t2'), index=False)

    df1 = pd.read_csv(getFilename('funplus02t1redownload'))
    df2 = pd.read_csv(getFilename('funplus02t2'))
    
    def preprocess_df1(df1):
        df1 = df1.melt(id_vars=['install_date'], var_name='media', value_name='7_days_revenue')
        df1['media'] = df1['media'].map({
            'facebook_7_days_revenue': 'Facebook Ads',
            'googleadwords_7_days_revenue': 'googleadwords_int',
            'bytedanceglobal_7_days_revenue': 'bytedanceglobal_int'
        })
        return df1
    # 预处理数据
    df1 = preprocess_df1(df1)

    # 合并数据
    merge_df = df1.merge(df2, on=['install_date', 'media'])

    # 计算ROI
    merge_df['roi'] = merge_df['7_days_revenue'] / merge_df['cost']

    merge_df = merge_df.sort_values(by=['media','install_date']).reset_index(drop=True)
    merge_df.to_csv(getFilename('funplus02t3redownload'), index=False)

def getSql1():
    sql = '''
        select * from topwar_ios_funplus02_redownload_raw where day > 0;
    '''
    print(sql)
    df = execSql(sql)
    return df

def main2():
    df1 = getSql1()
    # df1.to_csv(getFilename('getDataFromMC2_1'), index=False)

    # df2 = getSql2()
    # df2.to_csv(getFilename('getDataFromMC2_2'), index=False)

    # df1 = pd.read_csv(getFilename('getDataFromMC2_1'))
    df1 = df1.drop(columns=['day'])

    df2 = pd.read_csv(getFilename('getDataFromMC2_2'))
    df2 = df2[df2['revenue_7_days'] > 0]

    df = df1.merge(df2, on='appsflyer_id',how='inner')

    df['facebook_7_days_revenue'] = df['revenue_7_days'] * df['facebook ads count']
    df['googleadwords_7_days_revenue'] = df['revenue_7_days'] * df['googleadwords_int count']
    df['bytedanceglobal_7_days_revenue'] = df['revenue_7_days'] * df['bytedanceglobal_int count']
    df.drop(columns=['revenue_7_days','facebook ads count','googleadwords_int count','bytedanceglobal_int count'], inplace=True)

    def preprocess_df1(df):
        df = df.melt(id_vars=['install_date','appsflyer_id'], var_name='media', value_name='7_days_revenue')
        df['media'] = df['media'].map({
            'facebook_7_days_revenue': 'Facebook Ads',
            'googleadwords_7_days_revenue': 'googleadwords_int',
            'bytedanceglobal_7_days_revenue': 'bytedanceglobal_int'
        })
        return df
    # 预处理数据
    df1 = preprocess_df1(df)
    df1 = df1.groupby(['install_date','media'])['7_days_revenue'].sum().reset_index()

    df2 = pd.read_csv(getFilename('funplus02t2'))


    # 合并数据
    merge_df = df1.merge(df2, on=['install_date', 'media'])

    # 计算ROI
    merge_df['roi'] = merge_df['7_days_revenue'] / merge_df['cost']

    merge_df = merge_df.sort_values(by=['media','install_date']).reset_index(drop=True)
    merge_df.to_csv(getFilename('funplus02t3redownload'), index=False)


import matplotlib.pyplot as plt
def rollAndDraw():
    df = pd.read_csv(getFilename('funplus02t3redownload'))
    
    plt.figure(figsize=(18, 6))

    for media in df['media'].unique():
        df1 = df[df['media'] == media].copy()
        df1['install_date'] = pd.to_datetime(df1['install_date'])
        df1 = df1.set_index('install_date')
        df1 = df1.sort_index()
        df1 = df1.rolling(7).mean()
        df1 = df1.reset_index()
        df1.to_csv(getFilename('funplus02t4_%s' % media), index=False)

        # 绘制图形
        plt.plot(df1['install_date'], df1['roi'], label=media)

    # 设置图形属性
    plt.xlabel('Install Date')
    plt.ylabel('7-Day Average ROI')
    plt.legend()
    plt.title('7-Day Average ROI for Different Media')

    # 保存图形
    plt.savefig(getFilename('funplus02t5redownload', ext='jpg'))

def ewmAndDraw():
    df = pd.read_csv(getFilename('funplus02t3redownload'))
    
    plt.figure(figsize=(18, 6))

    for media in df['media'].unique():
        df1 = df[df['media'] == media].copy()
        df1['install_date'] = pd.to_datetime(df1['install_date'])
        df1 = df1.set_index('install_date')
        df1 = df1.sort_index()
        df1 = df1.ewm(span=7, adjust=False).mean()
        df1 = df1.reset_index()
        df1.to_csv(getFilename('funplus02t4Ewm_%s' % media), index=False)

        # 绘制图形
        plt.plot(df1['install_date'], df1['roi'], label=media)

    # 设置图形属性
    plt.xlabel('Install Date')
    plt.ylabel('7-Day Average ROI')
    plt.legend()
    plt.title('7-Day Average ROI for Different Media')

    # 保存图形
    plt.savefig(getFilename('funplus02t5redownloadEwm', ext='jpg'))

if __name__ == '__main__':
    main2()
    rollAndDraw()
    ewmAndDraw()



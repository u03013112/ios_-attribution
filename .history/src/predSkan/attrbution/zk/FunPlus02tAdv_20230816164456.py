# FunPlus02Adv的后续处理

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
            topwar_ios_funplus02_raw
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
                AND install_timestamp + 1 * 24 * 3600 THEN event_revenue_usd
                ELSE 0
            END
            ) AS revenue_1_days,
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
            g2.revenue_1_days,
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

def getAllRevenue():
    sql = '''
        SELECT
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date ,
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
            app_id = 'id1479198816'
            AND zone = 0
            AND day > '20230401'
            AND install_time > '2023-04-01'
        group by
            install_date;
    '''
    print(sql)
    df = execSql(sql)
    return df

def main():
    df = getDataFromMC()
    df.to_csv(getFilename('funplus02t1'), index=False)

    # df = getAdCost()
    # df.to_csv(getFilename('funplus02t2'), index=False)

    df1 = pd.read_csv(getFilename('funplus02t1'))
    df2 = pd.read_csv(getFilename('funplus02t2'))
    # df1 列：install_date,facebook_7_days_revenue,googleadwords_7_days_revenue,bytedanceglobal_7_days_revenue
    # df2 列：cost,media,install_date，其中media只有三个值：'Facebook Ads', 'googleadwords_int', 'bytedanceglobal_int'
    # 将df1 先改造列，变为 install_date,media,7_days_revenue
    # 再将df1中的media名称规范，规范为 'Facebook Ads', 'googleadwords_int', 'bytedanceglobal_int'
    # 再将df1和df2合并，按照install_date和media merge
    # 计算ROI，记在mergeDf中, 列：install_date,media,7_days_revenue,cost,roi
    # 保存mergeDf到csv文件到getFilename('funplus02t3')
    def preprocess_df1(df1):
        df1 = df1.melt(id_vars=['install_date'], var_name='media', value_name='7_days_revenue')
        df1['media'] = df1['media'].map({
            'facebook_7_days_revenue': 'Facebook Ads',
            'googleadwords_7_days_revenue': 'googleadwords_int',
            'bytedanceglobal_7_days_revenue': 'bytedanceglobal_int',
            'other_7_days_revenue': 'other'
        })
        return df1
    # 预处理数据
    df1 = preprocess_df1(df1)

    # 合并数据
    merge_df = df1.merge(df2, on=['install_date', 'media'])

    # 计算ROI
    merge_df['roi'] = merge_df['7_days_revenue'] / merge_df['cost']

    merge_df = merge_df.sort_values(by=['media','install_date']).reset_index(drop=True)
    merge_df.to_csv(getFilename('funplus02t3'), index=False)

import matplotlib.pyplot as plt
def rollAndDraw():
    df = pd.read_csv(getFilename('funplus02t3'))
    
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
    plt.savefig(getFilename('funplus02t5', ext='jpg'))

def rollAndDraw2():
    # 读取总体回收金额
    df_total = pd.read_csv(getFilename('funplus02tAllRevenue'))
    df_total['install_date'] = pd.to_datetime(df_total['install_date'])
    df_total = df_total.set_index('install_date').sort_index()
    df_total = df_total.rename(columns={'revenue_7_days': 'total_r7usd'})

    # 读取每个媒体的回收金额
    df = pd.read_csv(getFilename('funplus02t3'))
    df['install_date'] = pd.to_datetime(df['install_date'])
    df = df.set_index('install_date').sort_index()

    plt.figure(figsize=(18, 6))

    # 计算并绘制每个媒体的占比
    for media in df['media'].unique():
        df1 = df[df['media'] == media].copy()
        # df1 列改名 7_days_revenue -> '%s_r7usd' % media
        df1 = df1.rename(columns={'7_days_revenue': '%s_r7usd' % media})
        df_total = df_total.merge(df1, on='install_date')

    df_total['media'] = 0
    for media in df['media'].unique():
        df_total['media'] += df_total['%s_r7usd' % media]
        plt.plot(df_total.index, df_total['media'], label=media)

    # 绘制总体回收金额
    plt.plot(df_total.index, df_total['total_r7usd'], label='Total', color='black')


    # 设置图形属性
    plt.xlabel('Install Date')
    plt.ylabel('Daily Media Proportion')
    plt.legend()
    plt.title('Daily Media Proportion for Different Media and Total')

    # 保存图形
    plt.savefig(getFilename('funplus02t52', ext='jpg'))

def ewmAndDraw():
    df = pd.read_csv(getFilename('funplus02t3'))
    
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
    plt.savefig(getFilename('funplus02t5Ewm', ext='jpg'))

def getSql1():
    sql = '''
        select * from topwar_ios_funplus02_adv where day > 0;
    '''
    print(sql)
    df = execSql(sql)
    return df

def getSql2():
    sql = '''
        SELECT
            appsflyer_id,
            SUM(
                CASE
                    WHEN event_timestamp BETWEEN install_timestamp
                    AND install_timestamp + 1 * 24 * 3600 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_1_days,
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
            app_id = 'id1479198816'
            AND zone = 0
            AND day > '20230401'
            AND install_time > '2023-04-01'
        group by
            appsflyer_id
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

def main2():
    df1 = getSql1()
    df1.to_csv(getFilename('getDataFromMC2Adv_1'), index=False)

    df2 = getSql2()
    df2.to_csv(getFilename('getDataFromMC2_2'), index=False)

    df1 = pd.read_csv(getFilename('getDataFromMC2Adv_1'))
    df1 = df1.drop(columns=['day'])

    df2 = pd.read_csv(getFilename('getDataFromMC2_2'))
    df2 = df2[df2['revenue_7_days'] > 0]

    df = df1.merge(df2, on='appsflyer_id',how='inner')

    df['facebook_7_days_revenue'] = df['revenue_7_days'] * df['facebook ads rate']
    df['googleadwords_7_days_revenue'] = df['revenue_7_days'] * df['googleadwords_int rate']
    df['bytedanceglobal_7_days_revenue'] = df['revenue_7_days'] * df['bytedanceglobal_int rate']
    df.drop(columns=['revenue_7_days','facebook ads rate','googleadwords_int rate','bytedanceglobal_int rate'], inplace=True)

    print(df.head())

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

    print(df1.head())


    # 合并数据
    merge_df = df1.merge(df2, on=['install_date', 'media'])

    # 计算ROI
    merge_df['roi'] = merge_df['7_days_revenue'] / merge_df['cost']

    merge_df = merge_df.sort_values(by=['media','install_date']).reset_index(drop=True)
    merge_df.to_csv(getFilename('funplus02t3Adv'), index=False)


def main3():
    # df1 = getSql1()
    # df1.to_csv(getFilename('getDataFromMC2Adv_1'), index=False)

    # df2 = getSql2()
    # df2.to_csv(getFilename('getDataFromMC2_2'), index=False)

    df1 = pd.read_csv(getFilename('getDataFromMC2Adv_1'))
    df1 = df1.drop(columns=['day'])
    df1 = df1.groupby(['install_date','appsflyer_id']).sum().reset_index()

    df2 = pd.read_csv(getFilename('getDataFromMC2_2'))
    df2 = df2[df2['revenue_7_days'] > 0]

    df = df1.merge(df2, on='appsflyer_id',how='inner')

    df['bytedanceglobal_1_days_revenue'] = df['revenue_1_days'] * df['bytedanceglobal_int rate']
    df['googleadwords_1_days_revenue'] = df['revenue_1_days'] * df['googleadwords_int rate']
    df['facebook_1_days_revenue'] = df['revenue_1_days'] * df['facebook ads rate']

    df['facebook_7_days_revenue'] = df['revenue_7_days'] * df['facebook ads rate']
    df['googleadwords_7_days_revenue'] = df['revenue_7_days'] * df['googleadwords_int rate']
    df['bytedanceglobal_7_days_revenue'] = df['revenue_7_days'] * df['bytedanceglobal_int rate']

    df.drop(columns=['revenue_1_days','revenue_7_days','facebook ads rate','googleadwords_int rate','bytedanceglobal_int rate'], inplace=True)

    print(df.head())

    # def preprocess_df1(df):
    #     df = df.melt(id_vars=['install_date','appsflyer_id'], var_name='media', value_name='7_days_revenue')
    #     df['media'] = df['media'].map({
    #         'facebook_7_days_revenue': 'Facebook Ads',
    #         'googleadwords_7_days_revenue': 'googleadwords_int',
    #         'bytedanceglobal_7_days_revenue': 'bytedanceglobal_int'
    #     })
    #     return df
    
    def preprocess_df(df):
        df_7_days = df[['install_date', 'appsflyer_id', 'facebook_7_days_revenue', 'googleadwords_7_days_revenue', 'bytedanceglobal_7_days_revenue']]
        df_1_days = df[['install_date', 'appsflyer_id', 'facebook_1_days_revenue', 'googleadwords_1_days_revenue', 'bytedanceglobal_1_days_revenue']]
        # df_7_days.to_csv(getFilename('funplus02tAdvMain1_0'), index=False)

        df_7_days = df_7_days.melt(id_vars=['install_date','appsflyer_id'], var_name='media', value_name='7_days_revenue')
        df_1_days = df_1_days.melt(id_vars=['install_date','appsflyer_id'], var_name='media', value_name='1_days_revenue')

        # df_7_days.to_csv(getFilename('funplus02tAdvMain1_1'), index=False)

        media_mapping = {
            'facebook_7_days_revenue': 'Facebook Ads',
            'googleadwords_7_days_revenue': 'googleadwords_int',
            'bytedanceglobal_7_days_revenue': 'bytedanceglobal_int',
            'facebook_1_days_revenue': 'Facebook Ads',
            'googleadwords_1_days_revenue': 'googleadwords_int',
            'bytedanceglobal_1_days_revenue': 'bytedanceglobal_int'
        }
        
        df_7_days['media'] = df_7_days['media'].map(media_mapping)
        df_1_days['media'] = df_1_days['media'].map(media_mapping)
        
        df = pd.merge(df_1_days, df_7_days, on=['install_date', 'appsflyer_id', 'media'])
        # df.to_csv(getFilename('funplus02tAdvMain1'), index=False)
        return df
    # 预处理数据
    # df1 = preprocess_df1(df)
    # df1 = df1.groupby(['install_date','media'])['7_days_revenue'].sum().reset_index()
    
    df = preprocess_df(df)
    # print('preprocess_df:')
    # print(df.head())
    df1 = df.groupby(['install_date','media']).agg({'1_days_revenue': 'sum', '7_days_revenue': 'sum'}).reset_index()
    df1.to_csv(getFilename('funplus02tAdvMain2'), index=False)

    df2 = pd.read_csv(getFilename('funplus02t2'))

    print(df1.head())


    # 合并数据
    merge_df = df1.merge(df2, on=['install_date', 'media'])

    # 计算ROI
    merge_df['roi'] = merge_df['7_days_revenue'] / merge_df['cost']

    merge_df = merge_df.sort_values(by=['media','install_date']).reset_index(drop=True)
    merge_df.to_csv(getFilename('funplus02t3Adv'), index=False)

def debug():
    df = pd.read_csv(getFilename('funplus02t3Adv'))
    df = df[df['media'] == 'Facebook Ads']
    df = df[
        (df['install_date'] >= '2023-06-01') &
        (df['install_date'] <= '2023-06-30')
    ]
    print(df.head())
    print(df['1_days_revenue'].sum()/df['cost'].sum())

if __name__ == '__main__':
    
    # df = getAllRevenue()
    # df.to_csv(getFilename('funplus02tAllRevenue'), index=False)
    # main()
    # main2()
    # rollAndDraw()
    # ewmAndDraw()

    # rollAndDraw2()

    # main3()

    debug()



# 继续处理FunPlus02Ssot的结果

# FunPlus02得到的结果是一个数据库表
# 表名topwar_ios_funplus02_ssot_raw
# 表结构 
# 列1 appsflyer_id string
# 列2 install_date string 类似 ‘2023-05-31’
# 列3 day string 类似 ‘20230531’
# 列4 ‘facebook ads count’ double 
# 列5 ‘googleadwords_int count’ double
# 列6 ‘bytedanceglobal_int count’ double

# 表名ods_platform_appsflyer_events
# 表结构 
# 列1 appsflyer_id string
# 列2 install_timestamp bigint unix时间戳，单位秒
# 列3 event_timestamp bigint unix时间戳，单位秒
# 列4 event_revenue_usd double 事件收入，单位美元

# topwar_ios_funplus02_ssot_raw与ods_platform_appsflyer_events合并，计算上面3个媒体每日(install_date)的7日（7*24小时）回收金额
# 其中 ods_platform_appsflyer_events 与 topwar_ios_funplus02_ssot_raw 的合并条件是：ods_platform_appsflyer_events.appsflyer_id = topwar_ios_funplus02_ssot_raw.appsflyer_id
# 但是获取媒体count的时候要先将topwar_ios_funplus02_ssot_raw按照appsflyer_id分组，媒体count要求和
# 其中媒体的7日回收金额 计算方式为：ods_platform_appsflyer_events中event_timestamp在install_timestamp的7天内的event_revenue_usd的和 * 媒体 count
# 比如：媒体facebook的7日回收金额 计算方式为：ods_platform_appsflyer_events中event_timestamp在install_timestamp的7天内的event_revenue_usd的和 * facebook ads count


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
            topwar_ios_funplus02_ssot_raw
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

# 获得所有有归因结论的用户收入（包括模糊归因）
def getDataFromMC2():
    sql = '''
        select
            media_source as media,
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN event_revenue_usd
                    ELSE 0
                END
            ) as r1usd,
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 168 * 3600 THEN event_revenue_usd
                    ELSE 0
                END
            ) as r7usd,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            AND zone = 0
            AND day > '20230401'
            and install_time > '2023-04-01'
            and media_source in (
                'Facebook Ads',
                'restricted',
                'googleadwords_int',
                'bytedanceglobal_int'
            )
        group by
            media_source,
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
    # df = getDataFromMC()
    # df.to_csv(getFilename('funplus02tSsot1'), index=False)

    # df = getDataFromMC2()
    # df.to_csv(getFilename('funplus02tSsot2'), index=False)
    
    # df = getAdCost()
    # df.to_csv(getFilename('funplus02tSsot3'), index=False)
    
    df1 = pd.read_csv(getFilename('funplus02tSsot1'))
    df2 = pd.read_csv(getFilename('funplus02tSsot3'))
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
            'bytedanceglobal_7_days_revenue': 'bytedanceglobal_int'
        })
        return df1
    # 预处理数据
    df1 = preprocess_df1(df1)
    df1_5 = pd.read_csv(getFilename('funplus02tSsot2'))
    # df1_5 media 内容修改 restricted -> Facebook Ads，其他不变，用replace
    df1_5['media'] = df1_5['media'].replace({'restricted': 'Facebook Ads'})

    # df1_5 = df1_5.rename(columns={'r7usd': '7_days_revenue'})
    # df1 = pd.concat([df1, df1_5], axis=0)
    # df1 = df1.groupby(['install_date', 'media']).sum().reset_index()

    df1_5 = df1_5.rename(columns={'r7usd': '7_days_revenue'})
    df1 = df1.merge(df1_5, on=['install_date', 'media'], how='left',suffixes=('_zk', '_ssot'))
    df1['7_days_revenue'] = df1['7_days_revenue_zk'].fillna(0) + df1['7_days_revenue_ssot'].fillna(0)

    # 合并数据
    merge_df = df1.merge(df2, on=['install_date', 'media'])

    # 计算ROI
    merge_df['roi'] = merge_df['7_days_revenue'] / merge_df['cost']

    merge_df = merge_df.sort_values(by=['media','install_date']).reset_index(drop=True)
    merge_df.to_csv(getFilename('funplus02tSsot4'), index=False)

def getSql1():
    sql = '''
        select * from topwar_ios_funplus02_ssot_raw where day > 0;
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
    merge_df.to_csv(getFilename('funplus02tSsot4'), index=False)

def main3():
    # df1 = getSql1()
    # df1.to_csv(getFilename('getDataFromMC2Ssot_1'), index=False)

    # df2 = getSql2()
    # df2.to_csv(getFilename('getDataFromMC2_2'), index=False)

    df1 = pd.read_csv(getFilename('getDataFromMC2Ssot_1'))
    df1 = df1.drop(columns=['day'])
    df1 = df1.groupby(['install_date','appsflyer_id']).sum().reset_index()

    df2 = pd.read_csv(getFilename('getDataFromMC2_2'))
    df2 = df2[df2['revenue_7_days'] > 0]

    df = df1.merge(df2, on='appsflyer_id',how='inner')

    df['bytedanceglobal_1_days_revenue'] = df['revenue_1_days'] * df['bytedanceglobal_int count']
    df['googleadwords_1_days_revenue'] = df['revenue_1_days'] * df['googleadwords_int count']
    df['facebook_1_days_revenue'] = df['revenue_1_days'] * df['facebook ads count']

    df['facebook_7_days_revenue'] = df['revenue_7_days'] * df['facebook ads count']
    df['googleadwords_7_days_revenue'] = df['revenue_7_days'] * df['googleadwords_int count']
    df['bytedanceglobal_7_days_revenue'] = df['revenue_7_days'] * df['bytedanceglobal_int count']

    df.drop(columns=['revenue_1_days','revenue_7_days','facebook ads count','googleadwords_int count','bytedanceglobal_int count'], inplace=True)

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
    merge_df.to_csv(getFilename('funplus02tSsot4'), index=False)

def main3Tail():
    # 和模糊归因结果合并
    # df = getDataFromMC2()
    # df.to_csv(getFilename('funplus02tSsot2'), index=False)

    df = pd.read_csv(getFilename('funplus02tSsot2'))
    # df列media中 restricted -> Facebook Ads
    df['media'] = df['media'].replace({'restricted': 'Facebook Ads'})
    df.rename(columns={
        'r1usd': '1_days_revenue',
        'r7usd': '7_days_revenue'
    }, inplace=True)

    df2 = pd.read_csv(getFilename('funplus02tSsot4'))
    df2 = df2[['install_date','media','1_days_revenue','7_days_revenue']]

    # 合并数据，直接两个表拼接
    # merge_df = pd.concat([df, df2], axis=0)
    merge_df = df.merge(df2, on=['install_date', 'media'], how='left',suffixes=('_ssot', '_zk'))
    merge_df['1_days_revenue'] = merge_df['1_days_revenue_zk'].fillna(0) + merge_df['1_days_revenue_ssot'].fillna(0)
    merge_df['7_days_revenue'] = merge_df['7_days_revenue_zk'].fillna(0) + merge_df['7_days_revenue_ssot'].fillna(0)
    # merge_df = merge_df.groupby(['install_date','media']).sum().reset_index()

    merge_df = merge_df.sort_values(by=['media','install_date']).reset_index(drop=True)
    merge_df = merge_df[['install_date','media','1_days_revenue','7_days_revenue']]
    merge_df = merge_df.loc[(merge_df['install_date'] >= '2023-05-22') & (merge_df['install_date'] <= '2023-06-04')]

    merge_df.rename(columns={
        '1_days_revenue':'融合归因（包含概率归因） 1日收入（美元）',
        '7_days_revenue':'融合归因（包含概率归因） 7日收入（美元）'
    }, inplace=True)

    merge_df.to_csv(getFilename('funplus02tSsot4Tail'), index=False)
    
import matplotlib.pyplot as plt
def rollAndDraw():
    df = pd.read_csv(getFilename('funplus02tSsot4'))
    
    plt.figure(figsize=(18, 6))

    for media in df['media'].unique():
        df1 = df[df['media'] == media].copy()
        df1['install_date'] = pd.to_datetime(df1['install_date'])
        df1 = df1.set_index('install_date')
        df1 = df1.sort_index()
        df1 = df1.rolling(7).mean()
        df1 = df1.reset_index()
        df1.to_csv(getFilename('funplus02tSsot5_%s' % media), index=False)

        # 绘制图形
        plt.plot(df1['install_date'], df1['roi'], label=media)

    # 设置图形属性
    plt.xlabel('Install Date')
    plt.ylabel('7-Day Average ROI')
    plt.legend()
    plt.title('7-Day Average ROI for Different Media')

    # 保存图形
    plt.savefig(getFilename('funplus02tSsot6', ext='jpg'))

def ewmAndDraw():
    df = pd.read_csv(getFilename('funplus02tSsot4'))
    
    plt.figure(figsize=(18, 6))

    for media in df['media'].unique():
        df1 = df[df['media'] == media].copy()
        df1['install_date'] = pd.to_datetime(df1['install_date'])
        df1 = df1.set_index('install_date')
        df1 = df1.sort_index()
        df1 = df1.ewm(span=7, adjust=False).mean()
        df1 = df1.reset_index()
        df1.to_csv(getFilename('funplus02tSsot5Ewm_%s' % media), index=False)

        # 绘制图形
        plt.plot(df1['install_date'], df1['roi'], label=media)

    # 设置图形属性
    plt.xlabel('Install Date')
    plt.ylabel('7-Day Average ROI')
    plt.legend()
    plt.title('7-Day Average ROI for Different Media')

    # 保存图形
    plt.savefig(getFilename('funplus02tSsot6Ewm', ext='jpg'))

if __name__ == '__main__':
    # main2()
    # rollAndDraw()
    # ewmAndDraw()
    # main3()
    main3Tail()



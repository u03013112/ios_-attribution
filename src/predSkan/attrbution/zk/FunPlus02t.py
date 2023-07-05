# 继续处理FunPlus02的结果

# FunPlus02得到的结果是一个数据库表
# 表名topwar_ios_funplus02_raw
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

# topwar_ios_funplus02_raw与ods_platform_appsflyer_events合并，计算上面3个媒体每日(install_date)的7日（7*24小时）回收金额
# 其中 ods_platform_appsflyer_events 与 topwar_ios_funplus02_raw 的合并条件是：ods_platform_appsflyer_events.appsflyer_id = topwar_ios_funplus02_raw.appsflyer_id
# 但是获取媒体count的时候要先将topwar_ios_funplus02_raw按照appsflyer_id分组，媒体count要求和
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
    # df = getDataFromMC()
    # df.to_csv(getFilename('funplus02t1'), index=False)

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
    merge_df.to_csv(getFilename('funplus02t3'), index=False)


if __name__ == '__main__':
    main()



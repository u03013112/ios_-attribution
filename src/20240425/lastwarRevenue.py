# lastwar 融合归因 收入计算
# 由于使用了分媒体的融合归因数据，所以分国家分析的价值不大，暂时只分媒体。

import os
import pandas as pd
import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

# 获得1日收入、3日收入、7日收入，分媒体、分国家，按照安装日期汇总
# 其中startDayStr 和 endDayStr 是字符串，格式为'20231118' 是安装日期过滤
def getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr):
    filename = f'/src/data/lwRevenue_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str})
    else:
        print('从MC获得数据')


        sql = f'''
    SET
        odps.sql.timezone = Africa / Accra;

    set
        odps.sql.hive.compatible = true;

    set
        odps.sql.executionengine.enable.rand.time.seed = true;

    @rhData :=
    select
        customer_user_id,
        media,
        rate
    from
        lastwar_ios_funplus02_adv_uid_mutidays_media
    where
        day between '{startDayStr}' and '{endDayStr}';

    @biData :=
    SELECT
        game_uid as customer_user_id,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r1usd,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 3 * 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r3usd,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 7 * 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r7usd,
        install_day as install_date,
        country as country_code
    FROM
        rg_bi.ads_lastwar_ios_purchase_adv
    WHERE
        game_uid IS NOT NULL
    GROUP BY
        game_uid,
        install_day,
        country;

    @biData2 :=
    select
        customer_user_id,
        r1usd,
        r3usd,
        r7usd,
        CASE
            WHEN r1usd = 0 THEN 'free'
            WHEN r1usd > 0
            AND r1usd <= 10 THEN 'low'
            WHEN r1usd > 10
            AND r1usd <= 80 THEN 'mid'
            ELSE 'high'
        END as paylevel,
        install_date,
        country_code
    from
        @biData;

    select
        rh.media,
        sum(bi.r1usd * rh.rate) as r1usd,
        sum(bi.r3usd * rh.rate) as r3usd,
        sum(bi.r7usd * rh.rate) as r7usd,
        bi.paylevel,
        bi.install_date,
        bi.country_code,
        sum(rh.rate) as installs
    from
        @rhData as rh
        left join @biData2 as bi on rh.customer_user_id = bi.customer_user_id
    group by
        rh.media,
        bi.install_date,
        bi.country_code,
        bi.paylevel
    ;
        '''
        print(sql)
        df = execSql(sql)

        df.to_csv(filename,index=False)
    
    return df


# 按媒体区分，统计各媒体的如下数据，然后作比对
# 1. 1日收入、3日收入、7日收入
# 2. 3日收入/1日收入，7日收入/1日收入
# 3. 各个付费等级的人数占比
def analyzeRevenueDataIOSGroupByMedia(startDayStr,endDayStr):
    df = getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr)
    df['install_date'] = pd.to_datetime(df['install_date'],format='%Y%m%d')

    # 1. 1日收入、3日收入、7日收入
    df1 = df.groupby(['media']).agg({'r1usd':'sum','r3usd':'sum','r7usd':'sum','installs':'sum'}).reset_index()
    df1['r3usd/r1usd'] = df1['r3usd'] / df1['r1usd']
    df1['r7usd/r1usd'] = df1['r7usd'] / df1['r1usd']
    print(df1[['media','r3usd/r1usd','r7usd/r1usd']])
    df1.to_csv(f'/src/data/20240425df1_{startDayStr}_{endDayStr}_media.csv',index=False)
    # 2. 各个付费等级的人数占比
    df2 = df.groupby(['media','paylevel']).agg({'installs':'sum'}).reset_index()
    df2 = df2.pivot(index='media',columns='paylevel',values='installs').reset_index()
    df2['total'] = df2['free'] + df2['low'] + df2['mid'] + df2['high']
    df2['free'] = df2['free'] / df2['total']
    df2['low'] = df2['low'] / df2['total']
    df2['mid'] = df2['mid'] / df2['total']
    df2['high'] = df2['high'] / df2['total']
    df2 = df2.drop(columns=['total'])
    print(df2[['media','free','low','mid','high']])
    df2.to_csv(f'/src/data/20240425df2_{startDayStr}_{endDayStr}_media.csv',index=False)

    return df1,df2


if __name__ == '__main__':
    analyzeRevenueDataIOSGroupByMedia('20240415','20240421')
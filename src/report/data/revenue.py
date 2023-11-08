import os
import pandas as pd
import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

from src.report.data.tools import getFilename1
from src.report.geo import getIOSGeoGroup01
from src.report.media import getIOSMediaGroup01

# 获得1日收入、3日收入、7日收入，分媒体、分国家，按照安装日期汇总
def getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr,directory):
    print('getRevenueDataIOSGroupByGeoAndMedia 采用融合归因结论，媒体固定只能分这么多，请注意')
    filename = getFilename1('revenue',startDayStr,endDayStr,directory,'GroupByGeoAndMedia')
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str})
    else:
        print('从MC获得数据')

    # startDayStr 格式 20231001 转成 2023-10-01 00:00:00
    startDayStr2 = datetime.datetime.strptime(startDayStr,'%Y%m%d').strftime('%Y-%m-%d 00:00:00')
    # endDayStr 格式 20231001 转成 2023-10-01 23:59:59
    endDayStr2 = datetime.datetime.strptime(endDayStr,'%Y%m%d').strftime('%Y-%m-%d 23:59:59')

    sql = f'''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND install_timestamp between UNIX_TIMESTAMP(datetime '{startDayStr2}') AND UNIX_TIMESTAMP(datetime '{endDayStr2}')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '{startDayStr}'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        adv_uid_mutidays AS (
            SELECT
                customer_user_id,
                facebook_ads_rate,
                googleadwords_int_rate,
                bytedanceglobal_int_rate,
                other_rate
            FROM
                rg_bi.topwar_ios_funplus02_adv_uid_mutidays
            WHERE
                day between '{startDayStr}' AND '{endDayStr}'
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd,
                COALESCE(a.facebook_ads_rate, 0) AS facebook_ads_rate,
                COALESCE(a.googleadwords_int_rate, 0) AS googleadwords_int_rate,
                COALESCE(a.bytedanceglobal_int_rate, 0) AS bytedanceglobal_int_rate,
                COALESCE(a.other_rate, 0) AS other_rate
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
                AND o.event_timestamp >= t.install_timestamp
                LEFT JOIN adv_uid_mutidays a ON t.game_uid = a.customer_user_id
        )
        SELECT
            country_code,
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * facebook_ads_rate
            ) AS facebook_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * googleadwords_int_rate
            ) AS google_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * bytedanceglobal_int_rate
            ) AS bytedanceglobal_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * other_rate
            ) AS other_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd * (1 - facebook_ads_rate - googleadwords_int_rate - bytedanceglobal_int_rate - other_rate)
                    ELSE 0
                END
            ) AS organic_revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * facebook_ads_rate
            ) AS facebook_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * googleadwords_int_rate
            ) AS google_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * bytedanceglobal_int_rate
            ) AS bytedanceglobal_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * other_rate
            ) AS other_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd * (1 - facebook_ads_rate - googleadwords_int_rate - bytedanceglobal_int_rate - other_rate)
                    ELSE 0
                END
            ) AS organic_revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * facebook_ads_rate
            ) AS facebook_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * googleadwords_int_rate
            ) AS google_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * bytedanceglobal_int_rate
            ) AS bytedanceglobal_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * other_rate
            ) AS other_revenue_7d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd * (1 - facebook_ads_rate - googleadwords_int_rate - bytedanceglobal_int_rate - other_rate)
                    ELSE 0
                END
            ) AS organic_revenue_7d
        FROM
            joined_data
        GROUP BY
            install_date,
            country_code
        HAVING
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END
            ) > 0
        ORDER BY
            install_date;
    '''
    print(sql)
    df = execSql(sql)

    df_long = df.melt(id_vars=['country_code', 'install_date'], 
                    var_name='media_day', 
                    value_name='revenue')

    # 分割media_day列为media和day列
    df_long[['media', 'day']] = df_long['media_day'].str.split('_', n=1,expand=True)

    # 删除不再需要的media_day列
    df_long = df_long.drop(columns='media_day')

    # 使用pivot_table函数将其转换为你需要的格式
    df_pivot = df_long.pivot_table(index=['country_code', 'install_date', 'media'], 
                                columns='day', 
                                values='revenue').reset_index()

    # 重命名列名
    df_pivot.columns.name = ''
    df = df_pivot.rename(columns={'1d': 'revenue_1d', '3d': 'revenue_3d', '7d': 'revenue_7d'})

    # 分媒体采用的融合归因结论，这里不需要再转化就是需要的结果

    geoGroupList = getIOSGeoGroup01()
    df['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df.loc[df.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    
    df = df.groupby(['install_date','geoGroup','media'],as_index=False).sum().reset_index(drop=True)

    df.to_csv(filename,index=False)
    print('已获得%d条数据'%len(df))
    print('存储在%s'%filename)
    df['install_date'] = df['install_date'].astype(str)
    return df

def getRevenueDataIOSGroupByCampaignAndGeoAndMedia(startDayStr,endDayStr,directory):
    print('getRevenueDataIOSGroupByCampaignAndGeoAndMedia 采用融合归因结论，媒体固定只能分这么多，请注意')
    filename = getFilename1('revenue',startDayStr,endDayStr,directory,'GroupByCampaignAndGeoAndMedia')
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str,'campaign_id':str})
    else:
        print('从MC获得数据')

    # startDayStr 格式 20231001 转成 2023-10-01 00:00:00
    startDayStr2 = datetime.datetime.strptime(startDayStr,'%Y%m%d').strftime('%Y-%m-%d 00:00:00')
    # endDayStr 格式 20231001 转成 2023-10-01 23:59:59
    endDayStr2 = datetime.datetime.strptime(endDayStr,'%Y%m%d').strftime('%Y-%m-%d 23:59:59')

    sql = f'''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND install_timestamp between UNIX_TIMESTAMP(datetime '{startDayStr2}')
                AND UNIX_TIMESTAMP(datetime '{endDayStr2}')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '{startDayStr}'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        adv_uid_mutidays_campaign AS (
            SELECT
                customer_user_id,
                campaign_id,
                rate
            FROM
                rg_bi.topwar_ios_funplus02_adv_uid_mutidays_campaign
            WHERE
                day between '{startDayStr}'
                AND '{endDayStr}'
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd,
                a.campaign_id,
                a.rate
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
                AND o.event_timestamp >= t.install_timestamp
                LEFT JOIN adv_uid_mutidays_campaign a ON t.game_uid = a.customer_user_id
        )
        SELECT    
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            jd.country_code,
            jd.campaign_id,
            cmap.mediasource,
            cmap.campaign_name,
            SUM(rate) AS install,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END * rate
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END * rate
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END * rate
            ) AS revenue_7d
        FROM
            joined_data jd
            LEFT JOIN (
                SELECT 
                    cmap.campaign_id,
                    MAX(cmap.mediasource) AS mediasource,
                    MAX(cmap.campaign_name) AS campaign_name
                FROM 
                    rg_bi.dwb_overseas_mediasource_campaign_map AS cmap
                GROUP BY 
                    cmap.campaign_id
            ) AS cmap ON jd.campaign_id = cmap.campaign_id
        GROUP BY
            install_date,
            jd.country_code,
            jd.campaign_id,
            cmap.mediasource,
            cmap.campaign_name
        ORDER BY
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)

    geoGroupList = getIOSGeoGroup01()
    df['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df.loc[df.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']

    mediaGroupList = getIOSMediaGroup01()
    df['media'] = 'other'
    for mediaGroup in mediaGroupList:
        df.loc[df.mediasource.isin(mediaGroup['codeList']),'media'] = mediaGroup['name']

    df = df.groupby(['install_date','campaign_id','campaign_name','geoGroup','media'],as_index=False).sum().reset_index(drop=True)

    sql = f'''
        WITH tmp_unique_id AS (
            SELECT
                CAST(install_timestamp AS BIGINT) AS install_timestamp,
                game_uid,
                country_code
            FROM
                rg_bi.tmp_unique_id
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND install_timestamp between UNIX_TIMESTAMP(datetime '{startDayStr2}')
                AND UNIX_TIMESTAMP(datetime '{endDayStr2}')
        ),
        ods_platform_appsflyer_events AS (
            SELECT
                customer_user_id,
                event_timestamp,
                event_revenue_usd
            FROM
                rg_bi.ods_platform_appsflyer_events
            WHERE
                app = 102
                AND app_id = 'id1479198816'
                AND day >= '{startDayStr}'
                AND event_name IN ('af_purchase_oldusers', 'af_purchase')
                AND zone = 0
        ),
        joined_data AS (
            SELECT
                t.install_timestamp,
                t.game_uid,
                t.country_code,
                o.event_timestamp,
                o.event_revenue_usd
            FROM
                tmp_unique_id t
                LEFT JOIN ods_platform_appsflyer_events o ON t.game_uid = o.customer_user_id
                AND o.event_timestamp >= t.install_timestamp
        )
        SELECT
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            jd.country_code,
            COUNT(distinct game_uid) AS install,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 1 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 3 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN DATEDIFF(
                        FROM_UNIXTIME(event_timestamp),
                        FROM_UNIXTIME(install_timestamp),
                        'dd'
                    ) < 7 THEN event_revenue_usd
                    ELSE 0
                END
            ) AS revenue_7d
        FROM
            joined_data jd
        GROUP BY
            install_date,
            jd.country_code
        ORDER BY
            install_date;
    '''
    print(sql)
    df2 = execSql(sql)
    df2['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df2.loc[df2.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    df2 = df2.groupby(['install_date','geoGroup'],as_index=False).sum().reset_index(drop=True)
    dfGroup = df.groupby(['install_date','geoGroup'],as_index=False).sum().reset_index(drop=True)
    mergeDf = pd.merge(df2,dfGroup,on=['install_date','geoGroup'],how='left',suffixes=('_total','_other'))
    mergeDf['install'] = mergeDf['install_total'] - mergeDf['install_other']
    mergeDf['revenue_1d'] = mergeDf['revenue_1d_total'] - mergeDf['revenue_1d_other']
    mergeDf['revenue_3d'] = mergeDf['revenue_3d_total'] - mergeDf['revenue_3d_other']
    mergeDf['revenue_7d'] = mergeDf['revenue_7d_total'] - mergeDf['revenue_7d_other']
    mergeDf = mergeDf[['install_date','geoGroup','install','revenue_1d','revenue_3d','revenue_7d']]
    mergeDf['media'] = 'organic'
    mergeDf['campaign_id'] = 'organic'
    mergeDf['campaign_name'] = 'organic'
    # 将mergeDf中，revenue_7d 为 空的行，删除
    mergeDf = mergeDf.loc[mergeDf.revenue_7d.notnull()]
    df = pd.concat([df,mergeDf],ignore_index=True)

    df.to_csv(filename,index=False)
    print('已获得%d条数据'%len(df))
    print('存储在%s'%filename)
    df['install_date'] = df['install_date'].astype(str)

    return df


if __name__ == '__main__':
    startDayStr = '20230826'
    endDayStr = '20231025'
    directory = '/src/data/report/iOSWeekly20231018_20231025'
    df = getRevenueDataIOSGroupByCampaignAndGeoAndMedia(startDayStr,endDayStr,directory)
    print(df)

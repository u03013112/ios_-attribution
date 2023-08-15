# 获得 安卓越南 分campaign的7日回收数据

import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getVnR7Usd():
    sql = '''
        WITH installs AS (
            SELECT
                appsflyer_id AS uid,
                to_char(
                    to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                    "yyyy-mm-dd"
                ) AS install_date,
                install_timestamp,
                media_source,
                country_code,
                campaign_id,
                campaign
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp.vn'
                AND zone = 0
                AND event_name = 'install'
                AND day BETWEEN '20230101'
                AND '20230730'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
                AND to_date('2023-07-01', "yyyy-mm-dd")
        ),
        purchases AS (
            SELECT
                appsflyer_id AS uid,
                event_timestamp,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                event_name = 'af_purchase'
                AND zone = 0
                AND day BETWEEN '20230101'
                AND '20230730'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
                AND to_date('2023-07-01', "yyyy-mm-dd")
        )
        SELECT
            installs.install_date,
            installs.media_source,
            installs.campaign_id,
            installs.campaign,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 7 * 86400
                ),
                0
            ) AS r7usd
        FROM
            installs
            LEFT JOIN purchases ON installs.uid = purchases.uid
        GROUP BY
            installs.install_date,
            installs.media_source,
            installs.campaign_id,
            installs.campaign
        ;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/zk2/VnR7Usd.csv', index=False)
    return df

def getMediaData():
    sql = '''
        select
            mediasource as media,
            to_char(
                to_date(day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(installs) as installs,
            sum(cost) as cost
        from
        (
            select
                day,
                mediasource,
                getapppackagev2(
                    app,
                    mediasource,
                    campaign_name,
                    adset_name,
                    ad_name
                ) as app_package,
                campaign_name,
                adset_name,
                ad_name,
                impressions,
                clicks,
                installs,
                cost
            from
                ods_realtime_mediasource_cost
            where
                app = 102
                and day >= 20230101
                and day < 20230701
        )
        where
            app_package = 'com.topwar.gp.vn'
        group by
            mediasource,
            day
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/zk2/media_20230101_20230701',index=False)
    return df



if __name__ == '__main__':
    # getVnR7Usd()
    getMediaData()
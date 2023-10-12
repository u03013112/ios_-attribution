# 对数，和BI webUI对数
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def sql1():
    sql = '''
       select
            install_day,
            sum(revenue_value_usd) as revenue
        from
            dwd_overseas_revenue_afattribution_realtime
        where
            app = 102
            and zone = 0
            and window_cycle = 9999
            and day >= 20230910
            and day <= 20230918
            and app_package = 'id1479198816'
            and install_day > 20230901
            and install_day < 20230910
            and mediasource = 'bytedanceglobal_int'
        group by
            install_day; 
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/zk2/check1.csv',index=False)
    print(df)

def sql2():
    sql = '''
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
                AND mediasource = 'bytedanceglobal_int'
                AND install_timestamp >= UNIX_TIMESTAMP(datetime '2023-09-01 00:00:00')
                AND install_timestamp < UNIX_TIMESTAMP(datetime '2023-09-10 00:00:00')
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
                AND day >= '20230901'
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
            WHERE
                o.event_timestamp >= t.install_timestamp
        )
        SELECT
            to_char(FROM_UNIXTIME(install_timestamp), 'YYYYMMDD') AS install_date,
            SUM(event_revenue_usd) AS revenue
        FROM
            joined_data
        WHERE
            DATEDIFF(
                FROM_UNIXTIME(event_timestamp),
                FROM_UNIXTIME(install_timestamp),
                'dd'
            ) < 7
        GROUP BY
            install_date;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/zk2/check2.csv',index=False)
    print(df)

# sql1()
# sql2()

def check():
    df1 = pd.read_csv('/src/data/zk2/check1.csv')
    df2 = pd.read_csv('/src/data/zk2/check2.csv')
    df = df1.merge(df2,on='install_date',how='outer')
    print(df)

check()

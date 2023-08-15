# ios 自然量 付费用户数，付费金额 占比 2022-10-01 ~ 2023-04-30

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getDataFromMC(sinceDayStr = '20230101', untilDayStr = '20230808'):
    sql = f'''
        SELECT
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            COUNT(DISTINCT 
                CASE
                    WHEN event_timestamp - install_timestamp <= 1 * 24 * 3600 AND event_revenue_usd > 0 THEN appsflyer_id
                    ELSE NULL
                END
            ) as paid_user_count,
            COUNT(DISTINCT 
                CASE
                    WHEN event_timestamp - install_timestamp <= 1 * 24 * 3600 AND event_revenue_usd = 0 THEN appsflyer_id
                    ELSE NULL
                END
            ) as non_paid_user_count,
            SUM(
                CASE
                    WHEN event_timestamp - install_timestamp <= 1 * 24 * 3600 THEN event_revenue_usd
                    ELSE 0
                END
            ) as total_paid_amount
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND event_name = 'af_purchase'
            AND zone = 0
            AND day >= {sinceDayStr}
            AND day <= {untilDayStr}
        GROUP BY
            install_date
        ;
    '''

    print(sql)
    df = execSql(sql)
    return df

def getSkanData(sinceDayStr = '20230101', untilDayStr = '20230808'):
    sql = f'''
        SELECT
            install_date,
            sum(
                case when skad_conversion_value > 0
                    then 1
                    else 0
                end
            ) as paid_user_count,
            sum(
                case when skad_conversion_value = 0
                    then 1
                    else 0
                end
            ) as non_paid_user_count,
            sum(skad_revenue) as skad_revenue
        FROM
            ods_platform_appsflyer_skad_details
        WHERE
            app_id = 'id1479198816'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
            AND day >= { sinceDayStr }
            AND day <= { untilDayStr }
        GROUP BY
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

    
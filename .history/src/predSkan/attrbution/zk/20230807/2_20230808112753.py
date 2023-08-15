# ios 自然量 付费用户数，付费金额 占比 2022-10-01 ~ 2023-04-30

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getDataFromMC(sinceDayStr = '20230101', untilDayStr = '20230808'):
    sql = f'''
        select
            customer_user_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                    when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r1usd,
            sum(
                case
                    when event_timestamp - install_timestamp <= 2 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r2usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= {sinceDayStr}
            and day <= {untilDayStr}
        group by
            install_date,
            customer_user_id
    '''

    print(sql)
    df = execSql(sql)
    return df

def getSkanData():
    sql = f'''
    '''
    print(sql)
    df = execSql(sql)
    return df

    
# ios 自然量 付费用户数，付费金额 占比 2022-10-01 ~ 2023-04-30

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getAfData(sinceDayStr = '20230101', untilDayStr = '20230808'):
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
                    WHEN event_timestamp - install_timestamp <= 1 * 24 * 3600 AND event_revenue_usd is null THEN appsflyer_id
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
            AND event_name in (
                'af_purchase',
                'install'
            )
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
            ) as non_paid_user_count
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

def getSkanData2(sinceDayStr = '20230101', untilDayStr = '20230808'):
    sql = f'''
        SELECT
            install_date,
            sum(skad_revenue) as total_paid_amount
        FROM
            ods_platform_appsflyer_skad_details
        WHERE
            app_id = 'id1479198816'
            AND event_name = 'af_skad_revenue'
            AND day >= { sinceDayStr }
            AND day <= { untilDayStr }
        GROUP BY
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df



def main1():
    afDf = getAfData(sinceDayStr = '20221001', untilDayStr = '20230501')
    afDf = afDf[
        (afDf['install_date'] >= '2022-10-01')
        & (afDf['install_date'] <= '2023-04-30')
    ]
    afDf.to_csv('/src/data/afData20221001_20230501.csv', index=False)

    # skanDf = getSkanData(sinceDayStr = '20221001', untilDayStr = '20230501')
    # skanDf2 = getSkanData2(sinceDayStr = '20221001', untilDayStr = '20230501')
    # skanDf = pd.merge(skanDf, skanDf2, on='install_date', how='left')
    # skanDf = skanDf[
    #     (skanDf['install_date'] >= '2022-10-01')
    #     & (skanDf['install_date'] <= '2023-04-30')
    # ]
    # skanDf.to_csv('/src/data/skanData20221001_20230501.csv', index=False)

if __name__ == '__main__':
    main1()



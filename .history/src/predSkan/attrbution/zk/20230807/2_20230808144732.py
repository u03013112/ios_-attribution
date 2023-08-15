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
                'af_purchase'
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

def getAfData2(sinceDayStr = '20230101', untilDayStr = '20230808'):
    sql = f'''
        SELECT
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            COUNT(DISTINCT appsflyer_id) as no_paid_user_count
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND event_name in (
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
    # afDf = getAfData(sinceDayStr = '20221001', untilDayStr = '20230501')
    # afDf2 = getAfData2(sinceDayStr = '20221001', untilDayStr = '20230501')
    # afDf = pd.merge(afDf, afDf2, on='install_date', how='right')
    # afDf['no_paid_user_count'] = afDf['no_paid_user_count'] - afDf['paid_user_count']
    # afDf = afDf[
    #     (afDf['install_date'] >= '2022-10-01')
    #     & (afDf['install_date'] <= '2023-04-30')
    # ]
    # afDf.to_csv('/src/data/afData20221001_20230501.csv', index=False)
    
    # skanDf = getSkanData(sinceDayStr = '20221001', untilDayStr = '20230501')
    # skanDf2 = getSkanData2(sinceDayStr = '20221001', untilDayStr = '20230501')
    # skanDf = pd.merge(skanDf, skanDf2, on='install_date', how='left')
    # skanDf = skanDf[
    #     (skanDf['install_date'] >= '2022-10-01')
    #     & (skanDf['install_date'] <= '2023-04-30')
    # ]
    # skanDf.to_csv('/src/data/skanData20221001_20230501.csv', index=False)

    afDf = pd.read_csv('/src/data/afData20221001_20230501.csv')
    skanDf = pd.read_csv('/src/data/main1.csv')

    # 计算af和skan的付费用户数和付费金额占比
    df = afDf.merge(skanDf, on='install_date', how='left',suffixes=('_af','_skan'))

    df['paid_user skan/af'] = df['paid_user_count_skan'] / df['paid_user_count_af']
    df['paid_amount skan/af'] = df['total_paid_amount_skan'] / df['total_paid_amount_af']

    print(df[['install_date','paid_user skan/af','paid_amount skan/af']].corr())

    df['paid_user_count_af7'] = df['paid_user_count_af'].rolling(7).sum()
    df['paid_user_count_skan7'] = df['paid_user_count_skan'].rolling(7).sum()
    df['total_paid_amount_af7'] = df['total_paid_amount_af'].rolling(7).sum()
    df['total_paid_amount_skan7'] = df['total_paid_amount_skan'].rolling(7).sum()

    df['paid_user7 skan/af'] = df['paid_user_count_skan7'] / df['paid_user_count_af7']
    df['paid_amount7 skan/af'] = df['total_paid_amount_skan7'] / df['total_paid_amount_af7']

    print(df[['install_date','paid_user7 skan/af','paid_amount7 skan/af']].corr())

    df.to_csv('/src/data/zk2/afSkanData20221001_20230501.csv', index=False)
    

if __name__ == '__main__':
    main1()



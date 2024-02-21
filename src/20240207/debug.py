# 针对lw skan数值在20240113之后偏高的问题，进行调试

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def debug01():
    dateList = [
        {
            'start':'20240101',
            'end':'20240112'
        },
        {
            'start':'20240113',
            'end':'20240204'
        },
    ]
    for date in dateList:
        sql = f'''
            SELECT
                sum(
                    case
                        when skad_conversion_value in (0,32) then 0
                        else 1
                    end
                ) as pay_install,
                sum(
                    case
                        when skad_conversion_value between 1 and 10 then 1
                        when skad_conversion_value between 33 and 42 then 1
                        else 0
                    end
                ) as low_install,
                sum(
                    case
                        when skad_conversion_value between 11 and 21 then 1
                        when skad_conversion_value between 43 and 53 then 1
                        else 0
                    end
                ) as mid_install,
                sum(
                    case
                        when skad_conversion_value between 22 and 31 then 1
                        when skad_conversion_value between 54 and 63 then 1
                        else 0
                    end
                ) as high_install,
                count(*) as install
            FROM 
                ods_platform_appsflyer_skad_details
            WHERE
                day between '{date['start']}' and '{date['end']}'
                AND app_id = 'id6448786147'
                AND event_name in (
                    'af_skad_install',
                    'af_skad_redownload'
                )
            ;
        '''
        print(date['start'],date['end'])
        df = execSql(sql)
        print(df)

def debug02():
    # sql = '''
    #     select
    #         appsflyer_id,
    #         customer_user_id,
    #         to_char(to_date(install_time,"yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd") as install_date,
    #         sum(
    #             case
    #                 when event_timestamp - install_timestamp <= 24 * 3600
    #             then event_revenue_usd
    #             else 0
    #             end
    #         ) as d1_revenue
    #     from
    #         ods_platform_appsflyer_events
    #     where
    #         day >= '20240101'
    #         and app_id = 'id6448786147'
    #         and event_name = 'af_purchase'
    #     group by
    #         appsflyer_id,customer_user_id,install_date
    #         having
    #         d1_revenue > 753.61
    #     ;
    # '''
    # df = execSql(sql)
    # df.to_csv('/src/data/debug02_af.csv',index=False)

    sql = '''
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
            ) as d1_revenue,
            TO_CHAR(
                from_unixtime(CAST(install_timestamp AS bigint)),"yyyy-mm-dd"
            ) as install_date
        FROM
            rg_bi.ads_lastwar_ios_purchase_adv
        WHERE
            install_day >= '20240101'
        GROUP BY
            game_uid,
            install_date
        having
            d1_revenue > 753.61
        ;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/debug02_bi.csv',index=False)

    afDf = pd.read_csv('/src/data/debug02_af.csv')
    biDf = pd.read_csv('/src/data/debug02_bi.csv')

    df = pd.merge(afDf,biDf,on=['customer_user_id'],how='outer',suffixes=('_af','_bi'))

    df.to_csv('/src/data/debug02.csv',index=False)

if __name__ == '__main__':
    # debug01()
    debug02()
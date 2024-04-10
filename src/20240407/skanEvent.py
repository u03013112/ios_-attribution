# 新打点，af_sdk_update_skan
# 校对，主要方向
# 1、新打点与之前的AF数据差异
# 2、新打点与BI数据差异
# 3、AF数据与BI数据差异
# 4、第2与第3的差异，是否可以通过新打点解决
import base64
import json

import os
import sys
import pandas as pd

sys.path.append('/src')
from src.maxCompute import execSql

# 这个需要多一点时间，观察一下。暂时数据不足。
def getAF24HoursRevenue(startDayStr,endDayStr):
    filename = f'/src/data/af24HoursRevenue_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
            select
                to_char(from_unixtime(cast(install_timestamp as bigint)), 'yyyyMMdd') as install_day,
                sum(
                CASE
                    WHEN event_timestamp - install_timestamp between 0
                    and 24 * 3600 THEN event_revenue_usd
                    ELSE 0
                END
                ) as 24h_revenue_usd
            from ods_platform_appsflyer_events
            where
                zone = 0
                and app = 502
                and app_id = 'id6448786147'
                and day between '{startDayStr}' and '{endDayStr}'
                and event_name = 'af_purchase'
            group by
                install_day
            ;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

def getSKAN24HoursRevenue(startDayStr,endDayStr):
    filename = f'/src/data/skan24HoursRevenue_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
            select
                to_char(from_unixtime(cast(install_timestamp as bigint)), 'yyyyMMdd') as install_day,
                sum(
                CASE
                    WHEN event_timestamp - install_timestamp between 0
                    and 24 * 3600 THEN event_revenue_usd
                    ELSE 0
                END
                ) as 24h_revenue_usd
            from ods_platform_appsflyer_events
            where
                zone = 0
                and app = 502
                and app_id = 'id6448786147'
                and day between '{startDayStr}' and '{endDayStr}'
                and event_name = 'af_sdk_update_skan'
            group by
                install_day
            ;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

def getBI24HoursRevenue(startDayStr,endDayStr):
    filename = f'/src/data/bi24HoursRevenue_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
            select
                install_day,
                COALESCE(
                    SUM(
                        CASE
                            WHEN event_timestamp - install_timestamp between 0
                            and 24 * 3600 THEN revenue_value_usd
                            ELSE 0
                        END
                    ),
                    0
                ) as 24h_revenue_usd
            from ads_lastwar_ios_purchase_adv
            where
                install_day between {startDayStr} and {endDayStr}
            group by
                install_day
            ;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

def main(startDayStr,endDayStr):
    afDf = getAF24HoursRevenue(startDayStr,endDayStr)
    skanDf = getSKAN24HoursRevenue(startDayStr,endDayStr)
    biDf = getBI24HoursRevenue(startDayStr,endDayStr)

    # 过滤 由于需要计算24小时内的数据，所以需要保证数据的完整性，所以安装时间要在endDayStr往前推一天
    # install_day >= startDayStr and install_day <= endDayStr - 1day
    endInstallDayStr = (pd.to_datetime(endDayStr) - pd.Timedelta(days=1)).strftime('%Y%m%d')
    afDf = afDf[(afDf['install_day'] >= startDayStr) & (afDf['install_day'] <= endInstallDayStr)]
    skanDf = skanDf[(skanDf['install_day'] >= startDayStr) & (skanDf['install_day'] <= endInstallDayStr)]
    biDf = biDf[(biDf['install_day'] >= startDayStr) & (biDf['install_day'] <= endInstallDayStr)]

    # 合并数据
    df = pd.merge(afDf, skanDf, how='outer', on='install_day', suffixes=('_af', '_skan'))
    df = pd.merge(df, biDf, how='outer', suffixes=('', '_bi'), on='install_day')

    print(df)



def debug(startDayStr,endDayStr):
    # 找到所有20240408安装的uid的所有付费订单id
    sql = f'''
        SELECT
            A.uid,
            A.order_id
        FROM
        (
            SELECT
                uid,
                order_id
            FROM
                dwb_overseas_allproject_earnings_currency
            WHERE
                app = 502
                AND day = 20240408
                AND platform = 'appiosglobal'
                and uid is not null
                and order_id is not null
        ) AS A
        RIGHT JOIN
        (
            SELECT
                MAX(install_day) AS install_day,
                game_uid AS uid
            FROM
                ads_lastwar_ios_purchase_adv
            WHERE
                install_day = 20240408
                and game_uid is not null
            GROUP BY
                game_uid
        ) AS B
        ON A.uid = B.uid
        WHERE
            A.uid IS NOT NULL
            and B.uid IS NOT NULL
        ;
    '''
    print(sql)
    orderDf = execSql(sql)

    # 找到所有20240408的af_sdk_update_skan
    sql2 = '''
        select
            event_value
        from ods_platform_appsflyer_events
        where
            zone = 0
            and app = 502
            and app_id = 'id6448786147'
            and day = '20240408'
            and event_name = 'af_sdk_update_skan'
        ;
    '''
    print(sql2)
    skanDf = execSql(sql2)
    # skanDf 需要将列event_value解base64，然后解json，再在json中找到'af_order_id'字段
    # 解码base64，解析json，提取'af_order_id'字段
    def decode_and_extract_order_id(value):
        decoded_value = base64.b64decode(value).decode('utf-8')
        json_value = json.loads(decoded_value)
        return json_value.get('af_order_id')
    
    # 在DataFrame的每一行上应用这个函数
    skanDf['order_id'] = skanDf['event_value'].apply(decode_and_extract_order_id)

    orderDf.to_csv('/src/data/20240409debugOrder.csv', index=False)
    skanDf.to_csv('/src/data/20240409debugSkan.csv', index=False)

    df = pd.merge(orderDf, skanDf, how='outer', on='order_id')

    print(df)

    df.to_csv('/src/data/20240409debug.csv', index=False)

def debug2():
       # 找到所有20240408的af_sdk_update_skan
    sql2 = '''
        select
            day,
            event_name,
            event_value
        from ods_platform_appsflyer_events
        where
            zone = 0
            and app = 502
            and app_id = 'id6448786147'
            and day >= '20240407'
            and event_name in ('af_sdk_update_skan','af_purchase')
        ;
    '''
    print(sql2)
    skanDf = execSql(sql2)
    # skanDf 需要将列event_value解base64，然后解json，再在json中找到'af_order_id'字段
    # 解码base64，解析json，提取'af_order_id'字段
    def decode_and_extract_order_id(value):
        decoded_value = base64.b64decode(value).decode('utf-8')
        json_value = json.loads(decoded_value)
        return json_value.get('af_order_id')
    
    # 在DataFrame的每一行上应用这个函数
    skanDf['order_id'] = skanDf['event_value'].apply(decode_and_extract_order_id)

    skanDf.to_csv('/src/data/20240409debug2.csv', index=False)



if __name__ == '__main__':
    # af_sdk_update_skan 打点是20240402开始增加的，为了数据完整，从20240403开始
    startDayStr = '20240403'
    endDayStr = '20240408'

    # main(startDayStr,endDayStr)
    debug(startDayStr,endDayStr)
    # debug2()
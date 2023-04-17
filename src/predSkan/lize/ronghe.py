# 融合归因
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename
# SKAN的安装日期确定

# 1. 读取数据
# 安卓数据，'2022-10-01'~'2023-04-01'
# 每个用户一行，
# 列有 appsflyer_id	install_date	r1usd	r7usd	install_timestamp	last_payment_timestamp	postback_timestamp	media
def loadDataFromMC():
    sql = '''
        WITH user_event_data AS (
            SELECT
                appsflyer_id,
                MIN(install_timestamp) AS install_timestamp,
                MIN(event_timestamp) AS event_timestamp,
                SUM(event_revenue_usd) AS event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND day >= 20221001
                AND day <= 20230410
                AND install_time >= '2022-10-01'
                AND install_time < '2023-04-01'
                AND event_name = 'af_purchase'
            GROUP BY
                appsflyer_id
        ),
        last_payment_data AS (
            SELECT
                appsflyer_id,
                MAX(event_timestamp) AS last_payment_timestamp
            FROM
                user_event_data
            WHERE
                event_revenue_usd > 0
                AND event_timestamp - install_timestamp <= 24 * 3600
            GROUP BY
                appsflyer_id
        ),
        postback_data AS (
            SELECT
                DISTINCT u.appsflyer_id,
                u.install_timestamp,
                l.last_payment_timestamp,
                CASE
                    WHEN l.last_payment_timestamp IS NOT NULL THEN l.last_payment_timestamp + 24 * 3600 + FLOOR(RANDOM() * (24 * 3600 + 1))
                    ELSE u.install_timestamp + 24 * 3600 + FLOOR(RANDOM() * (24 * 3600 + 1))
                END AS postback_timestamp
            FROM
                user_event_data u
                LEFT JOIN last_payment_data l ON u.appsflyer_id = l.appsflyer_id
        )
        SELECT
            o.appsflyer_id,
            to_char(
                to_date(o.install_time, 'yyyy-mm-dd hh:mi:ss'),
                'yyyy-mm-dd'
            ) AS install_date,
            sum(
                CASE
                    WHEN o.event_timestamp - p.install_timestamp <= 1 * 24 * 3600 THEN CAST(o.event_revenue_usd AS DOUBLE)
                    ELSE 0
                END
            ) AS r1usd,
            sum(
                CASE
                    WHEN o.event_timestamp - p.install_timestamp <= 7 * 24 * 3600 THEN CAST(o.event_revenue_usd AS DOUBLE)
                    ELSE 0
                END
            ) AS r7usd,
            p.install_timestamp,
            p.last_payment_timestamp,
            p.postback_timestamp,
            o.media_source AS media
        FROM
            ods_platform_appsflyer_events o
            JOIN postback_data p ON o.appsflyer_id = p.appsflyer_id
        WHERE
            o.app_id = 'com.topwar.gp'
            AND o.zone = 0
            AND o.day >= 20221001
            AND o.day <= 20230410
            AND o.install_time >= '2022-10-01'
            AND o.install_time < '2023-04-01'
            AND o.event_name = 'af_purchase'
        GROUP BY
            install_date,
            o.appsflyer_id,
            p.install_timestamp,
            p.last_payment_timestamp,
            p.postback_timestamp,
            media
        ;
    '''
    df = execSql(sql)
    return df

# 按照AF方案进行安装日期的推测
# 首日付费金额为0的，用postback_timestamp - 36小时 作为激活时间
# 首日付费金额不为0的，用postback_timestamp - 48小时 作为激活时间
# 并将激活时间转化为'2022-10-01'格式，记作'af_install_date'
# 按照'install_date'和'af_install_date'，分别进行groupby，r1usd与r7usd按分组求和
# 将两个分组的结果合并，按照on 'install_date' = 'af_install_date' 进行join
# 然后计算r1usd_af与r7usd_af，分别为首日付费金额与7日付费金额的MAPE
# def afInstallDateMape(df):
#     def calculate_mape(df, column1, column2):
#         return abs((df[column1] - df[column2]) / df[column1]).mean()

#     df['af_install_date'] = df.apply(
#         lambda row: (pd.to_datetime(row['postback_timestamp'], unit='s') - pd.Timedelta(hours=36 if row['r1usd'] == 0 else 48)).strftime('%Y-%m-%d'),
#         axis=1
#     )
#     print(df.head())

#     grouped_by_install_date = df.groupby('install_date').agg({'r1usd': 'sum', 'r7usd': 'sum'})
#     grouped_by_af_install_date = df.groupby('af_install_date').agg({'r1usd': 'sum', 'r7usd': 'sum'})

#     print(grouped_by_install_date.head())
#     print(grouped_by_af_install_date.head())

#     merged_df = grouped_by_install_date.merge(grouped_by_af_install_date, left_index=True, right_index=True, suffixes=('', '_af'))

#     print(merged_df.head())

#     r1usd_mape = calculate_mape(merged_df, 'r1usd', 'r1usd_af')
#     r7usd_mape = calculate_mape(merged_df, 'r7usd', 'r7usd_af')

#     return r1usd_mape, r7usd_mape

def afInstallDateMape(df):
    def calculate_mape(df, column1, column2):
        df = df[df[column1] != 0]
        return abs((df[column1] - df[column2]) / df[column1]).mean()

    df['af_install_date'] = df.apply(
        lambda row: (pd.to_datetime(row['postback_timestamp'], unit='s') - pd.Timedelta(hours=36 if row['r1usd'] == 0 else 48)).strftime('%Y-%m-%d'),
        axis=1
    )

    # 按安装日期汇总
    grouped_by_install_date = df.groupby('install_date').agg({'r1usd': 'sum', 'r7usd': 'sum'})
    grouped_by_af_install_date = df.groupby('af_install_date').agg({'r1usd': 'sum', 'r7usd': 'sum'})

    merged_df = grouped_by_install_date.merge(grouped_by_af_install_date, left_index=True, right_index=True, suffixes=('', '_af'))

    total_r1usd_mape = calculate_mape(merged_df, 'r1usd', 'r1usd_af')
    total_r7usd_mape = calculate_mape(merged_df, 'r7usd', 'r7usd_af')

    print(f'total r1usd_mape: {total_r1usd_mape} r7usd_mape: {total_r7usd_mape}')

    # 按安装日期+媒体进行汇总
    grouped_by_install_date_media = df.groupby(['install_date', 'media']).agg({'r1usd': 'sum', 'r7usd': 'sum'})
    grouped_by_af_install_date_media = df.groupby(['af_install_date', 'media']).agg({'r1usd': 'sum', 'r7usd': 'sum'})

    merged_df_media = grouped_by_install_date_media.merge(grouped_by_af_install_date_media, left_index=True, right_index=True, suffixes=('', '_af'), how='left')

    # 按安装日期+媒体进行汇总
    grouped_by_install_date_media = df.groupby(['install_date', 'media']).agg({'r1usd': 'sum', 'r7usd': 'sum'}).reset_index()
    grouped_by_af_install_date_media = df.groupby(['af_install_date', 'media']).agg({'r1usd': 'sum', 'r7usd': 'sum'}).reset_index()

    merged_df_media = grouped_by_install_date_media.merge(grouped_by_af_install_date_media, left_on=['install_date', 'media'], right_on=['af_install_date', 'media'], suffixes=('', '_af'), how='left')

    # 计算各个媒体的MAPE
    media_list = df['media'].unique()
    # media_list = ['googleadwords_int','bytedanceglobal_int','Facebook Ads']
    for media in media_list:
        media_df = merged_df_media.loc[merged_df_media.media == media].dropna()
        media_r1usd_mape = calculate_mape(media_df, 'r1usd', 'r1usd_af')
        media_r7usd_mape = calculate_mape(media_df, 'r7usd', 'r7usd_af')
        print(f'{media} r1usd_mape: {media_r1usd_mape} r7usd_mape: {media_r7usd_mape}')

if __name__ == '__main__':
    # df = loadDataFromMC()
    # print(df.head())
    # print(df.shape)
    # print(df.columns)
    # print(df.dtypes)
    # print(df.describe())
    # print(df.isnull().sum())
    # df.to_csv(getFilename('androidUserPostback_20221001_20230401'), index=False)
    
    df = pd.read_csv(getFilename('androidUserPostback_20221001_20230401'))
    afInstallDateMape(df)
    
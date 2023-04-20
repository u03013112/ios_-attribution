# 融合归因
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

from src.predSkan.lize.retCheck2 import makeLevels1,makeCvMap,cvMapFixAvg1,addCv,check
# SKAN的安装日期确定

# 1. 读取数据
# 安卓数据，'2022-01-01'~'2023-04-01'
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
                AND day >= 20220101
                AND day <= 20230410
                AND install_time >= '2022-01-01'
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
            AND o.day >= 20220101
            AND o.day <= 20230410
            AND o.install_time >= '2022-01-01'
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

# 添加档位信息+档位平均金额，用于计算首日MAPE
def addCv1(df,N=32):
    # appsflyer_id 改名为 uid
    df = df.rename(columns={'appsflyer_id':'uid'})

    levels = makeLevels1(df,usd='r1usd',N=N)
    cvMapDf = makeCvMap(levels)
    cvMapDf = cvMapFixAvg1(df,cvMapDf,usd='r1usd')
    cvMapDf.to_csv(getFilename('cvMap%dAndroid_20220101_20230401'%N))
    tmpDf = addCv(df,cvMapDf,usd='r1usd',usdp='r1usdp')
    df = df.merge(tmpDf,how='left',on='uid')
    return df

def afInstallDateMape(df):
    def calculate_mape(df, column1, column2):
        df = df[df[column1] != 0]
        return abs((df[column1] - df[column2]) / df[column1]).mean()

    df['af_install_date'] = df.apply(
        lambda row: (pd.to_datetime(row['postback_timestamp'], unit='s') - pd.Timedelta(hours=36 if row['r1usd'] == 0 else 48)).strftime('%Y-%m-%d'),
        axis=1
    )

    # 按安装日期汇总
    grouped_by_install_date = df.groupby('install_date').agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'})
    grouped_by_af_install_date = df.groupby('af_install_date').agg({'r1usd': 'sum', 'r1usdp':'sum','r7usd': 'sum'})

    merged_df = grouped_by_install_date.merge(grouped_by_af_install_date, left_index=True, right_index=True, suffixes=('', '_af'))

    total_r1usd_mape = calculate_mape(merged_df, 'r1usd', 'r1usdp_af')
    total_r7usd_mape = calculate_mape(merged_df, 'r7usd', 'r7usd_af')

    print(f'total r1usd_mape: {total_r1usd_mape} r7usd_mape: {total_r7usd_mape}')

    # 按安装日期+媒体进行汇总
    grouped_by_install_date_media = df.groupby(['install_date', 'media']).agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'}).reset_index()
    grouped_by_af_install_date_media = df.groupby(['af_install_date', 'media']).agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'}).reset_index()

    merged_df_media = grouped_by_install_date_media.merge(grouped_by_af_install_date_media, left_on=['install_date', 'media'], right_on=['af_install_date', 'media'], suffixes=('', '_af'), how='left')

    # 计算各个媒体的MAPE
    media_list = df['media'].unique()
    # media_list = ['googleadwords_int','bytedanceglobal_int','Facebook Ads']
    for media in media_list:
        media_df = merged_df_media.loc[merged_df_media.media == media].dropna()
        media_r1usd_mape = calculate_mape(media_df, 'r1usd', 'r1usdp_af')
        media_r7usd_mape = calculate_mape(media_df, 'r7usd', 'r7usd_af')
        print(f'{media} r1usd_mape: {media_r1usd_mape} r7usd_mape: {media_r7usd_mape}')

# 与上面afInstallDateMape后面计算完全一致，主要区别是af_install_date的计算方式不同
# 上面是固定的按照用户首日是否付费，用postback_timestamp - 36小时或者48小时计算获得
# 此方法假定我们用了CV中的1Bit记录了激活日期是从1970年1月1日到激活日的天数为奇数还是偶数。
# 即df中的install_timestamp//86400为奇数时，该用户的激活日属性为1，为偶数时，该用户的激活日属性为0。
# 当用户首日付费金额为0时，用户的激活时间范围是 (postback_timestamp - 48*3600) ~ (postback_timestamp - 24*3600)
# 当用户首日付费金额不为0时，用户的激活时间范围是 (postback_timestamp - 72*3600) ~ (postback_timestamp - 24*3600)
# 在激活时间范围内，再根据用户激活日属性进行过滤。如果过滤后只有1天，则取这1天作为用户的安装日期。
# 如果过滤后有两天，则选择时间较长的那天作为安装日期。过滤后有A日的23：00~23:59和B日的0:00~23:00，取B日作为安装日期。
def f1InstallDateMape(df):
    def calculate_mape(df, column1, column2):
        df = df[df[column1] != 0]
        return abs((df[column1] - df[column2]) / df[column1]).mean()

    def get_install_date(row):
        postback_timestamp = pd.to_datetime(row['postback_timestamp'], unit='s')
        install_range_start = postback_timestamp - pd.Timedelta(hours=72 if row['r1usd'] != 0 else 48)
        install_range_end = postback_timestamp - pd.Timedelta(hours=24)
        install_range = pd.date_range(install_range_start, install_range_end, freq='D')

        activation_day_parity = row['install_timestamp'] // 86400 % 2

        filtered_dates = [date for date in install_range if (date - pd.Timestamp("1970-01-01")).days % 2 == activation_day_parity]

        if len(filtered_dates) == 1:
            return filtered_dates[0].strftime('%Y-%m-%d')
        elif len(filtered_dates) == 2:
            date1_duration = (postback_timestamp - filtered_dates[0]).total_seconds()
            date2_duration = (postback_timestamp - filtered_dates[1]).total_seconds()
            if date1_duration > date2_duration:
                return filtered_dates[0].strftime('%Y-%m-%d')
            else:
                return filtered_dates[1].strftime('%Y-%m-%d')
        else:
            print('不该出现这种情况')

    df['af_install_date'] = df.apply(get_install_date, axis=1)

    print(df.head(10))

    # 按安装日期汇总
    grouped_by_install_date = df.groupby('install_date').agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'})
    grouped_by_af_install_date = df.groupby('af_install_date').agg({'r1usd': 'sum', 'r1usdp':'sum','r7usd': 'sum'})

    merged_df = grouped_by_install_date.merge(grouped_by_af_install_date, left_index=True, right_index=True, suffixes=('', '_af'))

    total_r1usd_mape = calculate_mape(merged_df, 'r1usd', 'r1usdp_af')
    total_r7usd_mape = calculate_mape(merged_df, 'r7usd', 'r7usd_af')

    print(f'total r1usd_mape: {total_r1usd_mape} r7usd_mape: {total_r7usd_mape}')

    # 按安装日期+媒体进行汇总
    grouped_by_install_date_media = df.groupby(['install_date', 'media']).agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'}).reset_index()
    grouped_by_af_install_date_media = df.groupby(['af_install_date', 'media']).agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'}).reset_index()

    merged_df_media = grouped_by_install_date_media.merge(grouped_by_af_install_date_media, left_on=['install_date', 'media'], right_on=['af_install_date', 'media'], suffixes=('', '_af'), how='left')

    # 计算各个媒体的MAPE
    media_list = df['media'].unique()
    # media_list = ['googleadwords_int','bytedanceglobal_int','Facebook Ads']
    for media in media_list:
        media_df = merged_df_media.loc[merged_df_media.media == media].dropna()
        media_r1usd_mape = calculate_mape(media_df, 'r1usd', 'r1usdp_af')
        media_r7usd_mape = calculate_mape(media_df, 'r7usd', 'r7usd_af')
        print(f'{media} r1usd_mape: {media_r1usd_mape} r7usd_mape: {media_r7usd_mape}')

def f2InstallDateMape(df):
    def calculate_mape(df, column1, column2):
        df = df[df[column1] != 0]
        return abs((df[column1] - df[column2]) / df[column1]).mean()

    def get_install_date(row):
        postback_timestamp = pd.to_datetime(row['postback_timestamp'], unit='s')
        install_range_start = postback_timestamp - pd.Timedelta(hours=72 if row['r1usd'] != 0 else 48)
        install_range_end = postback_timestamp - pd.Timedelta(hours=24)
        install_range = pd.date_range(install_range_start, install_range_end, freq='D')

        activation_day_parity = row['install_timestamp'] // 86400 % 3

        filtered_dates = [date for date in install_range if (date - pd.Timestamp("1970-01-01")).days % 3 == activation_day_parity]

        if len(filtered_dates) == 1:
            return filtered_dates[0].strftime('%Y-%m-%d')
        else:
            print('不该出现这种情况')
    
    df['af_install_date'] = df.apply(get_install_date, axis=1)

    print(df.head(10))

    # 按安装日期汇总
    grouped_by_install_date = df.groupby('install_date').agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'})
    grouped_by_af_install_date = df.groupby('af_install_date').agg({'r1usd': 'sum', 'r1usdp':'sum','r7usd': 'sum'})

    merged_df = grouped_by_install_date.merge(grouped_by_af_install_date, left_index=True, right_index=True, suffixes=('', '_af'))

    total_r1usd_mape = calculate_mape(merged_df, 'r1usd', 'r1usdp_af')
    total_r7usd_mape = calculate_mape(merged_df, 'r7usd', 'r7usd_af')

    print(f'total r1usd_mape: {total_r1usd_mape} r7usd_mape: {total_r7usd_mape}')

    # 按安装日期+媒体进行汇总
    grouped_by_install_date_media = df.groupby(['install_date', 'media']).agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'}).reset_index()
    grouped_by_af_install_date_media = df.groupby(['af_install_date', 'media']).agg({'r1usd': 'sum','r1usdp':'sum', 'r7usd': 'sum'}).reset_index()

    merged_df_media = grouped_by_install_date_media.merge(grouped_by_af_install_date_media, left_on=['install_date', 'media'], right_on=['af_install_date', 'media'], suffixes=('', '_af'), how='left')

    # 计算各个媒体的MAPE
    media_list = df['media'].unique()
    # media_list = ['googleadwords_int','bytedanceglobal_int','Facebook Ads']
    for media in media_list:
        media_df = merged_df_media.loc[merged_df_media.media == media].dropna()
        media_r1usd_mape = calculate_mape(media_df, 'r1usd', 'r1usdp_af')
        media_r7usd_mape = calculate_mape(media_df, 'r7usd', 'r7usd_af')
        print(f'{media} r1usd_mape: {media_r1usd_mape} r7usd_mape: {media_r7usd_mape}')

if __name__ == '__main__':
    df = loadDataFromMC()
    print(df.head())
    print(df.shape)
    print(df.columns)
    print(df.dtypes)
    print(df.describe())
    print(df.isnull().sum())
    df.to_csv(getFilename('androidUserPostback_20220101_20230401'), index=False)
    
    df = pd.read_csv(getFilename('androidUserPostback_20220101_20230401'))
    # df = addCv1(df)
    # print(df.head(10))
    # afInstallDateMape(df)
    # f1InstallDateMape(df)
    df = addCv1(df,N = 16)
    f2InstallDateMape(df)
    
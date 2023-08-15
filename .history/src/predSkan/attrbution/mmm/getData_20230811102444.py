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
            install_day,
            media_source,
            campaign,
            campaign_id,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(installs) as installs,
            sum(cost) as cost
        from
            ods_platform_appsflyer_masters
        where
            app_id = 'com.topwar.gp.vn'
            AND app = '102'
            and day >= 20230101
            and day < 20230701
        group by
            install_day,
            media_source,
            campaign,
            campaign_id
    ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv('/src/data/zk2/media_20230101_20230701.csv',index=False)
    return df

def main():
    # df1 = pd.read_csv('/src/data/zk2/VnR7Usd.csv', dtype={'campaign_id':str})
    # df2 = pd.read_csv('/src/data/zk2/media_20230101_20230701.csv', dtype={'campaign_id':str})
    # # 将df2的install_day转换为install_date，格式为yyyymmdd -> yyyy-mm-dd
    # df2['install_date'] = pd.to_datetime(df2['install_day'], format='%Y%m%d')
    # df2['install_date'] = df2['install_date'].dt.strftime('%Y-%m-%d')
    # df2 = df2.drop(columns=['install_day'])

    # df = pd.merge(df1, df2, how='left', on=['install_date','media_source','campaign','campaign_id'])
    # df.to_csv('/src/data/zk2/VnR7UsdMedia.csv', index=False)

    df = pd.read_csv('/src/data/zk2/VnR7UsdMedia.csv')
    # 将media_source中的restricted -> Facebook Ads
    df.loc[df['media_source']=='restricted', 'media_source'] = 'Facebook Ads'
    # 再按照install_date,media_source,campaign,campaign_id分组
    df = df.groupby(['install_date','media_source','campaign','campaign_id']).sum().reset_index()

    # 广告分类
    # 添加一列 ‘ad type’，默认是‘unknown’
    df['ad type'] = 'unknown'
    # media_source = ‘googleadwords_int’ 的广告，ad type = ‘GG UAC’
    df.loc[df['media_source']=='googleadwords_int', 'ad type'] = 'GG UAC'
    # media_source = ‘Facebook Ads’ 的广告，campaign 包含 ‘AAA’ 的，ad type = ‘FB AAA’
    df.loc[
        (df['media_source']=='Facebook Ads')
        & (df['campaign'].str.contains('AAA'))
    , 'ad type'] = 'FB AAA'
    # media_source = ‘Facebook Ads’ 的广告，campaign 包含 ‘BAU’ 的，ad type = ‘FB BAU’
    df.loc[
        (df['media_source']=='Facebook Ads')
        & (df['campaign'].str.contains('BAU'))
    , 'ad type'] = 'FB BAU'
    # media_source = ‘bytedanceglobal_int’ 的广告，campaign 包含 ‘VO’，ad type = ‘TT VO’
    df.loc[
        (df['media_source']=='bytedanceglobal_int')
        & (df['campaign'].str.contains('VO'))
    , 'ad type'] = 'TT VO'
    # media_source = ‘bytedanceglobal_int’ 的广告，campaign 包含 ‘AEO’，ad type = ‘TT AEO’
    df.loc[
        (df['media_source']=='bytedanceglobal_int')
        & (df['campaign'].str.contains('AEO'))
    , 'ad type'] = 'TT AEO'

    adTypeList = df['ad type'].unique()
    for adType in adTypeList:
        print(adType)
        adTypeDf = df[df['ad type']==adType]
        adTypeDf['campaign_id'] = adTypeDf['campaign_id'].astype(str)
        campaignIdList = adTypeDf['campaign_id'].unique()
        print(campaignIdList)
        adTypeDf = adTypeDf.drop(columns=['media_source','campaign','campaign_id'])
        adTypeDf = adTypeDf.groupby(['install_date']).sum().reset_index()
        adTypeDf = adTypeDf.sort_values(by=['install_date'])
        # print(adTypeDf.head(10))
        print(adTypeDf.corr())
        print('\n')
        # 计算rolling 7 days的相关系数
        adTypeDf['r7usd7'] = adTypeDf['r7usd'].rolling(7).sum()
        adTypeDf['impressions7'] = adTypeDf['impressions'].rolling(7).sum()
        adTypeDf['clicks7'] = adTypeDf['clicks'].rolling(7).sum()
        adTypeDf['installs7'] = adTypeDf['installs'].rolling(7).sum()
        adTypeDf['cost7'] = adTypeDf['cost'].rolling(7).sum()
        print('rolling 7 days')
        print(adTypeDf[['install_date','r7usd7','impressions7','clicks7','installs7','cost7']].corr())


if __name__ == '__main__':
    # getVnR7Usd()
    # getMediaData()
    main()
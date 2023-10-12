import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def getDataFromMC():
    # 获得用户信息，这里要额外获得归因信息，精确到campaign
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
                campaign_id
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND event_name = 'install'
                AND day BETWEEN '20220101'
                AND '20221231'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-01-01', "yyyy-mm-dd")
                AND to_date('2022-12-31', "yyyy-mm-dd")
        ),
        purchases AS (
            SELECT
                appsflyer_id AS uid,
                event_timestamp,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                event_name in ('af_purchase_oldusers','af_purchase')
                AND zone = 0
                AND day BETWEEN '20220101'
                AND '20221231'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-01-01', "yyyy-mm-dd")
                AND to_date('2022-12-31', "yyyy-mm-dd")
        )
        SELECT
            installs.uid,
            installs.install_date,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 86400
                ),
                0
            ) AS r1usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 2 * 86400
                ),
                0
            ) AS r2usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 3 * 86400
                ),
                0
            ) AS r3usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 7 * 86400
                ),
                0
            ) AS r7usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 14 * 86400
                ),
                0
            ) AS r14usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 28 * 86400
                ),
                0
            ) AS r28usd,
            installs.install_timestamp,
            COALESCE(
                max(purchases.event_timestamp) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 1 * 86400
                ),
                0
            ) AS last_timestamp,
            installs.media_source,
            installs.country_code,
            installs.campaign_id
        FROM
            installs
            LEFT JOIN purchases ON installs.uid = purchases.uid
        GROUP BY
            installs.uid,
            installs.install_date,
            installs.install_timestamp,
            installs.media_source,
            installs.country_code,
            installs.campaign_id
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('androidFp07_2022'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp07_2022'))
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 media 中 'restricted' 改为 'Facebook Ads'
    df['media'] = df['media'].replace('restricted','Facebook Ads')

    return df

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
def debug2022():
    print('2022:')
    df = loadData()
    mediaList = [
        'Facebook Ads',
        'bytedanceglobal_int',
        'googleadwords_int',
        # 'other'
    ]
    df.loc[~df['media'].isin(mediaList), 'media'] = 'other'

    df = df.groupby(['media','install_date']).agg({
        'r3usd':'sum',
        'r7usd':'sum'
    }).reset_index()

    for media in mediaList:
        mediaDf = df[df['media']==media].copy()
        print(media)
        mediaDf['r7usd/r3usd'] = mediaDf['r7usd']/mediaDf['r3usd']
        mediaDf['r3usd rolling7'] = mediaDf['r3usd'].rolling(7).mean()
        mediaDf['r7usd rolling7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usd/r3usd rolling7'] = mediaDf['r7usd rolling7']/mediaDf['r3usd rolling7']

        mediaDf['install_date'] = pd.to_datetime(mediaDf['install_date'])
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(mediaDf['install_date'], mediaDf['r7usd/r3usd'], label='r7usd/r3usd',alpha=0.5)
        ax.plot(mediaDf['install_date'], mediaDf['r7usd/r3usd rolling7'], label='r7usd/r3usd rolling7')
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))  # 设置每7天显示一个日期
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45)
        plt.xlabel('Install Date')
        plt.ylabel('Values')
        plt.title(f'{media} - r7usd/r3usd')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f'/src/data/zk2/20230908_data2022_{media}.jpg')
        plt.close()
        
        a = mediaDf['r7usd/r3usd']
        print('min:',a.min())
        print('max:',a.max())
        print('mean:',a.mean())
        # 打印标准差，方差
        print('std:',a.std())
        print('var:',a.var())

def debug2023():
    print('2023:')
    # 加载数据
    df = pd.read_csv(getFilename('androidFp07_28'))
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 media 中 'restricted' 改为 'Facebook Ads'
    df['media'] = df['media'].replace('restricted','Facebook Ads')

    mediaList = [
        'Facebook Ads',
        'bytedanceglobal_int',
        'googleadwords_int',
        # 'other'
    ]
    df.loc[~df['media'].isin(mediaList), 'media'] = 'other'

    df = df.groupby(['media','install_date']).agg({
        'r3usd':'sum',
        'r7usd':'sum'
    }).reset_index()

    for media in mediaList:
        mediaDf = df[df['media']==media].copy()
        print(media)
        mediaDf['r7usd/r3usd'] = mediaDf['r7usd']/mediaDf['r3usd']
        mediaDf['r3usd rolling7'] = mediaDf['r3usd'].rolling(7).mean()
        mediaDf['r7usd rolling7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usd/r3usd rolling7'] = mediaDf['r7usd rolling7']/mediaDf['r3usd rolling7']

        mediaDf['install_date'] = pd.to_datetime(mediaDf['install_date'])
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(mediaDf['install_date'], mediaDf['r7usd/r3usd'], label='r7usd/r3usd',alpha=0.5)
        ax.plot(mediaDf['install_date'], mediaDf['r7usd/r3usd rolling7'], label='r7usd/r3usd rolling7')
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))  # 设置每7天显示一个日期
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45)
        plt.xlabel('Install Date')
        plt.ylabel('Values')
        plt.title(f'{media} - r7usd/r3usd')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f'/src/data/zk2/20230908_data2023_{media}.jpg')
        plt.close()
        
        a = mediaDf['r7usd/r3usd']
        print('min:',a.min())
        print('max:',a.max())
        print('mean:',a.mean())
        # 打印标准差，方差
        print('std:',a.std())
        print('var:',a.var())


if __name__ == '__main__':
    # getDataFromMC()
    debug2022()
    debug2023()
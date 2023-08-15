# 验算
import pandas as pd


# mmm数据选取安卓，2023-05-01~2023-05-31
# 2023年7月8日版本
# 暂时只比大盘和媒体这两个层级
# 数据存储在mmm/mmmAos202305.csv中

# AF数据选取安卓，2023-05-01~2023-05-31
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getAFDataFromMC():
    sql = '''
        WITH installs AS (
            SELECT
                customer_user_id AS uid,
                to_char(
                    to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                    "yyyy-mm-dd"
                ) AS install_date,
                install_timestamp,
                media_source,
                country_code
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND event_name = 'install'
                AND day BETWEEN '20230501'
                AND '20230608'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-05-01', "yyyy-mm-dd")
                AND to_date('2023-05-31', "yyyy-mm-dd")
        ),
        purchases AS (
            SELECT
                customer_user_id AS uid,
                install_time,
                event_timestamp,
                event_time,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND event_name = 'af_purchase'
                AND day BETWEEN '20230501'
                AND '20230608'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-05-01', "yyyy-mm-dd")
                AND to_date('2023-05-31', "yyyy-mm-dd")
        )
        SELECT
            installs.install_date,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 86400
                ),
                0
            ) AS r24hours_usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        SUBSTRING(purchases.event_time, 1, 10) = SUBSTRING(purchases.install_time, 1, 10)
                ),
                0
            ) AS r1usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 7 * 86400
                ),
                0
            ) AS r7usd,
            installs.media_source
        FROM
            installs
            LEFT JOIN purchases ON installs.uid = purchases.uid
        GROUP BY
            installs.install_date,
            installs.media_source;
    '''
    df = execSql(sql)
    df.to_csv('/src/data/androidAFMedia202305.csv', index=False)
    return df

import matplotlib.pyplot as plt
def main():
    afDf = pd.read_csv('/src/data/androidAFMedia202305.csv')
    mmmDf = pd.read_csv('/src/src/predSkan/mmm/mmmAos202305.csv')
    mmmDf = mmmDf[mmmDf['Platform'] == 'Android']
    mmmDf = mmmDf[['Channel','Date','Cohort IAP Revenue D0','Cohort IAP Revenue D6','Sum Spend']]
    # Date 列 原来格式 类似 "2023-05-14 00:00:00" str，直接字符串截取，变成 "2023-05-14"
    mmmDf['Date'] = mmmDf['Date'].str.slice(0,10)
    # Channel 列 内容替换
    mmmDf.loc[mmmDf['Channel'] == 'googleadwords', 'Channel'] = 'googleadwords_int'
    mmmDf.loc[mmmDf['Channel'] == 'bytedanceglobal', 'Channel'] = 'bytedanceglobal_int'
    mmmDf.rename(columns={
        'Channel':'media_source',
        'Date':'install_date',
        'Cohort IAP Revenue D0':'r1usd',
        'Cohort IAP Revenue D6':'r7usd',
        'Sum Spend':'cost'
    }, inplace=True)
    
    # 先进行大盘对比，即忽略媒体，只按照安装日期进行对比
    afTotalDf = afDf.groupby(['install_date']).sum().reset_index()
    mmmTotalDf = mmmDf.groupby(['install_date']).sum().reset_index()
    totalDf = afTotalDf.merge(mmmTotalDf, how='left', on=['install_date'], suffixes=('_af', '_mmm'))
    
    totalDf['mape1'] = (totalDf['r1usd_af'] - totalDf['r1usd_mmm']) / totalDf['r1usd_mmm']
    totalDf['mape1'] = totalDf['mape1'].abs()
    print('大盘MAPE1：',totalDf['mape1'].mean())
    print('大盘总金额比例1（AF/MMM)：',totalDf['r1usd_af'].sum()/totalDf['r1usd_mmm'].sum())
    
    totalDf['mape7'] = (totalDf['r7usd_af'] - totalDf['r7usd_mmm']) / totalDf['r7usd_mmm']
    totalDf['mape7'] = totalDf['mape7'].abs()
    print('大盘MAPE7：',totalDf['mape7'].mean())
    print('大盘总金额比例7（AF/MMM)：',totalDf['r7usd_af'].sum()/totalDf['r7usd_mmm'].sum())

    totalDf.to_csv('/src/data/zk2/afVsMmmTotal.csv', index=False)

    totalDf.plot(x='install_date', y=['r1usd_af', 'r1usd_mmm'], kind='line')
    plt.title('Total R1usd Comparison')
    plt.savefig('/src/data/zk2/afVsMmmTotalR1usd.png')

    totalDf.plot(x='install_date', y=['r7usd_af', 'r7usd_mmm'], kind='line')
    plt.title('Total R7usd Comparison')
    plt.savefig('/src/data/zk2/afVsMmmTotalR7usd.png')

    # 对比3个主要媒体，即Facebook Ads,googleadwords_int,bytedanceglobal_int
    # afDf 需要将media_source列中的restricted replace 为 Facebook Ads
    # afDf.loc[afDf['media_source'] == 'restricted', 'media_source'] = 'Facebook Ads'

    for media in ['Facebook Ads','googleadwords_int','bytedanceglobal_int']:
        afMediaDf = afDf[afDf['media_source'] == media]
        mmmMediaDf = mmmDf[mmmDf['media_source'] == media]
        mediaDf = afMediaDf.merge(mmmMediaDf, how='left', on=['install_date','media_source'], suffixes=('_af', '_mmm'))


        mediaDf['mape1'] = (mediaDf['r1usd_af'] - mediaDf['r1usd_mmm']) / mediaDf['r1usd_mmm']
        mediaDf['mape1'] = mediaDf['mape1'].abs()
        print(media,'MAPE1：',mediaDf['mape1'].mean())
        print(media,'总金额比例1（AF/MMM)：',mediaDf['r1usd_af'].sum()/mediaDf['r1usd_mmm'].sum())

        mediaDf['mape7'] = (mediaDf['r7usd_af'] - mediaDf['r7usd_mmm']) / mediaDf['r7usd_mmm']
        mediaDf['mape7'] = mediaDf['mape7'].abs()
        print(media,'MAPE7：',mediaDf['mape7'].mean())
        print(media,'总金额比例7（AF/MMM)：',mediaDf['r7usd_af'].sum()/mediaDf['r7usd_mmm'].sum())

        mediaDf.to_csv(f'/src/data/zk2/afVsMmm{media}.csv', index=False)

        mediaDf.plot(x='install_date', y=['r1usd_af', 'r1usd_mmm'], kind='line')
        plt.title(f'{media} R1usd Comparison')
        plt.savefig(f'/src/data/zk2/afVsMmm{media}TotalR1usd.png')

        mediaDf.plot(x='install_date', y=['r7usd_af', 'r7usd_mmm'], kind='line')
        plt.title(f'{media} R7usd Comparison')
        plt.savefig(f'/src/data/zk2/afVsMmm{media}TotalR7usd.png')


if __name__ == '__main__':
    # getAFDataFromMC()
    main()
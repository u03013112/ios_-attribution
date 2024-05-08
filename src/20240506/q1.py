# 融合归因分媒体中，skan失败（人数，金额）率。
import pandas as pd
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getDiffData2(startDayStr,endDayStr):
    filename1 = f'/src/data/zk/q1f1_2_{startDayStr}_{endDayStr}.csv'
    filename2 = f'/src/data/zk/q1f2_2_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename1):    
        sql1 = f'''
            select
                day,
                media,
                cv,
                sum(count) as count,
                sum(usd) as usd
            from lastwar_ios_rh_skan_raw
            where
                day between '{startDayStr}' and '{endDayStr}'
            group by
                day,
                media,
                cv
            ;
        '''
        print(sql1)
        df1 = execSql(sql1)
        df1.to_csv(filename1, index=False)
    else:
        print('read from file:',filename1)
        df1 = pd.read_csv(filename1)

    if not os.path.exists(filename2):    
        sql2 = f'''
            select
                day,
                media,
                cv,
                sum(count) as count,
                sum(usd) as usd
            from lastwar_ios_rh_skan_raw_failed
            where
                day between '{startDayStr}' and '{endDayStr}'
            group by
                day,
                media,
                cv
            ;
        '''
        print(sql2)
        df2 = execSql(sql2)
        df2.to_csv(filename2, index=False)
    else:
        print('read from file:',filename2)
        df2 = pd.read_csv(filename2)

    df = pd.merge(df1, df2, how='outer',on=['media','cv','day'], suffixes=('_skan', '_failed')).reindex()
    df = df.fillna(0)
    # df.to_csv('/src/data/zk/q1f3.csv', index=False)

    # 整体统计，失败的count占比，失败的usd占比
    print('整体统计，失败的count占比，失败的usd占比')
    count_failed_rate = df['count_failed'].sum() / df['count_skan'].sum()
    usd_failed_rate = df['usd_failed'].sum() / df['usd_skan'].sum()
    print(count_failed_rate,usd_failed_rate)

    # 按天统计，失败的count占比，失败的usd占比
    print('按天统计，失败的count占比，失败的usd占比')
    groupByDayDf = df.groupby(['day']).agg('sum').reset_index()
    groupByDayDf['count_failed_rate'] = groupByDayDf['count_failed'] / groupByDayDf['count_skan']
    groupByDayDf['usd_failed_rate'] = groupByDayDf['usd_failed'] / groupByDayDf['usd_skan']
    print(groupByDayDf[['day','count_failed_rate','usd_failed_rate']])

    # # 分媒体统计，失败的count占比，失败的usd占比
    # print('分媒体统计，失败的count占比，失败的usd占比')
    # groupByMediaDf = df.groupby(['media']).agg('sum').reset_index()
    # groupByMediaDf['count_failed_rate'] = groupByMediaDf['count_failed'] / groupByMediaDf['count_skan']
    # groupByMediaDf['usd_failed_rate'] = groupByMediaDf['usd_failed'] / groupByMediaDf['usd_skan']
    # print(groupByMediaDf[['media','count_failed_rate','usd_failed_rate']])
    

# 融合归因分媒体中，自然量人数占比，自然量金额占比
def getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr):
    filename = f'/src/data/lwRevenueMedia_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename, dtype={'install_date':str})
    else:
        print('从MC获得数据')


        sql = f'''
    SET
        odps.sql.timezone = Africa / Accra;

    set
        odps.sql.hive.compatible = true;

    set
        odps.sql.executionengine.enable.rand.time.seed = true;

    @rhData :=
    select
        customer_user_id,
        media,
        rate
    from
        lastwar_ios_funplus02_adv_uid_mutidays_media
    where
        day between '{startDayStr}' and '{endDayStr}';

    @biData :=
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
        ) as r1usd,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 3 * 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r3usd,
        COALESCE(
            SUM(
                CASE
                    WHEN event_timestamp <= install_timestamp + 7 * 24 * 3600 THEN revenue_value_usd
                    ELSE 0
                END
            ),
            0
        ) as r7usd,
        install_day as install_date,
        country as country_code
    FROM
        rg_bi.ads_lastwar_ios_purchase_adv
    WHERE
        game_uid IS NOT NULL
    GROUP BY
        game_uid,
        install_day,
        country;

    @biData2 :=
    select
        customer_user_id,
        r1usd,
        r3usd,
        r7usd,
        CASE
            WHEN r1usd = 0 THEN 'free'
            WHEN r1usd > 0
            AND r1usd <= 10 THEN 'low'
            WHEN r1usd > 10
            AND r1usd <= 80 THEN 'mid'
            ELSE 'high'
        END as paylevel,
        install_date,
        country_code
    from
        @biData;

    select
        rh.media,
        sum(bi.r1usd * rh.rate) as r1usd,
        sum(bi.r3usd * rh.rate) as r3usd,
        sum(bi.r7usd * rh.rate) as r7usd,
        bi.paylevel,
        bi.install_date,
        bi.country_code,
        sum(rh.rate) as installs
    from
        @rhData as rh
        left join @biData2 as bi on rh.customer_user_id = bi.customer_user_id
    group by
        rh.media,
        bi.install_date,
        bi.country_code,
        bi.paylevel
    ;
        '''
        print(sql)
        df = execSql(sql)

        df.to_csv(filename,index=False)
    
    return df

  

if __name__ == '__main__':
    getDiffData2('20240415','20240503')


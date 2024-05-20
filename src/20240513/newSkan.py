import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getLwAdCost(startDayStr,endDayStr):
    filename = f'/src/data/lwAdCost_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    install_day,
    mediasource,
    sum(
    cost_value_usd
    ) as cost
from 
    dwd_overseas_cost_allproject
where
    app = '502'
    AND app_package = 'id6448786147'
    AND cost_value_usd > 0
    AND facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    mediasource
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

def getLwRevenue(startDayStr,endDayStr):
    filename = f'/src/data/lwRevenue_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
SELECT
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
    install_day
FROM
    rg_bi.ads_lastwar_ios_purchase_adv
WHERE
    install_day between {startDayStr} and {endDayStr}
GROUP BY
    install_day
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

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

# 自然量占比变化
def oganicRateDiff(startDayStr = '20240501',endDayStr = '20240513'):
    totalRevenue = getLwRevenue(startDayStr,endDayStr)
    mediaRevenue = getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr)

    mediaRevenue.rename(columns={'install_date':'install_day'},inplace=True)

    # 计算自然量收入占比
    mediaRevenueGroupByDate = mediaRevenue.groupby('install_day').agg({'r1usd':'sum'}).reset_index()
    df = pd.merge(totalRevenue,mediaRevenueGroupByDate,on='install_day',how='left',suffixes=('_total','_media'))

    df = df[['install_day','r1usd_total','r1usd_media']]
    df['r1usd_oganic'] = df['r1usd_total'] - df['r1usd_media']
    df['r1usd_oganic / r1usd_total'] = df['r1usd_oganic'] / df['r1usd_total']

    df = df.sort_values('install_day')

    print(df)
    df.to_csv(f'/src/data/20240516_oganicRateDiff_{startDayStr}_{endDayStr}.csv',index=False)

# 媒体ROI变化
def mediaRoiDiff(startDayStr = '20240501',endDayStr = '20240513'):
    lwAdCost = getLwAdCost(startDayStr,endDayStr)
    mediaRevenue = getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr)

    mediaRevenue.rename(columns={'install_date':'install_day'},inplace=True)
    mediaRevenue = mediaRevenue.groupby(['install_day','media']).agg({'r1usd':'sum'}).reset_index()

    print('lwAdCost mediaList:',lwAdCost['mediasource'].unique())
    print('mediaRevenue mediaList:',mediaRevenue['media'].unique())
    
    # mediaRevenue media中字段与lwAdCost mediasource不一致，改为统一字段
    mediaRevenue.rename(columns={'media':'mediasource'},inplace=True)
    mediaRevenue.replace(
        {'mediasource':
            {
                'Facebook':'Facebook Ads',
                'Google':'googleadwords_int',
                'tiktokglobal_int':'bytedanceglobal_int',
                'twitter':'Twitter',
            }
        },inplace=True)

    df = pd.merge(lwAdCost,mediaRevenue,on=['install_day','mediasource'],how='left')
    df['roi'] = df['r1usd'] / df['cost']

    df = df[['install_day','mediasource','roi']]

    pivot_df = df.pivot(index='install_day', columns='mediasource', values='roi')
    pivot_df.columns = [f'{col} roi' for col in pivot_df.columns]

    print(pivot_df)
    pivot_df.to_csv(f'/src/data/20240516_mediaRoiDiff_{startDayStr}_{endDayStr}.csv',index=True)

# 各媒体付费等级分布变化
def mediaPayLevelDiff(startDayStr = '20240501',endDayStr = '20240513'):
    mediaRevenue = getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr)


    mediaRevenue.rename(columns={'install_date':'install_day'},inplace=True)
    mediaRevenue = mediaRevenue.groupby(['install_day','media','paylevel']).agg({'r1usd':'sum','installs':'sum'}).reset_index()

    mediaList = mediaRevenue['media'].unique()
    
    for media in mediaList:
        print('media:',media)
        mediaDf = mediaRevenue[mediaRevenue['media'] == media].copy()
        # 按照install_day分组，计算各paylevel的r1usd占比和installs占比
        # 按照install_day分组，计算各paylevel的r1usd占比和installs占比
        r1usd_sum = mediaDf.groupby('install_day')['r1usd'].sum()
        installs_sum = mediaDf.groupby('install_day')['installs'].sum()

        mediaDf['r1usd_percentage'] = mediaDf.apply(lambda row: row['r1usd'] / r1usd_sum[row['install_day']], axis=1)
        mediaDf['installs_percentage'] = mediaDf.apply(lambda row: row['installs'] / installs_sum[row['install_day']], axis=1)

        # 执行透视操作，将 'paylevel' 列的值转换为列名
        r1usd_pivot = mediaDf.pivot(index='install_day', columns='paylevel', values='r1usd_percentage')
        r1usd_pivot.columns = [f'{col} r1usd_percentage' for col in r1usd_pivot.columns]
        installs_pivot = mediaDf.pivot(index='install_day', columns='paylevel', values='installs_percentage')
        installs_pivot.columns = [f'{col} installs_percentage' for col in installs_pivot.columns]

        # 合并两个透视表
        result = pd.concat([r1usd_pivot, installs_pivot], axis=1)


        # 打印结果
        print(result)
        result.to_csv(f'/src/data/20240516_mediaPayLevelDiff_{startDayStr}_{endDayStr}_{media}.csv',index=True)



if __name__ == '__main__':
    startDayStr = '20240501'
    endDayStr = '20240515'

    oganicRateDiff(startDayStr,endDayStr)
    mediaRoiDiff(startDayStr,endDayStr)
    mediaPayLevelDiff(startDayStr,endDayStr)



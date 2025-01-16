import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import io
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getSKANDataFromMC():
    startDayStr = '20241201'
    endDayStr = '20241231'

    filename = '/src/data/20250107_ska_raw_lastwar.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f'''
SET
    odps.sql.timezone = Africa / Accra;

set
    odps.sql.hive.compatible = true;

set
    odps.sql.executionengine.enable.rand.time.seed = true;

@skad :=
SELECT
    skad_ad_network_id,
    skad_conversion_value as cv,
    timestamp as postback_timestamp,
    day
FROM
    ods_platform_appsflyer_skad_postbacks_copy
WHERE
    day between '{startDayStr}'
    and '{endDayStr}'
    AND app_id = '6448786147';

@media :=
select
    max (media_source) as media,
    skad_ad_network_id
from
    ods_platform_appsflyer_skad_details
where
    day between '{startDayStr}'
    and '{endDayStr}'
    and app_id = 'id6448786147'
group by
    skad_ad_network_id;

@ret :=
select
    media.media,
    skad.skad_ad_network_id,
    skad.cv,
    skad.postback_timestamp,
    skad.day
from
    @skad as skad
    left join @media as media on skad.skad_ad_network_id = media.skad_ad_network_id;

select
    COALESCE(ret.media, media2.media) AS media,
    ret.cv,
    ret.day,
    count(1) as count
from
    @ret as ret
    left join skad_network_id_map as media2 on ret.skad_ad_network_id = media2.skad_ad_network_id
group by
    COALESCE(ret.media, media2.media),
    ret.cv,
    ret.day
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df

csvStr20240706 = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2024-07-05 09:48:05,0,,,
id6448786147,1,af_purchase_update_skan_on,0,1,0,0.97,0,24,2024-07-05 09:48:05,0,,,
id6448786147,2,af_purchase_update_skan_on,0,1,0.97,0.99,0,24,2024-07-05 09:48:05,0,,,
id6448786147,3,af_purchase_update_skan_on,0,1,0.99,1.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,4,af_purchase_update_skan_on,0,1,1.92,2.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,5,af_purchase_update_skan_on,0,1,2.91,3.28,0,24,2024-07-05 09:48:05,0,,,
id6448786147,6,af_purchase_update_skan_on,0,1,3.28,5.85,0,24,2024-07-05 09:48:05,0,,,
id6448786147,7,af_purchase_update_skan_on,0,1,5.85,7.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,8,af_purchase_update_skan_on,0,1,7.67,9.24,0,24,2024-07-05 09:48:05,0,,,
id6448786147,9,af_purchase_update_skan_on,0,1,9.24,12.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,10,af_purchase_update_skan_on,0,1,12.4,14.95,0,24,2024-07-05 09:48:05,0,,,
id6448786147,11,af_purchase_update_skan_on,0,1,14.95,17.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,12,af_purchase_update_skan_on,0,1,17.96,22.37,0,24,2024-07-05 09:48:05,0,,,
id6448786147,13,af_purchase_update_skan_on,0,1,22.37,26.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,14,af_purchase_update_skan_on,0,1,26.96,31.81,0,24,2024-07-05 09:48:05,0,,,
id6448786147,15,af_purchase_update_skan_on,0,1,31.81,36.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,16,af_purchase_update_skan_on,0,1,36.25,42.53,0,24,2024-07-05 09:48:05,0,,,
id6448786147,17,af_purchase_update_skan_on,0,1,42.53,49.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,18,af_purchase_update_skan_on,0,1,49.91,57.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,19,af_purchase_update_skan_on,0,1,57.92,67.93,0,24,2024-07-05 09:48:05,0,,,
id6448786147,20,af_purchase_update_skan_on,0,1,67.93,81.27,0,24,2024-07-05 09:48:05,0,,,
id6448786147,21,af_purchase_update_skan_on,0,1,81.27,98.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,22,af_purchase_update_skan_on,0,1,98.25,117.86,0,24,2024-07-05 09:48:05,0,,,
id6448786147,23,af_purchase_update_skan_on,0,1,117.86,142.29,0,24,2024-07-05 09:48:05,0,,,
id6448786147,24,af_purchase_update_skan_on,0,1,142.29,180.76,0,24,2024-07-05 09:48:05,0,,,
id6448786147,25,af_purchase_update_skan_on,0,1,180.76,225.43,0,24,2024-07-05 09:48:05,0,,,
id6448786147,26,af_purchase_update_skan_on,0,1,225.43,276.72,0,24,2024-07-05 09:48:05,0,,,
id6448786147,27,af_purchase_update_skan_on,0,1,276.72,347.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,28,af_purchase_update_skan_on,0,1,347.4,472.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,29,af_purchase_update_skan_on,0,1,472.67,620.8,0,24,2024-07-05 09:48:05,0,,,
id6448786147,30,af_purchase_update_skan_on,0,1,620.8,972.22,0,24,2024-07-05 09:48:05,0,,,
id6448786147,31,af_purchase_update_skan_on,0,1,972.22,2038.09,0,24,2024-07-05 09:48:05,0,,,
'''


def getCvMap():
    csv_file_like_object = io.StringIO(csvStr20240706)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    # cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    return cvMapDf

def getRevenueDataIOSGroupByGeoAndMedia():
    startDayStr = '20241201'
    endDayStr = '20241231'

    filename = f'/src/data/20250107_lwRevenueMedia_{startDayStr}_{endDayStr}.csv'
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



def main():
    # skanDf = getSKANDataFromMC()
    # # skanDf = skanDf.loc[skanDf['media'] == 'Facebook']
    # skanDf['cv'] = pd.to_numeric(skanDf['cv'], errors='coerce')
    # skanDf['cv'] = skanDf['cv'].fillna(-1)
    # skanDf.loc[skanDf['cv']>=32,'cv'] -= 32
    # print('skanDf:')
    # print(skanDf)

    # cvMapDf = getCvMap()
    # cvMapDf.rename(columns={'conversion_value':'cv'}, inplace=True)

    # cvMapDf['avg'] = (cvMapDf['min_event_revenue'] + cvMapDf['max_event_revenue'])/2

    # skanDf = skanDf.merge(cvMapDf, on='cv', how='left')
    # print('skanDf2:')
    # print(skanDf)

    # skanDf['revenue'] = skanDf['count'] * skanDf['avg']
    # print('sum revenue:')
    # print(skanDf.groupby('media').sum())
    

    revenueData = getRevenueDataIOSGroupByGeoAndMedia()
    revenueData = revenueData.loc[revenueData['media'] == 'Facebook']
    revenueData = revenueData[(revenueData['install_date'] >= '20241216') & (revenueData['install_date'] <= '20241229')]
    print(revenueData.groupby('install_date').sum())

if __name__ == '__main__':
    main()
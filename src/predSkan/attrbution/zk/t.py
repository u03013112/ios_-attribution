import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getData():
    sql = '''
        SELECT
            new_map.media as new_map_media,
            new_map.cv as new_map_cv,
            new_map.count as new_map_count,
            detail_map.count as detail_map_count
        FROM
        (
            SELECT
            CASE
                WHEN skad_ad_network_id IN (
                'v9wttpbfk9.skadnetwork',
                'n38lu8286q.skadnetwork'
                ) THEN 'Facebook Ads'
                WHEN skad_ad_network_id IN (
                'cstr6suwn9.skadnetwork',
                'eqhxz8m8av.skadnetwork'
                ) THEN 'googleadwords_int'
                WHEN skad_ad_network_id IN (
                '238da6jt44.skadnetwork',
                '899vrgt9g8.skadnetwork',
                'mj797d8u6f.skadnetwork',
                '22mmun2rn5.skadnetwork'
                ) THEN 'bytedanceglobal_int'
            END as media,
            skad_conversion_value as cv,
            COUNT(*) as count
            FROM
            ods_platform_appsflyer_skad_postbacks_copy
            WHERE
            day BETWEEN '20230701'
            AND '20230730'
            AND app_id = 'id1479198816'
            AND skad_ad_network_id IN (
                'v9wttpbfk9.skadnetwork',
                'n38lu8286q.skadnetwork',
                'cstr6suwn9.skadnetwork',
                'eqhxz8m8av.skadnetwork',
                '238da6jt44.skadnetwork',
                '899vrgt9g8.skadnetwork',
                'mj797d8u6f.skadnetwork',
                '22mmun2rn5.skadnetwork'
            )
            GROUP BY
            media,
            skad_conversion_value
        ) as new_map
        JOIN (
            SELECT
            media_source as media,
            skad_conversion_value as cv,
            COUNT(*) as count
            FROM
            ods_platform_appsflyer_skad_details
            WHERE
            day BETWEEN '20230701'
            AND '20230730'
            AND app_id = 'id1479198816'
            AND media_source IN (
                'Facebook Ads',
                'googleadwords_int',
                'bytedanceglobal_int'
            )
            AND event_name in ('af_skad_install', 'af_skad_redownload')
            GROUP BY
            media,
            skad_conversion_value
        ) as detail_map ON new_map.media = detail_map.media
        AND new_map.cv = detail_map.cv
    ;
    '''
    df = execSql(sql)
    return df

import io
def getCvMap():
    csv_str = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id1479198816,0,,,,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,1,af_skad_revenue,0,1,0,1.64,0,24,2023-03-30 10:23:45,0,,,
id1479198816,2,af_skad_revenue,0,1,1.64,3.24,0,24,2023-03-30 10:23:45,0,,,
id1479198816,3,af_skad_revenue,0,1,3.24,5.35,0,24,2023-03-30 10:23:45,0,,,
id1479198816,4,af_skad_revenue,0,1,5.35,7.8,0,24,2023-03-30 10:23:45,0,,,
id1479198816,5,af_skad_revenue,0,1,7.8,10.71,0,24,2023-03-30 10:23:45,0,,,
id1479198816,6,af_skad_revenue,0,1,10.71,14.47,0,24,2023-03-30 10:23:45,0,,,
id1479198816,7,af_skad_revenue,0,1,14.47,18.99,0,24,2023-03-30 10:23:45,0,,,
id1479198816,8,af_skad_revenue,0,1,18.99,24.29,0,24,2023-03-30 10:23:45,0,,,
id1479198816,9,af_skad_revenue,0,1,24.29,31.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,10,af_skad_revenue,0,1,31.08,40.26,0,24,2023-03-30 10:23:45,0,,,
id1479198816,11,af_skad_revenue,0,1,40.26,51.52,0,24,2023-03-30 10:23:45,0,,,
id1479198816,12,af_skad_revenue,0,1,51.52,61.25,0,24,2023-03-30 10:23:45,0,,,
id1479198816,13,af_skad_revenue,0,1,61.25,70.16,0,24,2023-03-30 10:23:45,0,,,
id1479198816,14,af_skad_revenue,0,1,70.16,82.56,0,24,2023-03-30 10:23:45,0,,,
id1479198816,15,af_skad_revenue,0,1,82.56,97.38,0,24,2023-03-30 10:23:45,0,,,
id1479198816,16,af_skad_revenue,0,1,97.38,111.57,0,24,2023-03-30 10:23:45,0,,,
id1479198816,17,af_skad_revenue,0,1,111.57,125.27,0,24,2023-03-30 10:23:45,0,,,
id1479198816,18,af_skad_revenue,0,1,125.27,142.67,0,24,2023-03-30 10:23:45,0,,,
id1479198816,19,af_skad_revenue,0,1,142.67,161.66,0,24,2023-03-30 10:23:45,0,,,
id1479198816,20,af_skad_revenue,0,1,161.66,184.42,0,24,2023-03-30 10:23:45,0,,,
id1479198816,21,af_skad_revenue,0,1,184.42,204.85,0,24,2023-03-30 10:23:45,0,,,
id1479198816,22,af_skad_revenue,0,1,204.85,239.74,0,24,2023-03-30 10:23:45,0,,,
id1479198816,23,af_skad_revenue,0,1,239.74,264.97,0,24,2023-03-30 10:23:45,0,,,
id1479198816,24,af_skad_revenue,0,1,264.97,306.91,0,24,2023-03-30 10:23:45,0,,,
id1479198816,25,af_skad_revenue,0,1,306.91,355.15,0,24,2023-03-30 10:23:45,0,,,
id1479198816,26,af_skad_revenue,0,1,355.15,405.65,0,24,2023-03-30 10:23:45,0,,,
id1479198816,27,af_skad_revenue,0,1,405.65,458.36,0,24,2023-03-30 10:23:45,0,,,
id1479198816,28,af_skad_revenue,0,1,458.36,512.69,0,24,2023-03-30 10:23:45,0,,,
id1479198816,29,af_skad_revenue,0,1,512.69,817.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,30,af_skad_revenue,0,1,817.08,1819.03,0,24,2023-03-30 10:23:45,0,,,
id1479198816,31,af_skad_revenue,0,1,1819.03,2544.74,0,24,2023-03-30 10:23:45,0,,,
    '''
    csv_file_like_object = io.StringIO(csv_str)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    # cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf



def main():
    df = pd.read_csv('/src/data/zk2/t.csv')
    df.rename(columns={'new_map_media':'media','new_map_cv':'cv','new_map_count':'new_count','detail_map_count':'detail_count'},inplace=True)
    df.loc[df['cv']>=32,'cv'] -= 32
    cvMapDf = getCvMap()
    cvMapDf.rename(columns={'conversion_value':'cv','min_event_revenue':'min','max_event_revenue':'max'},inplace=True)
    
    cvMapDf['min'].fillna(0,inplace=True)
    cvMapDf['max'].fillna(0,inplace=True)
    cvMapDf['avg'] = (cvMapDf['min'] + cvMapDf['max'])/2
    cvMapDf = cvMapDf[['cv','avg']]
    
    df = pd.merge(df,cvMapDf,on='cv',how='left')
    df['new_usd'] = df['new_count'] * df['avg']
    df['detail_usd'] = df['detail_count'] * df['avg']
    df = df.groupby(['media']).agg({'new_usd':'sum','detail_usd':'sum'})
    df['diff'] = (df['new_usd'] - df['detail_usd'])/df['detail_usd']
    print(df)

if __name__ == '__main__':
    # df = getData()
    # # print(df)
    # df.to_csv('/src/data/zk2/t.csv', index=False)
    main()




import io
import os
import sys
import pandas as pd

sys.path.append('/src')
from src.maxCompute import execSql


def getDiffData():
    filename = '/src/data/20240417DiffData.csv'
    if not os.path.exists(filename):
        sql = '''
    SET odps.sql.timezone=Africa/Accra;
    set odps.sql.hive.compatible=true;
    set odps.sql.executionengine.enable.rand.time.seed=true;

    @afTable :=
    select
    customer_user_id,
    install_time,
    event_time,
    get_json_object(base64decode(event_value), '$.af_order_id') as order_id,
    event_revenue_usd
    from
    ods_platform_appsflyer_events
    where
    zone = 0
    and app = 502
    and app_id = 'id6448786147'
    and day between '20240403' and '20240408'
    and (event_timestamp - install_timestamp) between 0 and 86400
    and event_name = 'af_sdk_update_skan'
    ;

    @biTable :=
    select
    game_uid,
    order_id,
    install_day,
    event_time,
    revenue_value_usd
    from dwd_lastwar_order_revenue
    where
    app_package = 'id6448786147'
    and (event_time - install_timestamp) between 0 and 86400
    ;

    select
    afTable.customer_user_id,
    sum(afTable.event_revenue_usd) as afUsd,
    sum(biTable.revenue_value_usd) as biUsd
    from @afTable as afTable
    join @biTable as biTable
    on afTable.customer_user_id = biTable.game_uid AND biTable.install_day <= 20240407 AND biTable.install_day >= 20240403
    group by afTable.customer_user_id
    having ABS(afUsd - biUsd) > 0.01;
    ;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:', filename)
        df = pd.read_csv(filename)

    return df


def getCvMap():
    csv_str = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2023-11-28 11:13:03,0,,,
id6448786147,1,af_skad_revenue,0,1,0,0.99,0,24,2023-11-28 11:13:03,0,,,
id6448786147,2,af_skad_revenue,0,1,0.99,1.15,0,24,2023-11-28 11:13:03,0,,,
id6448786147,3,af_skad_revenue,0,1,1.15,1.3,0,24,2023-11-28 11:13:03,0,,,
id6448786147,4,af_skad_revenue,0,1,1.3,2.98,0,24,2023-11-28 11:13:03,0,,,
id6448786147,5,af_skad_revenue,0,1,2.98,3.41,0,24,2023-11-28 11:13:03,0,,,
id6448786147,6,af_skad_revenue,0,1,3.41,5.98,0,24,2023-11-28 11:13:03,0,,,
id6448786147,7,af_skad_revenue,0,1,5.98,7.46,0,24,2023-11-28 11:13:03,0,,,
id6448786147,8,af_skad_revenue,0,1,7.46,9.09,0,24,2023-11-28 11:13:03,0,,,
id6448786147,9,af_skad_revenue,0,1,9.09,12.05,0,24,2023-11-28 11:13:03,0,,,
id6448786147,10,af_skad_revenue,0,1,12.05,14.39,0,24,2023-11-28 11:13:03,0,,,
id6448786147,11,af_skad_revenue,0,1,14.39,18.17,0,24,2023-11-28 11:13:03,0,,,
id6448786147,12,af_skad_revenue,0,1,18.17,22.07,0,24,2023-11-28 11:13:03,0,,,
id6448786147,13,af_skad_revenue,0,1,22.07,26.57,0,24,2023-11-28 11:13:03,0,,,
id6448786147,14,af_skad_revenue,0,1,26.57,32.09,0,24,2023-11-28 11:13:03,0,,,
id6448786147,15,af_skad_revenue,0,1,32.09,37.42,0,24,2023-11-28 11:13:03,0,,,
id6448786147,16,af_skad_revenue,0,1,37.42,42.94,0,24,2023-11-28 11:13:03,0,,,
id6448786147,17,af_skad_revenue,0,1,42.94,50.34,0,24,2023-11-28 11:13:03,0,,,
id6448786147,18,af_skad_revenue,0,1,50.34,58.56,0,24,2023-11-28 11:13:03,0,,,
id6448786147,19,af_skad_revenue,0,1,58.56,67.93,0,24,2023-11-28 11:13:03,0,,,
id6448786147,20,af_skad_revenue,0,1,67.93,80.71,0,24,2023-11-28 11:13:03,0,,,
id6448786147,21,af_skad_revenue,0,1,80.71,100.32,0,24,2023-11-28 11:13:03,0,,,
id6448786147,22,af_skad_revenue,0,1,100.32,116.94,0,24,2023-11-28 11:13:03,0,,,
id6448786147,23,af_skad_revenue,0,1,116.94,130.41,0,24,2023-11-28 11:13:03,0,,,
id6448786147,24,af_skad_revenue,0,1,130.41,153.76,0,24,2023-11-28 11:13:03,0,,,
id6448786147,25,af_skad_revenue,0,1,153.76,196.39,0,24,2023-11-28 11:13:03,0,,,
id6448786147,26,af_skad_revenue,0,1,196.39,235.93,0,24,2023-11-28 11:13:03,0,,,
id6448786147,27,af_skad_revenue,0,1,235.93,292.07,0,24,2023-11-28 11:13:03,0,,,
id6448786147,28,af_skad_revenue,0,1,292.07,424.48,0,24,2023-11-28 11:13:03,0,,,
id6448786147,29,af_skad_revenue,0,1,424.48,543.77,0,24,2023-11-28 11:13:03,0,,,
id6448786147,30,af_skad_revenue,0,1,543.77,753.61,0,24,2023-11-28 11:13:03,0,,,
id6448786147,31,af_skad_revenue,0,1,753.61,1804,0,24,2023-11-28 11:13:03,0,,,
    '''
    csv_file_like_object = io.StringIO(csv_str)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    # cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf

def addCv(df, cvMapDf,usdCol='r1usd',cvCol='cv'):
    # 将数据类型转换为数值类型，无法解析的字符串转换为NaN
    df[usdCol] = pd.to_numeric(df[usdCol], errors='coerce')
    cvMapDf['min_event_revenue'] = pd.to_numeric(cvMapDf['min_event_revenue'], errors='coerce')
    cvMapDf['max_event_revenue'] = pd.to_numeric(cvMapDf['max_event_revenue'], errors='coerce')
    cvMapDf['conversion_value'] = pd.to_numeric(cvMapDf['conversion_value'], errors='coerce')

    df.loc[:, cvCol] = 0
    for index, row in cvMapDf.iterrows():
        df.loc[(df[usdCol] > row['min_event_revenue']) & (df[usdCol] <= row['max_event_revenue']), cvCol] = row['conversion_value']
    
    # 如果r1usd > 最大max_event_revenue，则取最大值
    df.loc[df[usdCol] > cvMapDf['max_event_revenue'].max(), cvCol] = cvMapDf['conversion_value'].max()
    return df


def getCvDiffUser():
    df = getDiffData()
    cvMapDf = getCvMap()

    addCv(df, cvMapDf,usdCol='afusd',cvCol='afCv')
    addCv(df, cvMapDf,usdCol='biusd',cvCol='biCv')
    
    diffDf = df.loc[df['biCv'] != df['afCv']]
    diffDf = diffDf.sort_values(by=['customer_user_id'])

    diffDf.to_csv('/src/data/diff.csv', index=False)

    print('sum af usd:',diffDf['afusd'].sum())
    print('sum bi usd:',diffDf['biusd'].sum())

    return diffDf

if __name__ == '__main__':
    getCvDiffUser()

# 调查skan无法匹配问题
# 目前只得到20240415的数据，看到主要缺失数据来自applovin的cv==18的数据
# 所以针对这部分进行调查

# 1、获得bi数据，找到cv==18的所有用户，即24小时付费金额在50.34，58.56之间的用户
# 时间应该在20240412~20240414之间，扩大范围可以扩大到20240407~20240414

import io
import os
import sys
import pandas as pd

sys.path.append('/src')
from src.maxCompute import execSql

def getCvDataFromBi(cv, startDayStr, endDayStr):
    filename = f'/src/data/20240415cv{cv}_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
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
        cvMapDf = cvMapDf.loc[(cvMapDf['conversion_value'] == cv)]
        min_event_revenue = cvMapDf['min_event_revenue'].values[0]
        max_event_revenue = cvMapDf['max_event_revenue'].values[0]

        sql = f'''
    select
    game_uid,
    install_day,
    sum(revenue_value_usd) as revenue
    from
        ads_lastwar_ios_purchase_adv
    where
    app_package = 'id6448786147'
    and (event_timestamp - install_timestamp) between 0
    and 86400
    and install_day between '{startDayStr}' and '{endDayStr}'
    group by 
        game_uid, 
        install_day
    having
        revenue between {min_event_revenue} and {max_event_revenue}
    ;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:', filename)
        df = pd.read_csv(filename, dtype={'install_day':str})

    return df

def main():
    cv = 18
    startDayStr = '20240407'
    endDayStr = '20240414'
    df = getCvDataFromBi(cv, startDayStr, endDayStr)
    df = df.sort_values(by='install_day', ascending=False)
    # print(df)
    df2 = df.loc[(df['install_day']>= '20240412') & (df['install_day']<= '20240414')]
    print(df2)
    print(len(df2))

if __name__ == '__main__':
    main()
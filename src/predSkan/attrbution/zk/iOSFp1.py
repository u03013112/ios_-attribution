# iOS版本，进行归因方案1
# 具体方案参照androidFp
# 由于CV Map改变，所以暂时只处理3月1日及以后数据
# 此文件对应阿里线上（dataworks）iOS归因中的FunPlus01

import io
import pandas as pd
from datetime import datetime, timedelta

# 参数dayStr，是当前的日期，即${yyyymmdd-1}，格式为'20230301'
# 生成安装日期是dayStr - 7的各媒体7日回收金额

# 为了兼容本地调试，要在所有代码钱调用此方法
def init():
    global execSql
    global dayStr
    if 'o' in globals():
        print('this is online version')

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader() as reader:
                pd_df = reader.to_pandas()
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']

    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        dayStr = '20230401'

def getSKANDataFromMC(dayStr):
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=14)
    dayMax = day - timedelta(days=0)
    dayMinStr = dayMin.strftime('%Y%m%d')
    dayMaxStr = dayMax.strftime('%Y%m%d')

    # 修改后的SQL语句
    sql = f'''
        SELECT
            media_source as media,
            skad_conversion_value as cv,
            timestamp as postback_timestamp
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day BETWEEN '{dayMinStr}' AND '{dayMaxStr}'
            AND app_id = 'id1479198816'
            AND event_name in (
                'af_skad_install',
                'af_skad_redownload'
            )
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

# 计算合法的激活时间范围
def skanAddValidInstallDate(skanDf):
    # 将postback_timestamp转换为datetime
    skanDf['postback_timestamp'] = pd.to_datetime(skanDf['postback_timestamp'])
    # 将cv转换为整数类型
    skanDf['cv'] = skanDf['cv'].astype(int)

    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=48)
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=72)
    skanDf.loc[:, 'max_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=24)
    return skanDf

def getAfDataFromMC(dayStr):
    # 将dayStr转换为日期对象
    day = datetime.strptime(dayStr, '%Y%m%d')

    # 计算dayMinStr和dayMaxStr
    dayMin = day - timedelta(days=14)
    dayMax = day - timedelta(days=0)
    dayMinStr = dayMin.strftime('%Y%m%d')
    dayMaxStr = dayMax.strftime('%Y%m%d')

    # 修改后的SQL语句
    sql = f'''
        SELECT
            appsflyer_id,
            install_timestamp,
            SUM(CASE WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN event_revenue_usd ELSE 0 END) as r1usd,
            SUM(CASE WHEN event_timestamp <= install_timestamp + 168 * 3600 THEN event_revenue_usd ELSE 0 END) as r7usd,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date
        FROM
            ods_platform_appsflyer_events
        WHERE
            app_id = 'id1479198816'
            AND zone = 0
            AND day BETWEEN '{dayMinStr}' AND '{dayMaxStr}'
            AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('{dayMinStr}', 'yyyyMMdd') AND to_date('{dayMaxStr}', 'yyyyMMdd')
        GROUP BY
            appsflyer_id,
            install_timestamp,
            install_date
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

def getCvMap():
    csv_str = '''
    app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id1479198816,0,,,,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,1,af_skad_revenue,0,1,0,1.64,0,24,2023-03-30 10:23:45,0,,,
id1479198816,2,af_skad_revenue,0,1,1.64,3.24,0,24,2023-03-30 10:23:45,0,,,
id1479198816,2,af_purchase,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,3,af_skad_revenue,0,1,3.24,5.35,0,24,2023-03-30 10:23:45,0,,,
id1479198816,3,af_purchase,1,2,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,4,af_skad_revenue,0,1,5.35,7.8,0,24,2023-03-30 10:23:45,0,,,
id1479198816,4,af_purchase,1,2,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,5,af_skad_revenue,0,1,7.8,10.71,0,24,2023-03-30 10:23:45,0,,,
id1479198816,5,af_purchase,2,3,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,6,af_skad_revenue,0,1,10.71,14.47,0,24,2023-03-30 10:23:45,0,,,
id1479198816,6,af_purchase,2,3,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,7,af_skad_revenue,0,1,14.47,18.99,0,24,2023-03-30 10:23:45,0,,,
id1479198816,7,af_purchase,3,4,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,8,af_skad_revenue,0,1,18.99,24.29,0,24,2023-03-30 10:23:45,0,,,
id1479198816,8,af_purchase,4,5,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,9,af_skad_revenue,0,1,24.29,31.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,9,af_purchase,5,7,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,10,af_skad_revenue,0,1,31.08,40.26,0,24,2023-03-30 10:23:45,0,,,
id1479198816,10,af_purchase,7,9,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,11,af_skad_revenue,0,1,40.26,51.52,0,24,2023-03-30 10:23:45,0,,,
id1479198816,11,af_purchase,9,10,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,12,af_skad_revenue,0,1,51.52,61.25,0,24,2023-03-30 10:23:45,0,,,
id1479198816,12,af_purchase,10,13,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,13,af_skad_revenue,0,1,61.25,70.16,0,24,2023-03-30 10:23:45,0,,,
id1479198816,13,af_purchase,13,15,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,14,af_skad_revenue,0,1,70.16,82.56,0,24,2023-03-30 10:23:45,0,,,
id1479198816,14,af_purchase,15,17,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,15,af_skad_revenue,0,1,82.56,97.38,0,24,2023-03-30 10:23:45,0,,,
id1479198816,15,af_purchase,17,20,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,16,af_skad_revenue,0,1,97.38,111.57,0,24,2023-03-30 10:23:45,0,,,
id1479198816,16,af_purchase,20,22,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,17,af_skad_revenue,0,1,111.57,125.27,0,24,2023-03-30 10:23:45,0,,,
id1479198816,17,af_purchase,22,26,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,18,af_skad_revenue,0,1,125.27,142.67,0,24,2023-03-30 10:23:45,0,,,
id1479198816,18,af_purchase,26,30,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,19,af_skad_revenue,0,1,142.67,161.66,0,24,2023-03-30 10:23:45,0,,,
id1479198816,19,af_purchase,30,34,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,20,af_skad_revenue,0,1,161.66,184.42,0,24,2023-03-30 10:23:45,0,,,
id1479198816,20,af_purchase,34,38,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,21,af_skad_revenue,0,1,184.42,204.85,0,24,2023-03-30 10:23:45,0,,,
id1479198816,21,af_purchase,38,43,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,22,af_skad_revenue,0,1,204.85,239.74,0,24,2023-03-30 10:23:45,0,,,
id1479198816,22,af_purchase,43,48,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,23,af_skad_revenue,0,1,239.74,264.97,0,24,2023-03-30 10:23:45,0,,,
id1479198816,23,af_purchase,48,55,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,24,af_skad_revenue,0,1,264.97,306.91,0,24,2023-03-30 10:23:45,0,,,
id1479198816,24,af_purchase,55,65,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,25,af_skad_revenue,0,1,306.91,355.15,0,24,2023-03-30 10:23:45,0,,,
id1479198816,25,af_purchase,65,72,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,26,af_skad_revenue,0,1,355.15,405.65,0,24,2023-03-30 10:23:45,0,,,
id1479198816,26,af_purchase,72,86,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,27,af_skad_revenue,0,1,405.65,458.36,0,24,2023-03-30 10:23:45,0,,,
id1479198816,27,af_purchase,86,92,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,28,af_skad_revenue,0,1,458.36,512.69,0,24,2023-03-30 10:23:45,0,,,
id1479198816,28,af_purchase,92,97,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,29,af_skad_revenue,0,1,512.69,817.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,29,af_purchase,97,98,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,30,af_skad_revenue,0,1,817.08,1819.03,0,24,2023-03-30 10:23:45,0,,,
id1479198816,30,af_purchase,98,99,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,31,af_skad_revenue,0,1,1819.03,2544.74,0,24,2023-03-30 10:23:45,0,,,
id1479198816,31,af_purchase,99,100,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,32,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,33,af_skad_revenue,0,1,0,1.64,0,24,2023-03-30 10:23:45,0,,,
id1479198816,33,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,34,af_skad_revenue,0,1,1.64,3.24,0,24,2023-03-30 10:23:45,0,,,
id1479198816,34,af_purchase,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,34,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,35,af_skad_revenue,0,1,3.24,5.35,0,24,2023-03-30 10:23:45,0,,,
id1479198816,35,af_purchase,1,2,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,35,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,36,af_skad_revenue,0,1,5.35,7.8,0,24,2023-03-30 10:23:45,0,,,
id1479198816,36,af_purchase,1,2,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,36,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,37,af_skad_revenue,0,1,7.8,10.71,0,24,2023-03-30 10:23:45,0,,,
id1479198816,37,af_purchase,2,3,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,37,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,38,af_skad_revenue,0,1,10.71,14.47,0,24,2023-03-30 10:23:45,0,,,
id1479198816,38,af_purchase,2,3,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,38,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,39,af_skad_revenue,0,1,14.47,18.99,0,24,2023-03-30 10:23:45,0,,,
id1479198816,39,af_purchase,3,4,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,39,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,40,af_skad_revenue,0,1,18.99,24.29,0,24,2023-03-30 10:23:45,0,,,
id1479198816,40,af_purchase,4,5,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,40,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,41,af_skad_revenue,0,1,24.29,31.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,41,af_purchase,5,7,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,41,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,42,af_skad_revenue,0,1,31.08,40.26,0,24,2023-03-30 10:23:45,0,,,
id1479198816,42,af_purchase,7,9,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,42,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,43,af_skad_revenue,0,1,40.26,51.52,0,24,2023-03-30 10:23:45,0,,,
id1479198816,43,af_purchase,9,10,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,43,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,44,af_skad_revenue,0,1,51.52,61.25,0,24,2023-03-30 10:23:45,0,,,
id1479198816,44,af_purchase,10,13,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,44,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,45,af_skad_revenue,0,1,61.25,70.16,0,24,2023-03-30 10:23:45,0,,,
id1479198816,45,af_purchase,13,15,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,45,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,46,af_skad_revenue,0,1,70.16,82.56,0,24,2023-03-30 10:23:45,0,,,
id1479198816,46,af_purchase,15,17,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,46,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,47,af_skad_revenue,0,1,82.56,97.38,0,24,2023-03-30 10:23:45,0,,,
id1479198816,47,af_purchase,17,20,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,47,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,48,af_skad_revenue,0,1,97.38,111.57,0,24,2023-03-30 10:23:45,0,,,
id1479198816,48,af_purchase,20,22,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,48,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,49,af_skad_revenue,0,1,111.57,125.27,0,24,2023-03-30 10:23:45,0,,,
id1479198816,49,af_purchase,22,26,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,49,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,50,af_skad_revenue,0,1,125.27,142.67,0,24,2023-03-30 10:23:45,0,,,
id1479198816,50,af_purchase,26,30,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,50,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,51,af_skad_revenue,0,1,142.67,161.66,0,24,2023-03-30 10:23:45,0,,,
id1479198816,51,af_purchase,30,34,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,51,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,52,af_skad_revenue,0,1,161.66,184.42,0,24,2023-03-30 10:23:45,0,,,
id1479198816,52,af_purchase,34,38,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,52,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,53,af_skad_revenue,0,1,184.42,204.85,0,24,2023-03-30 10:23:45,0,,,
id1479198816,53,af_purchase,38,43,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,53,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,54,af_skad_revenue,0,1,204.85,239.74,0,24,2023-03-30 10:23:45,0,,,
id1479198816,54,af_purchase,43,48,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,54,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,55,af_skad_revenue,0,1,239.74,264.97,0,24,2023-03-30 10:23:45,0,,,
id1479198816,55,af_purchase,48,55,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,55,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,56,af_skad_revenue,0,1,264.97,306.91,0,24,2023-03-30 10:23:45,0,,,
id1479198816,56,af_purchase,55,65,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,56,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,57,af_skad_revenue,0,1,306.91,355.15,0,24,2023-03-30 10:23:45,0,,,
id1479198816,57,af_purchase,65,72,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,57,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,58,af_skad_revenue,0,1,355.15,405.65,0,24,2023-03-30 10:23:45,0,,,
id1479198816,58,af_purchase,72,86,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,58,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,59,af_skad_revenue,0,1,405.65,458.36,0,24,2023-03-30 10:23:45,0,,,
id1479198816,59,af_purchase,86,92,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,59,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,60,af_skad_revenue,0,1,458.36,512.69,0,24,2023-03-30 10:23:45,0,,,
id1479198816,60,af_purchase,92,97,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,60,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,61,af_skad_revenue,0,1,512.69,817.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,61,af_purchase,97,98,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,61,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,62,af_skad_revenue,0,1,817.08,1819.03,0,24,2023-03-30 10:23:45,0,,,
id1479198816,62,af_purchase,98,99,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,62,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,63,af_skad_revenue,0,1,1819.03,2544.74,0,24,2023-03-30 10:23:45,0,,,
id1479198816,63,af_purchase,99,100,,,0,24,2023-03-30 10:23:45,0,,,
id1479198816,63,af_attribution_flag,0,1,,,0,24,2023-03-30 10:23:45,0,,,
    '''
    csv_file_like_object = io.StringIO(csv_str)
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
    cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]
    
    return cvMapDf

def addCv(df, cvMapDf):
    # 将数据类型转换为数值类型，无法解析的字符串转换为NaN
    df['r1usd'] = pd.to_numeric(df['r1usd'], errors='coerce')
    cvMapDf['min_event_revenue'] = pd.to_numeric(cvMapDf['min_event_revenue'], errors='coerce')
    cvMapDf['max_event_revenue'] = pd.to_numeric(cvMapDf['max_event_revenue'], errors='coerce')
    cvMapDf['conversion_value'] = pd.to_numeric(cvMapDf['conversion_value'], errors='coerce')

    df.loc[:, 'cv'] = 0
    for index, row in cvMapDf.iterrows():
        df.loc[(df['r1usd'] > row['min_event_revenue']) & (df['r1usd'] <= row['max_event_revenue']), 'cv'] = row['conversion_value']
    
    # 如果r1usd > 最大max_event_revenue，则取最大值
    df.loc[df['r1usd'] > cvMapDf['max_event_revenue'].max(), 'cv'] = cvMapDf['conversion_value'].max()
    return df# 暂时就只关心这3个媒体

mediaList = [
    'bytedanceglobal_int',
    'googleadwords_int',
    'Facebook Ads',
    'snapchat_int',
    'applovin_int'
]

# 归因方案1
# 均分归因，步骤如下
# 1、给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'，值为0
# 2、遍历skanDf，每行做如下处理：获取media，cv，min_valid_install_timestamp和max_valid_install_timestamp。
# 在userDf中找到userDf.cv == skanDf.cv，并且 skanDf.max_valid_install_timestamp >= userDf.install_timestamp >= skanDf.min_valid_install_timestamp 的行。
# 该行的media+' count'列的值加1/N，N是符合上面条件的行数。比如通过cv与时间戳过滤找到符合的行是2，则每行的media+' count'列的值加1/2
# 3、返回前验证，检查userDf中是否有行的所有的media count列的和大于1，如果有，统计一下有多少行，占比（行数/总行数）是多少
# 4、返回userDf
# userDf 拥有列 appsflyer_id  install_timestamp      r1usd      r7usd  cv
# skanDf 拥有列 postback_timestamp  media  cv  min_valid_install_timestamp  max_valid_install_timestamp  postback_date  min_valid_install_date  max_valid_install_date
def attribution1(userDf,skanDf):
    skanDf = skanDf.loc[skanDf['media'].isin(mediaList)]
    # 1. 给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'，值为0
    for media in mediaList:
        userDf[media + ' count'] = 0

    userDf['install_timestamp'] = pd.to_datetime(userDf['install_timestamp'], unit='s')

    # 2. 遍历skanDf，每行做如下处理
    for index, row in skanDf.iterrows():
        media = row['media']
        cv = row['cv']
        min_valid_install_timestamp = row['min_valid_install_timestamp']
        max_valid_install_timestamp = row['max_valid_install_timestamp']

        # 在userDf中找到符合条件的行
        condition = (
            (userDf['cv'] == cv) &
            (userDf['install_timestamp'] >= min_valid_install_timestamp) &
            (userDf['install_timestamp'] <= max_valid_install_timestamp)
        )
        matching_rows = userDf[condition]

        num_matching_rows = len(matching_rows)
        # print(f"media: {media}, cv: {cv}, num_matching_rows: {num_matching_rows}")

        if num_matching_rows > 0:
            userDf.loc[condition, media + ' count'] += 1 / num_matching_rows

    # 3. 检查userDf中是否有行的所有的media count列的和大于1，如果有，统计一下有多少行，占比（行数/总行数）是多少
    media_counts_sum = userDf[[media + ' count' for media in mediaList]].sum(axis=1)
    invalid_rows = media_counts_sum > 1
    num_invalid_rows = invalid_rows.sum()
    total_rows = len(userDf)
    invalid_ratio = num_invalid_rows / total_rows

    print(f"Invalid rows: {num_invalid_rows}")
    print(f"Invalid ratio: {invalid_ratio:.2%}")

    # 4. 返回userDf
    return userDf

# 获得安装日期，格式为%Y%m%d
def getInstallDayStr(dayStr):
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=7)
    installDayStr = dayMin.strftime('%Y%m%d')
    return installDayStr

# 获得安装日期，格式为%Y-%m-%d
def getInstallDayStr2(dayStr):
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=7)
    installDayStr = dayMin.strftime('%Y-%m-%d')
    return installDayStr

# 将结论总结
def result1(userDf,dayStr):
    # 将数据类型转换为数值类型
    for media in mediaList:
        userDf[media + ' count'] = pd.to_numeric(userDf[media + ' count'], errors='coerce')
    userDf['r1usd'] = pd.to_numeric(userDf['r1usd'], errors='coerce')
    userDf['r7usd'] = pd.to_numeric(userDf['r7usd'], errors='coerce')

    for media in mediaList:
        userDf.loc[:,media+' r1usd'] = userDf[media+' count'] * userDf['r1usd']
        userDf.loc[:,media+' r7usd'] = userDf[media+' count'] * userDf['r7usd']
    
    # userDf = userDf.groupby(['install_date']).agg('sum').reset_index()
    # 只保留一天的数据 dayStr - 7
    day = datetime.strptime(dayStr, '%Y%m%d')
    dayMin = day - timedelta(days=7)
    dayMinStr = dayMin.strftime('%Y-%m-%d')
    installDayStr = dayMin.strftime('%Y%m%d')
    
    # print(installDayStr)

    df = userDf.loc[userDf['install_date'] == dayMinStr]

    retDf = pd.DataFrame(columns=['install_date','media','r7usd'])
    for media in mediaList:
        r7usd = df['%s r7usd'%media].sum()
        retDf = retDf.append({'install_date':installDayStr,'media':media,'r7usd':r7usd},ignore_index=True)
    
    return retDf

from odps.models import Schema, Column, Partition
def createTable():
    if 'o' in globals():
        columns = [
            Column(name='media', type='string', comment='like google,bytedance,facebook'),
            Column(name='r7usd', type='double', comment='d7Revenue')
        ]
        partitions = [
            Partition(name='install_date', type='string', comment='like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('topwar_ios_funplus01', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def writeTable(df,installDayStr):
    if 'o' in globals():
        t = o.get_table('topwar_ios_funplus01')
        t.delete_partition('install_date=%s'%(installDayStr), if_exists=True)
        with t.open_writer(partition='install_date=%s'%(installDayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')

init()
skanDf = getSKANDataFromMC(dayStr)
# print(skanDf.head(10))
skanDf = skanAddValidInstallDate(skanDf)
# print(skanDf.head(10))

afDf = getAfDataFromMC(dayStr)
userDf = addCv(afDf,getCvMap())
# print(userDf.head(10))

# userDf.to_csv('/src/data/userDf.csv',index=False)
# skanDf.to_csv('/src/data/skanDf.csv',index=False)

# userDf = pd.read_csv('/src/data/userDf.csv')
# skanDf = pd.read_csv('/src/data/skanDf.csv')
# print(userDf.head(10))
# print(skanDf.head(10))

attDf = attribution1(userDf,skanDf)

# attDf.to_csv('/src/data/attDf.csv',index=False)

retDf = result1(attDf,dayStr)

# debug
# df = userDf.loc[userDf['install_date'] == '2023-03-25']
# print(df['r7usd'].sum())
print(retDf)
createTable()
writeTable(retDf,getInstallDayStr(dayStr))

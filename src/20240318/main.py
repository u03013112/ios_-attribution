# https://rivergame.feishu.cn/wiki/NLQywigoDiBQeOk28NMcLXovn8g
# 融合归因 欠分配用户寻找

import io
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

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



# 先找到分配失败的SKAN条目
# 然后找到AF数据中能够匹配这个条目的数据
# 再找到AF数据中的uid，对应的BI数据，主要是安装时间

def getAFData(cv = 0,minValidInstallTimestamp=0,maxValidInstallTimestamp=0):
    cvMap = getCvMap()
    # 根据cv找到对应的min_event_revenue和max_event_revenue
    cvMap = cvMap.loc[cvMap['conversion_value'] == cv]
    if cvMap.shape[0] == 0:
        print(f'没有找到cv={cv}的配置')
        return None
    min_event_revenue = cvMap.iloc[0]['min_event_revenue']
    max_event_revenue = cvMap.iloc[0]['max_event_revenue']

    sql = f'''
SELECT
    appsflyer_id as appsflyer_id,
    customer_user_id as customer_user_id,
    install_timestamp,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN event_revenue_usd
                ELSE 0
            END
        ),
        0
    ) as r1usd,
    TO_CHAR(
        from_unixtime(install_timestamp),
        "yyyy-mm-dd"
    ) as install_date,
    country_code as country_code
FROM
    rg_bi.ods_platform_appsflyer_events
WHERE
    install_timestamp BETWEEN {minValidInstallTimestamp}
    AND {maxValidInstallTimestamp}
    AND day between 20240215 and 20240229
    AND app_id = 'id6448786147'
    AND event_name = 'af_purchase'
GROUP BY
    appsflyer_id,
    customer_user_id,
    install_timestamp,
    country_code,
    install_date
HAVING
    r1usd > {min_event_revenue} AND r1usd < {max_event_revenue}
;
    '''
    print(sql)
    data = execSql(sql)
    return data


def getBIData(uidList):
    gameUids = tuple(uidList)

    if len(gameUids) == 0:
        return None

    if len(gameUids) == 1:
        gameUids = f'({gameUids[0]})'
    
    sql = f'''
    SELECT
    game_uid as customer_user_id,
    install_timestamp,
    COALESCE(
        SUM(
            CASE
                WHEN event_timestamp <= install_timestamp + 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as r1usd,
    TO_CHAR(
    TO_DATE(from_unixtime(cast (install_timestamp as bigint)), "yyyy-mm-dd hh:mi:ss"),"yyyy-mm-dd"
    ) as install_date,
    country as country_code
FROM
    rg_bi.ads_lastwar_ios_purchase_adv
WHERE
    game_uid in {gameUids}
GROUP BY
    game_uid,
    install_timestamp,
    country
    ;
'''
    print(sql)
    data = execSql(sql)
    return data

ll = [
    # [30,1708981200,1709154000],
    # [31,1708045200,1708218000],
    # [31,1708822800,1708995600],
    # [31,1707872400,1708045200],
    # [31,1707958800,1708131600],
    # [31,1708909200,1709082000],
    # [31,1708894800,1709067600],
    # [31,1708088400,1708261200],
    # [28,1708304400,1708477200],
    # [30,1708401600,1708574400],
    # [30,1708740000,1708912800],
    # [30,1708376400,1708549200],
    # [29,1708045200,1708218000],
    # [29,1708477200,1708650000],
    # [29,1708736400,1708909200],
    # [28,1708981200,1709154000],
    # [28,1708981200,1709154000],
    # [28,1708477200,1708650000],
    # [28,1708736400,1708909200],
    # [28,1708563600,1708736400],
]


def debug():
    # 没有匹配到的SKAN数据
    debugDf = pd.read_csv('/src/data/debug01.csv')

    # debugDfGroup = debugDf.groupby(['cv','min_valid_install_timestamp','max_valid_install_timestamp']).sum().reset_index()
    # debugDfGroup = debugDfGroup[['cv','min_valid_install_timestamp','max_valid_install_timestamp']]

    retDf = pd.DataFrame()
    debugDf = debugDf[debugDf['cv'] == 30]
    print(len(debugDf))

    # 逐行遍历
    for index, row in debugDf.iterrows():
        cv = row['cv']
        
        minValidInstallTimestamp = row['min_valid_install_timestamp']
        maxValidInstallTimestamp = row['max_valid_install_timestamp']

        # 过滤，如果minValidInstallTimestamp < 2024-02-15 00:00:00，跳过
        if minValidInstallTimestamp < 1707955200:
            continue

        # 过滤，如果maxValidInstallTimestamp > 2024-02-26 23:59:59，跳过
        if maxValidInstallTimestamp > 1708991999:
            continue

        print(f'cv={cv},minValidInstallTimestamp={minValidInstallTimestamp},maxValidInstallTimestamp={maxValidInstallTimestamp}')
        afData = getAFData(cv,minValidInstallTimestamp,maxValidInstallTimestamp)
        # print(afData)

        uidList = afData['customer_user_id'].tolist()

        biData = getBIData(uidList)
        # print(biData)

        df = pd.merge(afData, biData, how='inner', on=['customer_user_id'],suffixes=('_af', '_bi')).reindex()

        df['index'] = index
        print(df)
        retDf = pd.concat([retDf,df])

    retDf.to_csv('/src/data/debugRet0319.csv',index=False)

def debug2():
    debugDf = pd.read_csv('/src/data/debug01.csv')
    debugDf = debugDf[debugDf['cv'] == 30]
    # index = 行号
    debugDf['index'] = debugDf.index
    # print(debugDf)

    retDf = pd.read_csv('/src/data/debugRet0319.csv')
    
    # 两个表合并
    df = pd.merge(debugDf, retDf, how='inner', on=['index'],suffixes=('_debug', '_ret')).reindex()
    # print(df)
    df = df[['campaign_id','customer_user_id','index','min_valid_install_timestamp','max_valid_install_timestamp','cv']]
    # 找到没有找到这个用户的skan数据，找到他的campaign

    print(df)
    # 找到这个用户被分配到哪个campaign了
    # 两个campaign一起进行搜索，查看是否是skan数量大于用户数量


if __name__ == '__main__':
    # debug()
    debug2()

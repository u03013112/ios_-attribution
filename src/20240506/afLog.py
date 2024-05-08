import io
import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
import pandas as pd


def readAfLog():
    df = pd.read_csv('afLog.csv')
    df1 = df[['appsflyer_id normal','sumrevenue']]
    df2 = df[['appsflyer_id skan sdk update','max cv']]

    df = df1.merge(df2, how='right', left_on='appsflyer_id normal', right_on='appsflyer_id skan sdk update')

    df.to_csv('/src/data/afLogMerged.csv', index=False)

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
    return df

def cvCheck():
    df = pd.read_csv('/src/data/afLogMerged.csv')
    df = df[df['sumrevenue'].isna() == False]

    df.rename(columns={'sumrevenue':'r1usd'}, inplace=True)
    df = addCv(df, getCvMap())
    
    df.loc[df['max cv'] >= 32, 'max cv'] -= 32
    diffDf = df.loc[df['cv'] != df['max cv']]
    print(diffDf)


def getLwRevenue():
    filename = f'/src/data/afLogStep3.csv'
    if not os.path.exists(filename):
        sql = f'''
SELECT
    game_uid,
    COALESCE(
        SUM(
            CASE
                WHEN event_time <= install_timestamp + 24 * 3600 THEN revenue_value_usd
                ELSE 0
            END
        ),
        0
    ) as r1usd
FROM
    dwd_overseas_revenue_allproject
WHERE
    app = '502'
    and zone = 0
    and app_package = 'id6448786147'
    and install_day = 20240430
GROUP BY
    game_uid
HAVING
    r1usd > 0
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

def getBIData():
    df = getLwRevenue()
    df = addCv(df, getCvMap())
    df = df.groupby('cv').agg('count').reset_index()
    df.rename(columns={'game_uid':'count'}, inplace=True)
    df = df[['cv','count']]
    # print(df)

    afDf = pd.read_csv('/src/data/afLogMerged.csv')
    afDf = afDf[['max cv','appsflyer_id skan sdk update']]
    afDf.loc[afDf['max cv'] >= 32, 'max cv'] -= 32
    afDf = afDf.groupby('max cv').agg('count').reset_index()
    afDf.rename(columns={
        'max cv':'cv',
        'appsflyer_id skan sdk update':'count'
    }, inplace=True)
    afDf = afDf[['cv','count']]

    df = df.merge(afDf, how='outer', on='cv', suffixes=('_bi','_af'))
    df.fillna(0, inplace=True)
    df = df.sort_values(by='cv', ascending=True)
    df.to_csv('/src/data/afLogStep3Ret.csv', index=False)

    cvMap = getCvMap()
    cvMap.rename(columns={
        'conversion_value':'cv'
    }, inplace=True)
    cvMap['avg'] = (cvMap['max_event_revenue'] + cvMap['min_event_revenue']) / 2
    cvMap = cvMap[['cv','avg']]
    # avg保留两位小数
    cvMap['avg'] = cvMap['avg'].apply(lambda x: round(x, 2))
    # print(cvMap)
    df = df.merge(cvMap, how='left', on='cv')
    df['afUsd'] = df['count_af'] * df['avg']
    df['biUsd'] = df['count_bi'] * df['avg']
    # afUsd 与 biUsd 保留两位小数
    df['afUsd'] = df['afUsd'].apply(lambda x: round(x, 2))
    df['biUsd'] = df['biUsd'].apply(lambda x: round(x, 2))

    df['diff'] = df['afUsd'] - df['biUsd']
    df['diff'] = df['diff'].apply(lambda x: round(x, 2))
    df.to_csv('/src/data/afLogStep3Ret.csv', index=False)
    print(df['afUsd'].sum())
    print(df['biUsd'].sum())
    print(df['diff'].sum())

def getAfEvent():
    filename = f'/src/data/afEvent20240430.csv'
    if not os.path.exists(filename):
        sql = f'''
select
appsflyer_id,
SUM(
  case when event_timestamp - install_timestamp between 0 and 86400
  then event_revenue_usd else 0
  end
) as 24hours_revenue
from ods_platform_appsflyer_events
where
app_id = 'id6448786147'
and event_name in ('af_purchase','af_purchase_oldusers')
and zone = 0
and day between '20240430' and '20240501'
and install_time between  '2024-04-30 00:00:00' and '2024-04-30 23:59:59'
group by
appsflyer_id
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)

    return df

def step4():
    afDf = getAfEvent()
    afLogDf = pd.read_csv('afLog.csv')
    df1 = afLogDf[['appsflyer_id normal','sumrevenue']]
    df2 = afLogDf[['appsflyer_id skan sdk update','max cv']]


    df = afDf.merge(df1, how='outer', left_on='appsflyer_id', right_on='appsflyer_id normal')
    print(df['24hours_revenue'].sum())
    print(df['sumrevenue'].sum())

    df.to_csv('/src/data/afLogStep4.csv', index=False)

def getAfEvent2():
    filename = f'/src/data/afEvent2_20240430.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    appsflyer_id,
    event_name,
    customer_user_id,
    SUM(
    case when event_timestamp - install_timestamp between 0 and 86400
    then event_revenue_usd else 0
    end
    ) as 24hours_revenue
from 
    ods_platform_appsflyer_events
where
    app_id = 'id6448786147'
    and event_name in ('af_purchase','af_purchase_oldusers')
    and zone = 0
    and day between '20240430' and '20240501'
    and install_time between  '2024-04-30 00:00:00' and '2024-04-30 23:59:59'
group by
    appsflyer_id,
    event_name,
    customer_user_id
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)

    return df



def step5():
    # 首先，先要确认BI与AF的主要差异体现在old users
    # 再找到为什么会错打old users

    afDf = getAfEvent2()
    afDf = afDf[afDf['24hours_revenue']>0.01]
    afDf.rename(columns={
        'customer_user_id':'uid',
        '24hours_revenue':'24h_revenue_af'
    }, inplace=True)

    
    biDf = getLwRevenue()
    biDf = biDf[biDf['r1usd']>0]
    biDf.rename(columns={
        'game_uid':'uid',
        'r1usd':'24h_revenue_bi'
    }, inplace=True)


    df = afDf.merge(biDf, how='outer', on='uid')
    df = df.sort_values(by='24h_revenue_bi', ascending=False)

    df.to_csv('/src/data/afLogStep5.csv', index=False)

    print('af sum revenue:',df['24h_revenue_af'].sum())
    print('bi sum revenue:',df['24h_revenue_bi'].sum())

    df.fillna(0, inplace=True)
    diffDf = df.loc[df['24h_revenue_af'] != df['24h_revenue_bi']]

    diffDfGroupByEventName = diffDf.groupby(['event_name']).agg(
        {'24h_revenue_af':'sum','24h_revenue_bi':'sum'}
    ).reset_index()
    print(diffDfGroupByEventName)

    biBiggerDf = diffDf.loc[diffDf['24h_revenue_bi'] > diffDf['24h_revenue_af']]
    print('biBiggerDf count:',biBiggerDf['uid'].count())
    print('24h_revenue_bi sum:',biBiggerDf['24h_revenue_bi'].sum())
    print('24h_revenue_af sum:',biBiggerDf['24h_revenue_af'].sum())


def debug():
    df = pd.read_csv('afLog.csv')
    df1 = df[['appsflyer_id normal','sumrevenue']]
    print(df1['sumrevenue'].sum())

if __name__ == '__main__':
    # readAfLog()
    # cvCheck()

    # getBIData()
    # step4()
    step5()
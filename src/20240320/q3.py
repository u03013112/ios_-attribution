# 评定融合归因的媒体效果

# 输入参数：
# 1. 时间段，开始时间，结束时间

# 步骤：
# 1.从BI获得这段时间的所有用户的24小时付费，7日付费。其中24小时付费按照目前的CV Map映射成CV。
#   按照campaign id，media，cv分组，统计人数，24小时付费，7日付费
# 2.统计每个CV的7日付费/24小时付费，即付费增长率，作为基准
#   统计每个媒体的CV付费增长率，与大盘的CV付费增长率进行比较
# 3.统计大盘的CV分布，作为基准
#   统计每个媒体的CV分布，与大盘的CV分布进行比较


# 数据源

# 表 lastwar_ios_funplus02_adv_uid_mutidays_campaign2
# 列 customer_id, insall_date, campaign_id, rate, day

# 表 ads_lastwar_ios_purchase_adv
# 列 game_uid, install_timestamp, event_timestamp, revenue_value_usd, day

# 表 dwb_overseas_mediasource_campaign_map
# 列 campaign_id, mediasource

# lastwar_ios_funplus02_adv_uid_mutidays_campaign2 和 dwb_overseas_mediasource_campaign_map 可以通过 campaign_id 连接，在原有的基础上，增加 mediasource 列。叫做结果表1。
# 查询ads_lastwar_ios_purchase_adv 按照gameuid分组，统计24小时付费，7日付费。即拥有列gameuid，24小时付费，7日付费。叫做结果表2。
# 最后将结果表1 和 结果表2 按照gameuid连接，得到最终结果表。叫做结果表3。

import os
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


# 返回df拥有列campaign_id,mediasource,cv,r1usd,r7usd
def step1(startDayStr,endDayStr):
    filename = f'/src/data/zk/q3_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):

        cvStr = 'CASE\n'
        cvMapDf = getCvMap()
        for index, row in cvMapDf.iterrows():
            if row["conversion_value"] == 0:
                continue
            if index == len(cvMapDf) - 1:
                cvStr += f'WHEN t2.r1usd > {row["min_event_revenue"]} THEN {row["conversion_value"]}\n'
            else:
                cvStr += f'WHEN t2.r1usd > {row["min_event_revenue"]} AND t2.r1usd <= {row["max_event_revenue"]} THEN {row["conversion_value"]}\n'
        cvStr += 'ELSE 0\nEND as cv'

        sql = f'''
    SET odps.sql.timezone=Africa/Accra;
    set odps.sql.hive.compatible=true;

    @mediasource_campaign_map :=
    SELECT
        campaign_id,
        MIN(mediasource) AS mediasource
    FROM
        dwb_overseas_mediasource_campaign_map
    GROUP BY
        campaign_id;

    @t1 := SELECT
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2.day,
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2.customer_user_id,
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2.install_date,
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2.campaign_id,
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2.rate,
    mediasource_campaign_map.mediasource
    FROM
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2
    left JOIN
    @mediasource_campaign_map mediasource_campaign_map
    ON
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2.campaign_id = mediasource_campaign_map.campaign_id
    where
    lastwar_ios_funplus02_adv_uid_mutidays_campaign2.day between '{startDayStr}' and '{endDayStr}'
    ;

    @t2 := SELECT
    game_uid as customer_user_id,
    COALESCE(
    SUM(
        CASE
        WHEN event_timestamp - install_timestamp between 0 and 24 * 3600 THEN revenue_value_usd
        ELSE 0
        END
    ),
    0
    ) as r1usd,
    COALESCE(
    SUM(
        CASE
        WHEN event_timestamp - install_timestamp between 0 and 7 * 24 * 3600 THEN revenue_value_usd
        ELSE 0
        END
    ),
    0
    ) as r7usd
    FROM
    rg_bi.ads_lastwar_ios_purchase_adv
    GROUP BY
    game_uid
    ;

    @t3 :=
    select
        t1.day,
        t1.customer_user_id,
        t1.campaign_id,
        t1.mediasource,
        t1.rate,
        t2.r1usd,
        t2.r7usd,
        t1.rate * t2.r1usd as r1usd_m,
        t1.rate * t2.r7usd as r7usd_m,
        {cvStr}
    from @t1 t1
    join @t2 t2
    on t1.customer_user_id = t2.customer_user_id
    ;

    select
    campaign_id,
    mediasource,
    cv,
    sum(rate) as uid_count,
    sum(r1usd_m) as r1usd,
    sum(r7usd_m) as r7usd
    from @t3
    group by
    cv,
    campaign_id,
    mediasource
    ;
    '''

        print(sql)
        df = execSql(sql)
        print(df)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df

# 获得大盘的cv count，r1usd，r7usd
def step2(startDayStr,endDayStr):
    filename = f'/src/data/zk/q3_2_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
SET odps.sql.timezone=Africa/Accra;
set odps.sql.hive.compatible=true;

@t1 := SELECT
game_uid as customer_user_id,
COALESCE(
  SUM(
    CASE
    WHEN event_timestamp - install_timestamp between 0
    and 24 * 3600 THEN revenue_value_usd
    ELSE 0
    END
  ),
  0
) as r1usd,
COALESCE(
  SUM(
    CASE
    WHEN event_timestamp - install_timestamp between 0
    and 7 * 24 * 3600 THEN revenue_value_usd
    ELSE 0
    END
  ),
  0
) as r7usd
FROM
  ads_lastwar_ios_purchase_adv
where
  install_day between '{startDayStr}' and '{endDayStr}'
GROUP BY
game_uid;

@t2 := select
  customer_user_id,
  r1usd,
  r7usd,
  CASE
  WHEN t1.r1usd > 0.0 AND t1.r1usd <= 0.99 THEN 1.0
  WHEN t1.r1usd > 0.99 AND t1.r1usd <= 1.15 THEN 2.0
  WHEN t1.r1usd > 1.15 AND t1.r1usd <= 1.3 THEN 3.0
  WHEN t1.r1usd > 1.3 AND t1.r1usd <= 2.98 THEN 4.0
  WHEN t1.r1usd > 2.98 AND t1.r1usd <= 3.41 THEN 5.0
  WHEN t1.r1usd > 3.41 AND t1.r1usd <= 5.98 THEN 6.0
  WHEN t1.r1usd > 5.98 AND t1.r1usd <= 7.46 THEN 7.0
  WHEN t1.r1usd > 7.46 AND t1.r1usd <= 9.09 THEN 8.0
  WHEN t1.r1usd > 9.09 AND t1.r1usd <= 12.05 THEN 9.0
  WHEN t1.r1usd > 12.05 AND t1.r1usd <= 14.39 THEN 10.0
  WHEN t1.r1usd > 14.39 AND t1.r1usd <= 18.17 THEN 11.0
  WHEN t1.r1usd > 18.17 AND t1.r1usd <= 22.07 THEN 12.0
  WHEN t1.r1usd > 22.07 AND t1.r1usd <= 26.57 THEN 13.0
  WHEN t1.r1usd > 26.57 AND t1.r1usd <= 32.09 THEN 14.0
  WHEN t1.r1usd > 32.09 AND t1.r1usd <= 37.42 THEN 15.0
  WHEN t1.r1usd > 37.42 AND t1.r1usd <= 42.94 THEN 16.0
  WHEN t1.r1usd > 42.94 AND t1.r1usd <= 50.34 THEN 17.0
  WHEN t1.r1usd > 50.34 AND t1.r1usd <= 58.56 THEN 18.0
  WHEN t1.r1usd > 58.56 AND t1.r1usd <= 67.93 THEN 19.0
  WHEN t1.r1usd > 67.93 AND t1.r1usd <= 80.71 THEN 20.0
  WHEN t1.r1usd > 80.71 AND t1.r1usd <= 100.32 THEN 21.0
  WHEN t1.r1usd > 100.32 AND t1.r1usd <= 116.94 THEN 22.0
  WHEN t1.r1usd > 116.94 AND t1.r1usd <= 130.41 THEN 23.0
  WHEN t1.r1usd > 130.41 AND t1.r1usd <= 153.76 THEN 24.0
  WHEN t1.r1usd > 153.76 AND t1.r1usd <= 196.39 THEN 25.0
  WHEN t1.r1usd > 196.39 AND t1.r1usd <= 235.93 THEN 26.0
  WHEN t1.r1usd > 235.93 AND t1.r1usd <= 292.07 THEN 27.0
  WHEN t1.r1usd > 292.07 AND t1.r1usd <= 424.48 THEN 28.0
  WHEN t1.r1usd > 424.48 AND t1.r1usd <= 543.77 THEN 29.0
  WHEN t1.r1usd > 543.77 AND t1.r1usd <= 753.61 THEN 30.0
  WHEN t1.r1usd > 753.61 THEN 31.0
  ELSE 0
  END as cv
from @t1 t1
;

select
  count(distinct customer_user_id) as uid_count,
  sum(r1usd) as r1usd,
  sum(r7usd) as r7usd,
  cv
from @t2
group by cv
;
'''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename)
    return df


# 和目前bi对数用，对数结果，基本吻合。
def debug():
    startDayStr = '20240215'
    endDayStr = '20240215'
    df1 = step1(startDayStr,endDayStr)

    df = df1.groupby(['mediasource']).agg('sum')
    print(df)

# cv分组
cvGroupList = [
    {'name':'low','min':0,'max':10},
    {'name':'mid','min':11,'max':21},
    {'name':'high','min':22,'max':31}
]

def df1CvUserRate(startDayStr,endDayStr):
    df1 = step1(startDayStr,endDayStr)
    groupByMediaDf = df1.groupby(['mediasource','cv']).agg('sum').reset_index()

    mediaList = groupByMediaDf['mediasource'].unique()
    for media in mediaList:
        mediaDf = groupByMediaDf[groupByMediaDf['mediasource'] == media].copy()
        mediaDf['uid_count rate'] = mediaDf['uid_count'] / mediaDf['uid_count'].sum()
        mediaDf['r1usd rate'] = mediaDf['r1usd'] / mediaDf['r1usd'].sum()
        mediaDf['r7usd rate'] = mediaDf['r7usd'] / mediaDf['r7usd'].sum()
        # 将uid_count rate，r1usd rate，r7usd rate，cv rate，改为百分比，保留2位小数
        mediaDf['uid_count'] = mediaDf['uid_count'].apply(lambda x: int(x))
        mediaDf['uid_count rate'] = mediaDf['uid_count rate'].apply(lambda x: format(x, '.2%'))
        mediaDf['r1usd rate'] = mediaDf['r1usd rate'].apply(lambda x: format(x, '.2%'))
        mediaDf['r7usd rate'] = mediaDf['r7usd rate'].apply(lambda x: format(x, '.2%'))
        mediaDf.to_csv(f'/src/data/zk/q3_{media}_{startDayStr}_{endDayStr}.csv', index=False)

def df1CvGroupUserRate(startDayStr,endDayStr):
    df1 = step1(startDayStr,endDayStr)
    # 按照cv分组进行分组
    df1['cvGroup'] = 'unknown'
    for cvGroup in cvGroupList:
        df1.loc[(df1['cv']>=cvGroup['min']) & (df1['cv']<=cvGroup['max']),'cvGroup'] = cvGroup['name']
    groupByCvGroupDf = df1.groupby(['mediasource','cvGroup']).agg('sum').reset_index()

    mediaList = groupByCvGroupDf['mediasource'].unique()
    for media in mediaList:
        mediaDf = groupByCvGroupDf[groupByCvGroupDf['mediasource'] == media]
        mediaDf['uid_count rate'] = mediaDf['uid_count'] / mediaDf['uid_count'].sum()
        mediaDf['r1usd rate'] = mediaDf['r1usd'] / mediaDf['r1usd'].sum()
        mediaDf['r7usd rate'] = mediaDf['r7usd'] / mediaDf['r7usd'].sum()
        # 将uid_count rate，r1usd rate，r7usd rate，cv rate，改为百分比，保留2位小数
        mediaDf['uid_count'] = mediaDf['uid_count'].apply(lambda x: int(x))
        mediaDf['uid_count rate'] = mediaDf['uid_count rate'].apply(lambda x: format(x, '.2%'))
        mediaDf['r1usd rate'] = mediaDf['r1usd rate'].apply(lambda x: format(x, '.2%'))
        mediaDf['r7usd rate'] = mediaDf['r7usd rate'].apply(lambda x: format(x, '.2%'))
        mediaDf.to_csv(f'/src/data/zk/q3_{media}_{startDayStr}_{endDayStr}_cv_group.csv', index=False)

def df2CvUserRate(startDayStr,endDayStr):
    df2 = step2(startDayStr,endDayStr)
    df2['uid_count rate'] = df2['uid_count'] / df2['uid_count'].sum()
    df2['r1usd rate'] = df2['r1usd'] / df2['r1usd'].sum()
    df2['r7usd rate'] = df2['r7usd'] / df2['r7usd'].sum()
    # 将uid_count rate，r1usd rate，r7usd rate，cv rate，改为百分比，保留2位小数
    df2['uid_count rate'] = df2['uid_count rate'].apply(lambda x: format(x, '.2%'))
    df2['r1usd rate'] = df2['r1usd rate'].apply(lambda x: format(x, '.2%'))
    df2['r7usd rate'] = df2['r7usd rate'].apply(lambda x: format(x, '.2%'))
    df2.to_csv(f'/src/data/zk/q3_d2_{startDayStr}_{endDayStr}.csv', index=False)

def df2CvGroupUserRate(startDayStr,endDayStr):
    df2 = step2(startDayStr,endDayStr)
    # 按照cv分组进行分组
    df2['cvGroup'] = 'unknown'
    for cvGroup in cvGroupList:
        df2.loc[(df2['cv']>=cvGroup['min']) & (df2['cv']<=cvGroup['max']),'cvGroup'] = cvGroup['name']
    groupByCvGroupDf = df2.groupby(['cvGroup']).agg('sum').reset_index()
    groupByCvGroupDf['uid_count rate'] = groupByCvGroupDf['uid_count'] / groupByCvGroupDf['uid_count'].sum()
    groupByCvGroupDf['r1usd rate'] = groupByCvGroupDf['r1usd'] / groupByCvGroupDf['r1usd'].sum()
    groupByCvGroupDf['r7usd rate'] = groupByCvGroupDf['r7usd'] / groupByCvGroupDf['r7usd'].sum()
    # 将uid_count rate，r1usd rate，r7usd rate，cv rate，改为百分比，保留2位小数
    groupByCvGroupDf['uid_count rate'] = groupByCvGroupDf['uid_count rate'].apply(lambda x: format(x, '.2%'))
    groupByCvGroupDf['r1usd rate'] = groupByCvGroupDf['r1usd rate'].apply(lambda x: format(x, '.2%'))
    groupByCvGroupDf['r7usd rate'] = groupByCvGroupDf['r7usd rate'].apply(lambda x: format(x, '.2%'))
    groupByCvGroupDf.to_csv(f'/src/data/zk/q3_d2_cv_group_{startDayStr}_{endDayStr}.csv', index=False)

# 付费增长率
def df1CvPay(startDayStr,endDayStr):
    df1 = step1(startDayStr,endDayStr)
    groupByMediaDf = df1.groupby(['mediasource','cv']).agg('sum').reset_index()

    mediaList = groupByMediaDf['mediasource'].unique()
    for media in mediaList:
        mediaDf = groupByMediaDf[groupByMediaDf['mediasource'] == media]
        mediaDf['r7usd/r1usd'] = mediaDf['r7usd'] / mediaDf['r1usd']
        mediaDf.to_csv(f'/src/data/zk/q3_{media}_{startDayStr}_{endDayStr}_pay.csv', index=False)

def df1CvGroupPay(startDayStr,endDayStr):
    df1 = step1(startDayStr,endDayStr)
    # 按照cv分组进行分组
    df1['cvGroup'] = 'unknown'
    for cvGroup in cvGroupList:
        df1.loc[(df1['cv']>=cvGroup['min']) & (df1['cv']<=cvGroup['max']),'cvGroup'] = cvGroup['name']
    groupByCvGroupDf = df1.groupby(['mediasource','cvGroup']).agg('sum').reset_index()

    mediaList = groupByCvGroupDf['mediasource'].unique()
    for media in mediaList:
        mediaDf = groupByCvGroupDf[groupByCvGroupDf['mediasource'] == media]
        mediaDf['r7usd/r1usd'] = mediaDf['r7usd'] / mediaDf['r1usd']
        mediaDf.to_csv(f'/src/data/zk/q3_{media}_{startDayStr}_{endDayStr}_pay_group.csv', index=False)

def df2CvPay(startDayStr,endDayStr):
    df2 = step2(startDayStr,endDayStr)
    df2['r7usd/r1usd'] = df2['r7usd'] / df2['r1usd']
    df2.to_csv(f'/src/data/zk/q3_d2_pay_{startDayStr}_{endDayStr}.csv', index=False)

def df2CvGroupPay(startDayStr,endDayStr):
    df2 = step2(startDayStr,endDayStr)
    # 按照cv分组进行分组
    df2['cvGroup'] = 'unknown'
    for cvGroup in cvGroupList:
        df2.loc[(df2['cv']>=cvGroup['min']) & (df2['cv']<=cvGroup['max']),'cvGroup'] = cvGroup['name']
    groupByCvGroupDf = df2.groupby(['cvGroup']).agg('sum').reset_index()
    groupByCvGroupDf['r7usd/r1usd'] = groupByCvGroupDf['r7usd'] / groupByCvGroupDf['r1usd']
    groupByCvGroupDf.to_csv(f'/src/data/zk/q3_d2_pay_group_{startDayStr}_{endDayStr}.csv', index=False)

# 24小时无付费用户平均7日付费金额
def R0R7(startDayStr,endDayStr):
    df2 = step2(startDayStr,endDayStr)
    count = df2.loc[df2['cv'] == 0,'uid_count'].sum()
    r7 = df2.loc[df2['cv'] == 0,'r7usd'].sum()
    r0r7 = r7 / count
    print(f'大盘 24小时无付费用户 7日付费每用户:{r0r7}')

    df1 = step1(startDayStr,endDayStr)
    df1 = df1.loc[df1['cv'] == 0]
    mediaList = df1['mediasource'].unique()
    for media in mediaList:
        mediaDf = df1.loc[df1['mediasource'] == media]
        count = mediaDf['uid_count'].sum()
        r7 = mediaDf['r7usd'].sum()
        r0r7 = r7 / count
        print(f'{media} 24小时无付费用户 7日付费每用户:{r0r7}')


def userCount(startDayStr,endDayStr):
    df1 = step1(startDayStr,endDayStr)
    mediaList = df1['mediasource'].unique()
    for media in mediaList:
        mediaDf = df1.loc[df1['mediasource'] == media]
        count = mediaDf['uid_count'].sum()
        print(f'{media} 用户数:{count}')
    

def cvFenBu():
    totalDf = pd.read_csv('/src/data/zk/q3_d2_20240215_20240229.csv')
    mediaList = [
        {'name':'FB','csvFilename':'/src/data/zk/q3_Facebook\ Ads_20240215_20240229.csv'},
        {'name':'Google','csvFilename':'/src/data/zk/q3_googleadwords_int_20240215_20240229.csv'},
        {'name':'bytedance','csvFilename':'/src/data/zk/q3_bytedanceglobal_int_20240215_20240229.csv'},
        {'name':'applovin','csvFilename':'/src/data/zk/q3_applovin_int_20240215_20240229.csv'},
    ]
    
if __name__ == '__main__':
    startDayStr = '20240215'
    endDayStr = '20240229'
    # startDayStr = '20240304'
    # endDayStr = '20240306'

    R0R7(startDayStr,endDayStr)
    # userCount(startDayStr,endDayStr)

    # df1CvUserRate(startDayStr,endDayStr)
    # df1CvGroupUserRate(startDayStr,endDayStr)
    # df2CvUserRate(startDayStr,endDayStr)
    # df2CvGroupUserRate(startDayStr,endDayStr)

    # df1CvPay(startDayStr,endDayStr)
    # df1CvGroupPay(startDayStr,endDayStr)
    # df2CvPay(startDayStr,endDayStr)
    # df2CvGroupPay(startDayStr,endDayStr)

    

    
    
    

    
    
    
    
    
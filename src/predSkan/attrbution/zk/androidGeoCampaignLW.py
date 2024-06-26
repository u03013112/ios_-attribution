# 按照国家撞库
# 与androidGeo.py区别是，撞库的维度是campaign，不再是媒体

# 思路
# 1、安卓数据->skan报告
# 2、只针对3个媒体，Facebook Ads,googleadwords_int,bytedanceglobal_int
# 3、针对几个比较大的campaign，这个需要计算一下，考虑每个媒体选取前2个campaign
# 4、针对6个campaign，分别计算每个campaign的MAPE

import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def getDataFromMC():
    # 获得用户信息，这里要额外获得归因信息，精确到campaign
    sql = '''
        WITH installs AS (
            SELECT
                appsflyer_id AS uid,
                to_char(
                    to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                    "yyyy-mm-dd"
                ) AS install_date,
                install_timestamp,
                media_source,
                country_code,
                campaign_id
            FROM
                ods_platform_appsflyer_events
            WHERE
                zone = '0'
                and app = 502
                and app_id = 'com.fun.lastwar.gp'
                AND event_name = 'install'
                AND day BETWEEN '20240201'AND '20240315'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2024-02-01', "yyyy-mm-dd")
                AND to_date('2024-03-07', "yyyy-mm-dd")
        ),
        purchases AS (
            SELECT
                appsflyer_id AS uid,
                event_timestamp,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                event_name = 'af_purchase'
                AND zone = 0
                AND day BETWEEN '20240201'AND '20240315'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2024-02-01', "yyyy-mm-dd")
                AND to_date('2024-03-07', "yyyy-mm-dd")
        )
        SELECT
            installs.uid,
            installs.install_date,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 86400
                ),
                0
            ) AS r1usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 2 * 86400
                ),
                0
            ) AS r2usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 3 * 86400
                ),
                0
            ) AS r3usd,
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 7 * 86400
                ),
                0
            ) AS r7usd,
            installs.install_timestamp,
            COALESCE(
                max(purchases.event_timestamp) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + 1 * 86400
                ),
                0
            ) AS last_timestamp,
            installs.media_source,
            installs.country_code,
            installs.campaign_id
        FROM
            installs
            LEFT JOIN purchases ON installs.uid = purchases.uid
        GROUP BY
            installs.uid,
            installs.install_date,
            installs.install_timestamp,
            installs.media_source,
            installs.country_code,
            installs.campaign_id
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('androidFp07'), index=False)
    return df

def loadData():
    # 加载数据
    print('loadData from file:',getFilename('androidFp07'))
    df = pd.read_csv(getFilename('androidFp07'))
    print('loadData done')
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 media 中 'restricted' 改为 'Facebook Ads'
    df['media'] = df['media'].replace('restricted','Facebook Ads')

    return df

def getCountryFromCampaign():
    # 获得campaign对应的国家信息
    # sql = '''
    #     select
    #         day,
    #         media_source,
    #         campaign_id,
    #         country_code,
    #         cost
    #     from
    #         ods_platform_appsflyer_masters
    #     where
    #         app_id = 'com.fun.lastwar.gp'
    #         AND day BETWEEN '20240201' AND '20240315'
    #         AND app = '502'
    #         AND cost >= 1
    #     ;
    # '''

    sql = '''
        SELECT
            install_day as day,
            mediasource as media_source,
            campaign_id,
            country as country_code,
            sum(cost_value_usd) as cost
        FROM
            rg_bi.dwd_overseas_cost_allproject
        WHERE
            app = 502
            AND app_package = 'com.fun.lastwar.gp'
            AND cost_value_usd >= 1 
            AND install_day BETWEEN '20240201' AND '20240315'
        group by
            install_day,
            campaign_id,
            mediasource,
            country
        ;
    '''
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('campaignGeo'), index=False)
    return df

# 改一下格式
def getCountryFromCampaign2():
    df = pd.read_csv(getFilename('campaignGeo'))
    # 统计country_code为空的数量
    print('country_code为空的数量',df['country_code'].isnull().sum())
    # 打印country_code为空的行
    print(df[df['country_code'].isnull()])

    df['country_code'].fillna('unknown', inplace=True)

    # 对结果进行分组，并将country_code连接成逗号分隔的字符串
    groupedDf = df.groupby(['day', 'media_source', 'campaign_id']).agg({
        'country_code': lambda x: '|'.join(sorted(set(x)))
    }).reset_index()

    # 重命名country_code列为country_code_list
    groupedDf.rename(columns={'country_code': 'country_code_list'}, inplace=True)

    groupedDf.to_csv(getFilename('campaignGeo2'), index=False)
    return df

def makeLevels1(userDf, usd='r1usd', N=32):
    # `makeLevels1`函数接受一个包含用户数据的DataFrame（`userDf`），一个表示用户收入的列名（`usd`，默认为'r1usd'），以及分组的数量（`N`，默认为8）。
    # 其中第0组特殊处理，第0组是收入等于0的用户。
    # 过滤收入大于0的用户进行后续分组，分为N-1组，每组的总收入大致相等。
    # 根据收入列（`usd`）对用户DataFrame（`userDf`）进行排序。
    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值。
    # 计算所有这些用户的总收入。
    # 计算每组的目标收入（总收入除以分组数量）。
    # 初始化当前收入（`current_usd`）和组索引（`group_index`）。
    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入。然后，将该用户的收入值存储到`levels`数组中，并将当前收入重置为0，组索引加1。当组索引达到N-1时，停止遍历。
    # 返回`levels`数组。
    
    # 过滤收入大于0的用户
    filtered_df = userDf[userDf[usd] > 0]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df[usd].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N)

    # 初始化当前收入（`current_usd`）和组索引（`group_index`）
    current_usd = 0
    group_index = 0

    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入
    for index, row in df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            # 将该用户的收入值存储到`levels`数组中
            levels[group_index] = row[usd]
            # 将当前收入重置为0，组索引加1
            current_usd = 0
            group_index += 1
            # 当组索引达到N-1时，停止遍历
            if group_index == N - 1:
                break

    return levels

def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0],
        'avg':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        min = mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1]
        max = levels[i]
        mapData['min_event_revenue'].append(min)
        mapData['max_event_revenue'].append(max)
        mapData['avg'].append((min+max)/2)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def addCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = int(cv1)
        
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = int(cv1)
    return userDfCopy

def dataStep1_24():
    df = pd.read_csv(getFilename('androidFp07'), converters={'campaign_id':str})
    df['media_source'] = df['media_source'].replace('restricted','Facebook Ads')
    # r1usd
    levels = makeLevels1(df,usd='r1usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r1usd',cv='cv')
    
    df.to_csv(getFilename('androidFpMergeDataStep1Campaign24'), index=False)
    print('dataStep1 24h done')    
    return df

def dataStep1_48():
    df = pd.read_csv(getFilename('androidFp07'), converters={'campaign_id':str})
    df['media_source'] = df['media_source'].replace('restricted','Facebook Ads')
    # r1usd
    levels = makeLevels1(df,usd='r2usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r2usd',cv='cv')
    
    df.to_csv(getFilename('androidFpMergeDataStep1Campaign48'), index=False)
    print('dataStep1 48h done')    
    return df

def dataStep2(df):
    df.rename(columns={'media_source':'media'},inplace=True)
    df = df [[
        'uid',
        'install_date',
        'r1usd',
        'r2usd',
        'r3usd',
        'r7usd',
        'install_timestamp',
        'last_timestamp',
        'media',
        'cv',
        'country_code',
        'campaign_id',
    ]]
    df.to_csv(getFilename('androidFpMergeDataStep2Campaign'), index=False)
    print('dataStep2 done')
    return df

# 暂时就只关心这3个媒体
mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int'
]

# 制作一个模拟的SKAN报告
def makeSKAN(df):
    # 过滤，只要媒体属于mediaList的条目
    df = df.loc[df['media'].isin(mediaList)]
    # 重排索引
    df = df.reset_index(drop=True)

    cvDf = df

    # 添加postback_timestamp
    # 如果用户的r1usd == 0，postback_timestamp = install_timestamp + 24小时 + 0~24小时之间随机时间
    # 如果用户的r1usd > 0，postback_timestamp = last_timestamp + 24小时 + 0~24小时之间随机时间
    # 添加postback_timestamp
    zero_r1usd_mask = cvDf['r1usd'] == 0
    non_zero_r1usd_mask = cvDf['r1usd'] > 0

    cvDf.loc[zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[zero_r1usd_mask, 'install_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=zero_r1usd_mask.sum())
    cvDf.loc[non_zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[non_zero_r1usd_mask, 'last_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=non_zero_r1usd_mask.sum())

    # postback_timestamp 转成 int
    cvDf['postback_timestamp'] = cvDf['postback_timestamp'].astype(int)

    # print(cvDf.head(30))

    skanDf = cvDf[['uid','postback_timestamp','media','cv','campaign_id','country_code']]
    # skanDf.to_csv(getFilename('skan2'), index=False)
    return skanDf

# 计算合法的激活时间范围
def skanAddValidInstallDate(skanDf):
    # 计算skan报告中的用户有效激活时间范围
    # 具体方式如下
    # cv = 0 的用户，有效时间范围是 (postback时间 - 48小时) ~ (postback时间 - 24小时)
    # cv > 0 的用户，有效时间范围是 (postback时间 - 72小时) ~ (postback时间 - 24小时)
    # 将每个用户的有效范围记录到skanDf中，记作min_valid_install_timestamp和max_valid_install_timestamp
    # 为了方便查看，请将postback时间戳和min_valid_install_timestamp和max_valid_install_timestamp都转换为日期格式也记录到skanDf中
    # 命名为postback_date，min_valid_install_date，max_valid_install_date
    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 48*3600).astype(int)
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 72*3600).astype(int)
    skanDf.loc[:, 'max_valid_install_timestamp'] = (skanDf['postback_timestamp'] - 24*3600).astype(int)

    # 将时间戳转换为日期格式
    skanDf['postback_date'] = pd.to_datetime(skanDf['postback_timestamp'], unit='s')
    skanDf['min_valid_install_date'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')
    return skanDf

# 激活时间范围 进行近似
# 近似成分钟
def skanValidInstallDate2Min(skanDf,N = 60):
    # skanDf 中列 min_valid_install_timestamp 和 max_valid_install_timestamp 按分钟取整
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'] // N * N
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'] // N * N
    skanDf['min_valid_install_date'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_date'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')

    return skanDf

# skan数据分组，按照media,cv,min_valid_install_timestamp和max_valid_install_timestamp分组，统计每组的用户数
def skanGroupby(skanDf):
    skanDf['user_count'] = 1

    skanGroupbyDf = skanDf.groupby(['media', 'cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp','campaign_id']).agg(
        {'user_count': 'sum'}
    ).reset_index()

    return skanGroupbyDf

def skanAddGeo(skanDf):
    print('skanAddGeo')
    campaignGeo2Df = pd.read_csv(getFilename('campaignGeo2'), converters={'campaign_id':str})
    campaignGeo2Df['day'] = pd.to_datetime(campaignGeo2Df['day'], format='%Y%m%d')

    # min_valid_install_timestamp 向前推7天，因为广告的转化窗口是7天
    # 但实际确实发现有部分转化时间超过7天的，这里放宽到8天
    skanDf['min_valid_install_timestamp'] -= 8*24*3600
    
    # 将时间戳列转换为datetime格式
    skanDf['min_valid_install_timestamp'] = pd.to_datetime(skanDf['min_valid_install_timestamp'], unit='s')
    skanDf['max_valid_install_timestamp'] = pd.to_datetime(skanDf['max_valid_install_timestamp'], unit='s')

    unmatched_rows = 0

    # 定义一个函数，用于根据campaign_id和时间戳范围查找匹配的country_code_list
    def get_country_code_list(row):
        matched_rows = campaignGeo2Df[
            (campaignGeo2Df['campaign_id'] == row['campaign_id']) &
            (campaignGeo2Df['day'] >= row['min_valid_install_timestamp']) &
            (campaignGeo2Df['day'] <= row['max_valid_install_timestamp'])
        ]

        if matched_rows.empty:
            # print('No matched rows for row: ', row)
            nonlocal unmatched_rows
            unmatched_rows += 1

        # 合并所有匹配行的country_code_list，排序并去重
        country_codes = set()
        for country_code_list in matched_rows['country_code_list']:
            country_codes.update(country_code_list.split('|'))

        return '|'.join(sorted(country_codes))

    # 应用函数，将匹配的country_code_list添加到skanDf
    tqdm.pandas(desc="Processing rows")
    skanDf['country_code_list'] = skanDf.progress_apply(get_country_code_list, axis=1)

    # 将min_valid_install_timestamp 和 max_valid_install_timestamp 重新转换为时间戳格式，单位秒
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(np.int64) // 10 ** 9
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'].astype(np.int64) // 10 ** 9

    # min_valid_install_timestamp 恢复，将上面减去的8天加回来
    skanDf['min_valid_install_timestamp'] += 8*24*3600

    # 计算未匹配的行数在总行数中的占比
    unmatched_rows_ratio = unmatched_rows / len(skanDf)

    # 在函数结束时打印未匹配的行数以及未匹配的行数在总行数中的占比
    print(f"Unmatched rows: {unmatched_rows}")
    print(f"Unmatched rows ratio: {unmatched_rows_ratio:.2%}")

    return skanDf
    

# 制作待归因用户Df
def makeUserDf(df):
    userDf = df
    userDf = userDf[['uid','install_timestamp','r1usd','r2usd','r3usd','r7usd','cv','country_code','campaign_id','media']].copy()
    userDf['cv'] = userDf['cv'].astype(int)
    return userDf

def userInstallDate2Min(userDf,N = 60):
    userDf['install_timestamp'] = userDf['install_timestamp'] // N * N
    return userDf

# user数据分组，按照install_timestamp和cv进行分组，统计每组的用户数和r7usd（汇总）
def userGroupby(userDf):
    # 按照install_timestamp和cv进行分组，统计每组的用户数和r7usd（汇总）
    # 将分组结果保存到userGroupbyDf中
    # userGroupbyDf的列名为install_timestamp,cv,user_count和r7usd
    # user_count是每组的用户数
    # r7usd是每组的r7usd汇总
    userGroupbyDf = userDf.groupby(['install_timestamp','cv','country_code']).agg({'uid':'count','r1usd':'sum','r2usd':'sum','r3usd':'sum','r7usd':'sum'}).reset_index()
    userGroupbyDf.rename(columns={'uid':'user_count'}, inplace=True)
    return userGroupbyDf

from tqdm import tqdm
# def meanAttribution(userDf, skanDf):
#     campaignList = getChooseCampaignList()
#     for campaignId in campaignList:
#         userDf['%s rate'%(campaignId)] = 0

#     # 将country_code_list列的空值填充为空字符串
#     skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')
#     userDf['attribute'] = userDf.apply(lambda x: [], axis=1)

#     # 待分配的skan条目的索引
#     pending_skan_indices = skanDf.index.tolist()

#     N = 3 # 最多进行3次分配
#     for i in range(N):  
#         print(f"开始第 {i + 1} 次分配")

#         new_pending_skan_indices = []

#         # 使用过滤条件选择要处理的skanDf行
#         skanDf_to_process = skanDf.loc[pending_skan_indices]
#         print(f"待处理的skanDf行数：{len(skanDf_to_process)}")
#         for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
#             campaignId = item['campaign_id']
#             cv = item['cv']
#             min_valid_install_timestamp = item['min_valid_install_timestamp']
#             max_valid_install_timestamp = item['max_valid_install_timestamp']
#             # print('min_valid_install_timestamp',min_valid_install_timestamp)
#             # print('max_valid_install_timestamp',max_valid_install_timestamp)

#             if i == N-2:
#                 min_valid_install_timestamp -= 24*3600
#             if i == N-1:
#                 # 由于经常有分不出去的情况，所以最后一次分配，不考虑国家
#                 item_country_code_list = ''
#                 min_valid_install_timestamp -= 48*3600
#                 # print('最后一次分配，不考虑国家，且时间范围向前推一天')
#                 # print(item)
#             else:
#                 item_country_code_list = item['country_code_list']

#             if cv < 0:
#                 # print('cv is null')
#                 if item_country_code_list == '':
#                     condition = (
#                         (userDf['install_timestamp'] >= min_valid_install_timestamp) &
#                         (userDf['install_timestamp'] <= max_valid_install_timestamp) &
#                         (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
#                     )
#                 else:
#                     country_code_list = item_country_code_list.split('|')
#                     condition = (
#                         (userDf['install_timestamp'] >= min_valid_install_timestamp) &
#                         (userDf['install_timestamp'] <= max_valid_install_timestamp) &
#                         (userDf['country_code'].isin(country_code_list)) &
#                         (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
#                     )
#             else:
#                 # 先检查item_country_code_list是否为空
#                 if item_country_code_list == '':
#                     condition = (
#                         (userDf['cv'] == cv) &
#                         (userDf['install_timestamp'] >= min_valid_install_timestamp) &
#                         (userDf['install_timestamp'] <= max_valid_install_timestamp) &
#                         (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
#                     )
#                 else:
#                     country_code_list = item_country_code_list.split('|')
#                     condition = (
#                         (userDf['cv'] == cv) &
#                         (userDf['install_timestamp'] >= min_valid_install_timestamp) &
#                         (userDf['install_timestamp'] <= max_valid_install_timestamp) &
#                         (userDf['country_code'].isin(country_code_list)) &
#                         (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
#                     )

#             matching_rows = userDf[condition]
#             total_matching_count = matching_rows['user_count'].sum()

#             if total_matching_count > 0:
#                 rate = item['user_count'] / total_matching_count
#                 userDf.loc[condition, 'attribute'] = userDf.loc[condition, 'attribute'].apply(lambda x: x + [{'campaignId': campaignId, 'skan index': index, 'rate': rate}])
#             else:
#                 new_pending_skan_indices.append(index)
#                 print(f"没有匹配到用户",item)

#         # 找出需要重新分配的行
#         rows_to_redistribute = userDf[userDf['attribute'].apply(lambda x: sum([item['rate'] for item in x]) > 1)]
#         print(f"需要重新分配的行数：{len(rows_to_redistribute)}")
#         # 对每一行，找出需要重新分配的skan条目，并将它们添加到new_pending_skan_indices列表中
#         for _, row in tqdm(rows_to_redistribute.iterrows(), total=len(rows_to_redistribute)):
#             attribute_list = row['attribute']
#             total_rate = sum([item['rate'] for item in attribute_list])
#             max_rate_to_remove = total_rate - 1

#             attribute_list_sorted = sorted(attribute_list, key=lambda x: x['rate'])
#             removed_items = []
#             removed_rate = 0

#             for item in attribute_list_sorted:
#                 if removed_rate + item['rate'] <= max_rate_to_remove:
#                     removed_rate += item['rate']
#                     removed_items.append(item)
#                 else:
#                     break

#             for item in removed_items:
#                 attribute_list.remove(item)
#                 new_pending_skan_indices.append(item['skan index'])

#         pending_skan_indices = new_pending_skan_indices
#         # pending_skan_indices 要进行排重
#         pending_skan_indices = list(set(pending_skan_indices))

#         print(f"第 {i + 1} 次分配结束，还有 {len(pending_skan_indices)} 个待分配条目")
#         pendingDf = skanDf.loc[pending_skan_indices]
        
#         print('待分配的skan数量：')
#         print(pendingDf.groupby('campaign_id').size())

#     for campaignId in campaignList:
#         userDf[campaignId + ' rate'] = userDf['attribute'].apply(lambda x: sum([item['rate'] for item in x if item['campaignId'] == campaignId]))

#     userDf = userDf.drop(columns=['attribute'])
#     return userDf

def meanAttribution(userDf, skanDf):
    campaignList = getChooseCampaignList()
    for campaignId in campaignList:
        userDf['%s count'%(campaignId)] = 0

    unmatched_rows = 0
    unmatched_user_count = 0

    # 将country_code_list列的空值填充为空字符串
    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')

    for index, row in tqdm(skanDf.iterrows(), total=len(skanDf)):
        campaignId = row['campaign_id']
        cv = row['cv']
        min_valid_install_timestamp = row['min_valid_install_timestamp']
        max_valid_install_timestamp = row['max_valid_install_timestamp']
        
        # 先检查row['country_code_list']是否为空
        if row['country_code_list'] == '':
            condition = (
                (userDf['cv'] == cv) &
                (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                (userDf['install_timestamp'] <= max_valid_install_timestamp)
            )
        else:
            country_code_list = row['country_code_list'].split('|')
            condition = (
                (userDf['cv'] == cv) &
                (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                (userDf['install_timestamp'] <= max_valid_install_timestamp) &
                (userDf['country_code'].isin(country_code_list))
            )

        matching_rows = userDf[condition]
        num_matching_rows = len(matching_rows)

        if num_matching_rows > 0:
            z = row['user_count']
            m = matching_rows['user_count'].sum()
            count = z / m

            userDf.loc[condition, '%s count'%(campaignId)] += count
        else:
            print(f"Unmatched row: {row}")
            unmatched_rows += 1
            unmatched_user_count += row['user_count']

    unmatched_ratio = unmatched_rows / len(skanDf)
    unmatched_user_count_ratio = unmatched_user_count / skanDf['user_count'].sum()
    print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
    print(f"Unmatched user count ratio: {unmatched_user_count_ratio:.2%}")

    return userDf

import gc
def meanAttributionFastv2(userDf, skanDf):
    campaignList = getChooseCampaignList()
    for campaignId in campaignList:
        userDf['%s rate'%(campaignId)] = 0

    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')
    
    pending_skan_indices = skanDf.index.tolist()
    N = 4
    attributeDf = pd.DataFrame(columns=['user index', 'campaignId', 'skan index', 'rate'])

    for i in range(N):  
        user_indices = []
        campaignIds = []
        skan_indices = []
        rates = []
        print(f"开始第 {i + 1} 次分配")
        new_pending_skan_indices = []
        skanDf_to_process = skanDf.loc[pending_skan_indices]
        print(f"待处理的skanDf行数：{len(skanDf_to_process)}")
        
        # 在每次循环开始时，预先计算每一行的media rate的总和
        userDf['total media rate'] = userDf.apply(lambda x: sum([x[campaignId + ' rate'] for campaignId in campaignList]), axis=1)
        if i == 1:
            print('第%d次分配，时间范围向前推一天'%(i+1))
        if i >= 2:
            print('第%d次分配，不考虑国家，且时间范围向前推两天'%(i+1))
        
        for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
            campaignId = str(item['campaign_id'])
            cv = item['cv']
            min_valid_install_timestamp = item['min_valid_install_timestamp']
            max_valid_install_timestamp = item['max_valid_install_timestamp']
            
            if i == 1:
                min_valid_install_timestamp -= 24*3600
            if i >= 2:
                item_country_code_list = ''
                min_valid_install_timestamp -= 48*3600
            else:
                item_country_code_list = item['country_code_list']

            # 将所有的匹配条件都单独写出来
            # condition_rate = userDf.apply(lambda x: sum([x[media + ' rate'] for media in mediaList]) < 0.95, axis=1)
            # 使用预先计算的media rate总和进行匹配
            condition_rate = userDf['total media rate'] < 0.95
            condition_time = (userDf['install_timestamp'] >= min_valid_install_timestamp) & (userDf['install_timestamp'] <= max_valid_install_timestamp)
            condition_country = userDf['country_code'].isin(item_country_code_list.split('|')) if item_country_code_list != '' else pd.Series([True] * len(userDf))
            condition_cv = userDf['cv'] == cv if cv >= 0 else pd.Series([True] * len(userDf))

            if cv < 0:
                if item_country_code_list == '':
                    condition = condition_rate & condition_time
                else:
                    condition = condition_rate & condition_time & condition_country
            else:
                if item_country_code_list == '':
                    condition = condition_rate & condition_time & condition_cv
                else:
                    condition = condition_rate & condition_time & condition_cv & condition_country

            matching_rows = userDf[condition]
            total_matching_count = matching_rows['user_count'].sum()

            if total_matching_count > 0:
                rate = item['user_count'] / total_matching_count

                userDf.loc[condition, 'total media rate'] += rate
                user_indices.extend(matching_rows.index)
                campaignIds.extend([campaignId] * len(matching_rows))
                skan_indices.extend([index] * len(matching_rows))
                rates.extend([rate] * len(matching_rows))
                # print(user_indices)
            else:
                new_pending_skan_indices.append(index)

        print('未分配成功：', len(new_pending_skan_indices))
        attributeDf2 = pd.DataFrame({'user index': user_indices, 'campaignId': campaignIds, 'skan index': skan_indices, 'rate': rates})
        # print('len user_indices:',len(user_indices))
        # print('attributeDf2:',attributeDf2)
        print('0')
        attributeDf = attributeDf.append(attributeDf2, ignore_index=True)
        # print('attributeDf:',attributeDf)
        print('1')
        # 找出需要重新分配的行
        grouped_attributeDf = attributeDf.groupby('user index')['rate'].sum()
        print('2')
        index_to_redistribute = grouped_attributeDf[grouped_attributeDf > 1].index
        print('3')
        sorted_rows_to_redistribute = attributeDf[attributeDf['user index'].isin(index_to_redistribute)].sort_values(
            ['user index', 'rate'], ascending=[True, False])
        print('4')
        sorted_rows_to_redistribute['cumulative_rate'] = sorted_rows_to_redistribute.groupby('user index')['rate'].cumsum()
        print('5')
        # 找出需要移除的行
        rows_to_remove = sorted_rows_to_redistribute[sorted_rows_to_redistribute['cumulative_rate'] > 1]
        print('6')
        # 记录需要移除的skan index
        removed_skan_indices = set(rows_to_remove['skan index'])
        print('7')
        # 从attributeDf中移除这些行
        attributeDf = attributeDf[~attributeDf['skan index'].isin(removed_skan_indices)]
        # print('attributeDf:',attributeDf)
        print('移除过分配的skan：', len(removed_skan_indices),'条')

        # 更新待分配的skan索引列表
        pending_skan_indices = list(set(new_pending_skan_indices).union(removed_skan_indices))

        print(f"第 {i + 1} 次分配结束，还有 {len(pending_skan_indices)} 个待分配条目")
        
        # 更新media rate
        for campaignId in campaignList:
            userDf[campaignId + ' rate'] = 0
            userDf[campaignId + ' rate'] = attributeDf[attributeDf['campaignId'] == campaignId].groupby('user index')['rate'].sum()
            userDf[campaignId + ' rate'] = userDf[campaignId + ' rate'].fillna(0)
        
        # 计算每个媒体的未分配的用户数
        pending_counts = skanDf.loc[pending_skan_indices].groupby('campaign_id')['user_count'].sum()

        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('campaign_id')['user_count'].sum()

        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts

        # 将三个计算结果合并为一个DataFrame
        result_df = pd.concat([total_counts, pending_counts, pending_ratios], axis=1)

        # 设置列名和索引
        result_df.columns = ['总skan用户数', '未分配用户数', '未分配比例']
        result_df.index.name = 'campaign_id'

        # 将未分配比例转换为2位小数的百分比
        result_df['未分配比例'] = result_df['未分配比例'].apply(lambda x: f"{x*100:.2f}%")

        # 打印结果
        print(result_df)

        # 计算所有的未分配用户占比
        total_pending_ratio = pending_counts.sum() / total_counts.sum()
        print("所有的未分配用户占比：")
        print(total_pending_ratio)

        gc.collect()

    columnsDict = {}
    for campaignId in campaignList:
        columnsDict[campaignId + ' rate'] = campaignId + ' count'

    userDf.rename(columns=columnsDict,inplace=True)

    return userDf



def meanAttributionResult(userDf, mediaList=mediaList):
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date
    campaignList = getChooseCampaignList()
    # Calculate campaignId r7usd
    for campaignId in campaignList:
        campaignId_count_col = campaignId + ' count'
        userDf[campaignId + ' r1usd'] = userDf['r1usd'] * userDf[campaignId_count_col]
        userDf[campaignId + ' r7usd'] = userDf['r7usd'] * userDf[campaignId_count_col]
        userDf[campaignId + ' user_count'] = userDf['user_count'] * userDf[campaignId_count_col]

    # 分割userDf为两个子数据框，一个包含r1usd，另一个包含r7usd
    userDf_r1usd = userDf[['install_date'] + [campaignId + ' r1usd' for campaignId in campaignList]]
    userDf_r7usd = userDf[['install_date'] + [campaignId + ' r7usd' for campaignId in campaignList]]

    # 对两个子数据框分别进行melt操作
    userDf_r1usd = userDf_r1usd.melt(id_vars=['install_date'], var_name='campaign_id', value_name='r1usd')
    userDf_r1usd['campaign_id'] = userDf_r1usd['campaign_id'].str.replace(' r1usd', '')
    userDf_r1usd = userDf_r1usd.groupby(['install_date', 'campaign_id']).sum().reset_index()
    
    userDf_r7usd = userDf_r7usd.melt(id_vars=['install_date'], var_name='campaign_id', value_name='r7usd')
    userDf_r7usd['campaign_id'] = userDf_r7usd['campaign_id'].str.replace(' r7usd', '')
    userDf_r7usd = userDf_r7usd.groupby(['install_date', 'campaign_id']).sum().reset_index()


    # 将两个子数据框连接在一起
    userDf = userDf_r1usd.merge(userDf_r7usd, on=['install_date', 'campaign_id'])
    
    # Save to CSV
    # userDf.to_csv(getFilename('attribution1Ret48'), index=False)
    return userDf

from sklearn.metrics import r2_score
def checkRet(retDf,prefix='attCampaign24_'):
    # 读取原始数据
    rawDf = loadData()

    campaignDf = pd.read_csv(getFilename('campaignGeo'), converters={'campaign_id':str})
    campaignDf = campaignDf.groupby(['media_source','campaign_id']).agg('sum').reset_index()
    campaignDf = campaignDf[['campaign_id','media_source']]
    campaignList = getChooseCampaignList()
    rawDf = rawDf[rawDf['campaign_id'].isin(campaignList)]
    rawDf = rawDf.merge(campaignDf, on='campaign_id', how='left')
    # 将install_timestamp转为install_date
    rawDf['install_date'] = pd.to_datetime(rawDf['install_timestamp'], unit='s').dt.date
    rawDf['user_count'] = 1
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['campaign_id', 'media_source','install_date']).agg(
        {
            'r1usd': 'sum',
            'r2usd': 'sum',
            'r7usd': 'sum',
            'user_count':'sum'
        }).reset_index()

    # rawDf 和 retDf 进行合并
    # retDf.rename(columns={'r7usd':'r7usdp'}, inplace=True)
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['campaign_id', 'install_date'], how='left',suffixes=('', 'p'))
    # 计算MAPE
    rawDf['MAPE'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf.loc[rawDf['r7usd'] == 0,'MAPE'] = 0
    # rawDf = rawDf.loc[rawDf['install_date']<'2023-02-01']
    rawDf = rawDf.loc[rawDf['MAPE']>0]
    rawDf.to_csv(getFilename(prefix+'attribution24RetCheck'), index=False)

    # 计算整体的MAPE和R2
    totalDf = rawDf.groupby('install_date').agg({'r7usd': 'sum','r7usdp': 'sum'}).reset_index()
    totalDf['MAPE'] = abs(totalDf['r7usd'] - totalDf['r7usdp']) / totalDf['r7usd']
    MAPE = totalDf['MAPE'].mean()
    # r2 = r2_score(rawDf['r7usd'], rawDf['r7usdp'])
    print('MAPE:', MAPE)
    # print('R2:', r2)

    for campaignId in campaignList:
        mediaDf = rawDf[rawDf['campaign_id'] == campaignId].copy()
        MAPE = mediaDf['MAPE'].mean()
        
        # 计算r7usd和r7usdp的7日均线的MAPE
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']
        MAPE7 = mediaDf['MAPE7'].mean()
        print(f"\nmedia:{mediaDf['media_source'].max()}:")
        print(f"campaignId: {campaignId}, MAPE: {MAPE}")
        print(f"campaignId: {campaignId}, MAPE7: {MAPE7}")

    df = pd.read_csv(getFilename(prefix+'attribution24RetCheck'))
    r7PR1 = df['r7usd'] / df['r1usd']
    print(r7PR1.mean())
    r7pPR1 = df['r7usdp'] / df['r1usd']
    # 排除r7pPR1 = inf的情况
    r7pPR1 = r7pPR1[r7pPR1 != np.inf]
    print(r7pPR1.mean())

    return df

# 按照Media进行分组
def checkRetByMedia(retDf,prefix='attCampaign24_'):
    # 读取原始数据
    rawDf = loadData()

    campaignDf = pd.read_csv(getFilename('campaignGeo'), converters={'campaign_id':str})
    campaignDf = campaignDf.groupby(['media_source','campaign_id']).agg('sum').reset_index()
    campaignDf = campaignDf[['campaign_id','media_source']]
    campaignList = getChooseCampaignList()
    rawDf = rawDf[rawDf['campaign_id'].isin(campaignList)]
    rawDf = rawDf.merge(campaignDf, on='campaign_id', how='left')
    # 将install_timestamp转为install_date
    rawDf['install_date'] = pd.to_datetime(rawDf['install_timestamp'], unit='s').dt.date
    rawDf['user_count'] = 1
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['campaign_id', 'media_source','install_date']).agg(
        {
            'r1usd': 'sum',
            'r2usd': 'sum',
            'r7usd': 'sum',
            'user_count':'sum'
        }).reset_index()

    # rawDf 和 retDf 进行合并
    # retDf.rename(columns={'r7usd':'r7usdp'}, inplace=True)
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['campaign_id', 'install_date'], how='left',suffixes=('', 'p'))
    # 按照media_source进行分组
    rawDf = rawDf.groupby(['media_source','install_date']).agg({'r7usd': 'sum','r7usdp': 'sum'}).reset_index()
    # 计算MAPE
    rawDf['MAPE'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf.loc[rawDf['r7usd'] == 0,'MAPE'] = 0
    # rawDf = rawDf.loc[rawDf['install_date']<'2023-02-01']
    rawDf = rawDf.loc[rawDf['MAPE']>0]
    rawDf.to_csv(getFilename(prefix+'attribution24RetCheckByMedia'), index=False)

    for media in rawDf['media_source'].unique():
        mediaDf = rawDf[rawDf['media_source'] == media].copy()
        MAPE = mediaDf['MAPE'].mean()
        print(f"\nmedia:{media}:")
        print(f"MAPE: {MAPE}")

        # 计算r7usd和r7usdp的7日均线的MAPE
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']
        MAPE7 = mediaDf['MAPE7'].mean()
        print(f"MAPE7: {MAPE7}")
        
    return 


import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DateFormatter

def draw24(df,prefix='attCampaign24_',label = 'r1usd'):
    # df = pd.read_csv(getFilename('attribution24RetCheck'), converters={'campaign_id':str})
    # 将df中的campaign_id转成str类型
    df['campaign_id'] = df['campaign_id'].astype(str)

    # 将不同的媒体分开画图，图片宽一点
    # install_date作为x轴，每隔7天画一个点
    # 双y轴，y1是r7usd和r7usdp；y2是MAPE（用虚线）。
    # 图片保存到'/src/data/zk/att1_{media}.jpg'
    # Convert 'install_date' to datetime
    df['install_date'] = pd.to_datetime(df['install_date'])

    campaignList = getChooseCampaignList()
    for campaignId in campaignList:
        media_df = df[df['campaign_id'] == campaignId]
        
        # Create the plot with the specified figure size
        fig, ax1 = plt.subplots(figsize=(24, 6))

        plt.title(campaignId)

        # Plot r7usd and r7usdp on the left y-axis
        ax1.plot(media_df['install_date'], media_df['r7usd'], label='r7usd')
        ax1.plot(media_df['install_date'], media_df['r7usdp'], label='r7usdp')
        ax1.plot(media_df['install_date'], media_df['r2usd'], label=label)
        ax1.set_ylabel('r7usd and r7usdp')
        ax1.set_xlabel('Install Date')

        # Plot MAPE on the right y-axis with dashed line
        ax2 = ax1.twinx()
        ax2.plot(media_df['install_date'], media_df['MAPE'], label='MAPE', linestyle='--', color='red')
        ax2.set_ylabel('MAPE')

        # Set x-axis to display dates with a 7-day interval
        ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        plt.xticks(media_df['install_date'][::14], rotation=45)

        # Add legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')

        # Save the plot as a jpg image
        plt.savefig(f'/src/data/zk2/{prefix}{campaignId}.jpg', bbox_inches='tight')
        plt.close()

        # 再画一张，r7usd和r7usdp的7日均线的图

        fig, ax3 = plt.subplots(figsize=(24, 6))

        mediaDf = media_df.copy()
        mediaDf['r2usd7'] = mediaDf['r2usd'].rolling(7).mean()
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']

        plt.title(campaignId + 'rolling 7 days')

        ax3.plot(mediaDf['install_date'], mediaDf['r7usd7'], label='r7usd7')
        ax3.plot(mediaDf['install_date'], mediaDf['r7usdp7'], label='r7usdp7')
        ax3.plot(mediaDf['install_date'], mediaDf['r2usd7'], label=f'{label}7')
        ax3.set_ylabel('r7usd7 and r7usdp7')
        ax3.set_xlabel('Install Date')

        ax4 = ax3.twinx()
        ax4.plot(mediaDf['install_date'], mediaDf['MAPE7'], label='MAPE7', linestyle='--', color='red')
        ax4.set_ylabel('MAPE7')

        # Set x-axis to display dates with a 7-day interval
        ax3.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        plt.xticks(mediaDf['install_date'][::14], rotation=45)

        # Add legends
        lines1, labels1 = ax3.get_legend_handles_labels()
        lines2, labels2 = ax4.get_legend_handles_labels()
        ax3.legend(lines1 + lines2, labels1 + labels2, loc='best')

        # Save the plot as a jpg image
        plt.savefig(f'/src/data/zk2/{prefix}rolling7_{campaignId}.jpg', bbox_inches='tight')
        plt.close()


def chooseCampaign(N = 2):
    # 选取campaign id，每个媒体选取2个花费最高的campaign。
    df = pd.read_csv(getFilename('campaignGeo'))
    df = df.groupby(['media_source','campaign_id']).agg({'cost':'sum'}).reset_index()
    df = df.sort_values(by=['media_source','cost'],ascending=False)
    df = df.groupby(['media_source']).head(N)

    df.to_csv(getFilename('chooseCampaign'), index=False)

def getChooseCampaignList():
    campaignDf = pd.read_csv(getFilename('chooseCampaign'))
    campaignDf = campaignDf.loc[campaignDf['media_source'].isin(mediaList)]
    # campaignDf['campaign_id'] 改为str类型
    campaignDf['campaign_id'] = campaignDf['campaign_id'].astype(str)
    campaignList = campaignDf['campaign_id'].tolist()
    print('getChooseCampaignList:',campaignList)
    return campaignList

def main24(fast = False,onlyCheck = False):
    if onlyCheck == False:
        if fast == False:
            # 24小时版本归因
            # getDataFromMC()
            getCountryFromCampaign()
            getCountryFromCampaign2()
            
            df = dataStep1_24()
            df2 = dataStep2(df)

            # 做一下简单的campaign过滤，只获得选中的campaign
            # chooseCampaign(N=5)
            campaignList = getChooseCampaignList()
            
            print(df2.head(5))
            df3 = df2.loc[df2['campaign_id'].isin(campaignList)].copy()
            print('df3 len:',len(df3))

            skanDf = makeSKAN(df3)
            skanDf = skanAddValidInstallDate(skanDf)

            print('skan data len:',len(skanDf))
            
            skanDf = skanValidInstallDate2Min(skanDf,N = 3600)
            skanDf = skanGroupby(skanDf)
            skanDf.to_csv(getFilename('skanAOSCampaignG24'),index=False)
            print('skan data group len:',len(skanDf))

            skanDf = skanAddGeo(skanDf)
            skanDf.to_csv(getFilename('skanAOSCampaignG24Geo'), index=False)

            userDf = makeUserDf(df2.copy())
            print('user data len:',len(userDf))
            
            userDf = userInstallDate2Min(userDf,N = 3600)
            userDf = userGroupby(userDf)
            userDf.to_csv(getFilename('userAOSCampaignG24'),index=False)
            print('user data group len:',len(userDf))
        else:
            userDf = pd.read_csv(getFilename('userAOSCampaignG24'))
            skanDf = pd.read_csv(getFilename('skanAOSCampaignG24Geo'))
        
        # just for test
        # skanDf = skanDf.head(100)

        # userDf = meanAttribution(userDf, skanDf)
        userDf = meanAttributionFastv2(userDf, skanDf)
        userDf.to_csv(getFilename('meanAttribution24'), index=False)
        userDf = pd.read_csv(getFilename('meanAttribution24'))
        userDf = meanAttributionResult(userDf)
        userDf.to_csv(getFilename('meanAttributionResult24'), index=False)

    userDf = pd.read_csv(getFilename('meanAttributionResult24'), converters={'campaign_id':str})
    df = checkRet(userDf)
    checkRetByMedia(userDf)
    draw24(df,prefix='attCampaign24_')

def main48(fast = False,onlyCheck = False):
    if onlyCheck == False:
        if fast == False:
            # 48小时版本归因
            # getDataFromMC()
            # getCountryFromCampaign()
            # getCountryFromCampaign2()

            df = dataStep1_48()
            df2 = dataStep2(df)

            # 做一下简单的campaign过滤，只获得选中的campaign
            campaignList = getChooseCampaignList()
            df3 = df2.loc[df2['campaign_id'].isin(campaignList)].copy()
            skanDf = makeSKAN(df3)
            skanDf = skanAddValidInstallDate(skanDf)

            print('skan data len:',len(skanDf))
            
            skanDf = skanValidInstallDate2Min(skanDf,N = 3600)
            skanDf = skanGroupby(skanDf)
            skanDf.to_csv(getFilename('skanAOSCampaignG48'),index=False)
            print('skan data group len:',len(skanDf))

            skanDf = skanAddGeo(skanDf)
            skanDf.to_csv(getFilename('skanAOSCampaignG48Geo'), index=False)

            userDf = makeUserDf(df2.copy())
            print('user data len:',len(userDf))
            
            userDf = userInstallDate2Min(userDf,N = 3600)
            userDf = userGroupby(userDf)
            userDf.to_csv(getFilename('userAOSCampaignG48'),index=False)
            print('user data group len:',len(userDf))

        else:
            userDf = pd.read_csv(getFilename('userAOSCampaignG48'))
            skanDf = pd.read_csv(getFilename('skanAOSCampaignG48Geo'), converters={'campaign_id':str})
        
        # userDf = meanAttribution(userDf, skanDf)
        # skanDf = skanDf.head(100)
        userDf = meanAttributionFastv2(userDf, skanDf)
        userDf.to_csv(getFilename('meanAttribution48'), index=False)
        userDf = pd.read_csv(getFilename('meanAttribution48'))
        userDf = meanAttributionResult(userDf)
        userDf.to_csv(getFilename('meanAttributionResult48'), index=False)

    userDf = pd.read_csv(getFilename('meanAttributionResult48'), converters={'campaign_id':str})
    df = checkRet(userDf,prefix='attCampaign48_')
    checkRetByMedia(userDf,prefix='attCampaign48_')
    draw24(df,prefix='attCampaign48_')

def debug():
    # campaign_id,install_date,r1usd,r2usd,r7usd,user_count,r1usdp,r7usdp,MAPE
    df24 = pd.read_csv(getFilename('attCampaign24_attribution24RetCheck'))
    df48 = pd.read_csv(getFilename('attCampaign48_attribution48RetCheck'))

    df24 = [['campaign_id','install_date','user_count','r1usd','r7usd','r7usdp']]
    df48 = [['campaign_id','install_date','r7usdp']]

    df = df24.merge(df48,on=['campaign_id','install_date'],how='left',sum_suffixes=('', '48'))
    df = df.sort_values(by=['campaign_id','install_date'],ascending=True)

    campaignIdList = []
    mapeList = []
    mape48List = []

    for campaignId in df['campaign_id'].unique():
        campaignDf = df[df['campaign_id'] == campaignId]
        campaignDf['r7usd rolling7'] = campaignDf['r7usd'].rolling(7).mean()
        campaignDf['r7usdp rolling7'] = campaignDf['r7usdp'].rolling(7).mean()
        campaignDf['r7usdp48 rolling7'] = campaignDf['r7usdp48'].rolling(7).mean()
        campaignDf['MAPE rolling7'] = abs(campaignDf['r7usd rolling7'] - campaignDf['r7usdp rolling7']) / campaignDf['r7usd rolling7']
        campaignDf['MAPE48 rolling7'] = abs(campaignDf['r7usd rolling7'] - campaignDf['r7usdp48 rolling7']) / campaignDf['r7usd rolling7']
        mape = campaignDf['MAPE rolling7'].mean()
        mape48 = campaignDf['MAPE48 rolling7'].mean()

        print(f"campaignId: {campaignId}, MAPE: {mape}, MAPE48: {mape48}")
        campaignIdList.append(campaignId)
        mapeList.append(mape)
        mape48List.append(mape48)

    retDf = pd.DataFrame({'campaign_id':campaignIdList,'MAPE':mapeList,'MAPE48':mape48List})
    retDf = retDf.sort_values(by=['MAPE'],ascending=False)

    retDf.to_csv(getFilename('debug'),index=False)


if __name__ == '__main__':
    # chooseCampaign()
    # print('main24')
    main24(fast = False,onlyCheck = False)
    # main24(fast = True,onlyCheck = True)
    # print('main48')
    # main48(fast = False,onlyCheck = False)
    main48(fast = True,onlyCheck = True)

    # debug()
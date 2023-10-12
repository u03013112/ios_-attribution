# 计算融合归因结论在中长期的表现，按照月的回收金额计算偏差
# 顺便把之前的代码进行一定程度的重构，使得代码更加清晰
# 将统计用户多日回收部分代码进行重构，使得代码更加清晰

import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

daysList = [1,7,14,30,60,90,120]

# 暂时就只关心这4个媒体
mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    # 'snapchat_int'
]


def getDataFromMC():
    daysSqlWithComma = ''
    for i in daysList:
        daysSqlWithComma += '''
            COALESCE(
                sum(purchases.event_revenue_usd) FILTER (
                    WHERE
                        purchases.event_timestamp <= installs.install_timestamp + %d*86400
                ),
                0
            ) AS r%dusd,
        '''%(i,i)
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
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND event_name = 'install'
                AND day BETWEEN '20230101'
                AND '20230730'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
                AND to_date('2023-07-01', "yyyy-mm-dd")
        ),
        purchases AS (
            SELECT
                appsflyer_id AS uid,
                event_timestamp,
                event_revenue_usd
            FROM
                ods_platform_appsflyer_events
            WHERE
                event_name in ('af_purchase_oldusers','af_purchase')
                AND zone = 0
                AND day BETWEEN '20230101'
                AND '20230730'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
                AND to_date('2023-07-01', "yyyy-mm-dd")
        )
        SELECT
            installs.uid,
            installs.install_date,
            %s
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
    '''%(daysSqlWithComma)
    print(sql)
    df = execSql(sql)
    df.to_csv(getFilename('20230908long0'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('20230908long0'))
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 media 中 'restricted' 改为 'Facebook Ads'
    df['media'] = df['media'].replace('restricted','Facebook Ads')

    return df

def getCountryFromCampaign():
    # 获得campaign对应的国家信息
    sql = '''
        select
            day,
            media_source,
            campaign_id,
            country_code,
            cost
        from
            ods_platform_appsflyer_masters
        where
            app_id = 'com.topwar.gp'
            AND day BETWEEN '20230101' AND '20230701'
            AND app = '102'
            AND cost >= 1
        ;
    '''
    df = execSql(sql)
    df.to_csv(getFilename('campaignGeo'), index=False)
    return df

# 改一下格式
def getCountryFromCampaign2():
    df = pd.read_csv(getFilename('campaignGeo'))
    df['country_code'].fillna('unknown', inplace=True)

    # 对结果进行分组，并将country_code连接成逗号分隔的字符串
    groupedDf = df.groupby(['day', 'media_source', 'campaign_id', 'cost']).agg({
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

def dataStep1():
    df = pd.read_csv(getFilename('20230908long0'))
    df['media_source'] = df['media_source'].replace('restricted','Facebook Ads')
    # r1usd
    levels = makeLevels1(df,usd='r1usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r1usd',cv='cv')
    
    df.to_csv(getFilename('20230908longStep1'), index=False)
    print('dataStep1 done')    

def dataStep2():
    df = pd.read_csv(getFilename('20230908longStep1'))
    df.rename(columns={'media_source':'media'},inplace=True)
    rUsdList = []
    for i in daysList:
        rUsdList.append('r%dusd'%i)
    df = df [[
        'uid',
        'install_date',
        'install_timestamp',
        'last_timestamp',
        'media',
        'cv',
        'country_code',
        'campaign_id',
    ] + rUsdList]
    df.to_csv(getFilename('20230908longStep2'), index=False)
    print('dataStep2 done')

# 制作一个模拟的SKAN报告
def makeSKAN():
    df = pd.read_csv(getFilename('20230908longStep2'))
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
    # skanDf = pd.read_csv(getFilename('skanAOS6G'), converters={'campaign_id':str})
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
def makeUserDf():
    userDf = pd.read_csv(getFilename('20230908longStep2'))

    rUsdList = []
    for i in daysList:
        rUsdList.append('r%dusd'%i)

    userDf = userDf[['uid','install_timestamp','cv','country_code','campaign_id','media'] + rUsdList]
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
    aggDict = {
        'uid':'count',
    }
    for i in daysList:
        aggDict['r%dusd'%i] = 'sum'
    userGroupbyDf = userDf.groupby(['install_timestamp','cv','country_code']).agg(aggDict).reset_index()
    userGroupbyDf.rename(columns={'uid':'user_count'}, inplace=True)
    return userGroupbyDf

import gc
from tqdm import tqdm
def meanAttributionFastv2(userDf, skanDf):
    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')
    
    pending_skan_indices = skanDf.index.tolist()
    N = 3
    attributeDf = pd.DataFrame(columns=['user index', 'media', 'skan index', 'rate'])

    # 初始化userDf中的media rate列
    mediaList = skanDf['media'].unique()
    for media in mediaList:
        userDf[media + ' rate'] = 0

    for i in range(N):  
        user_indices = []
        medias = []
        skan_indices = []
        rates = []
        print(f"开始第 {i + 1} 次分配")
        new_pending_skan_indices = []
        skanDf_to_process = skanDf.loc[pending_skan_indices]
        print(f"待处理的skanDf行数：{len(skanDf_to_process)}")
        
        # 在每次循环开始时，预先计算每一行的media rate的总和
        userDf['total media rate'] = userDf.apply(lambda x: sum([x[media + ' rate'] for media in mediaList]), axis=1)
        for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
            media = item['media']
            cv = item['cv']
            min_valid_install_timestamp = item['min_valid_install_timestamp']
            max_valid_install_timestamp = item['max_valid_install_timestamp']
            
            if i == N-2:
                min_valid_install_timestamp -= 24*3600
            if i == N-1:
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
                medias.extend([media] * len(matching_rows))
                skan_indices.extend([index] * len(matching_rows))
                rates.extend([rate] * len(matching_rows))
            else:
                new_pending_skan_indices.append(index)

        print('未分配成功：', len(new_pending_skan_indices))
        attributeDf2 = pd.DataFrame({'user index': user_indices, 'media': medias, 'skan index': skan_indices, 'rate': rates})
        print('0')
        attributeDf = attributeDf.append(attributeDf2, ignore_index=True)
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
        print('移除过分配的skan：', len(removed_skan_indices),'条')

        # 更新待分配的skan索引列表
        pending_skan_indices = list(set(new_pending_skan_indices).union(removed_skan_indices))

        print(f"第 {i + 1} 次分配结束，还有 {len(pending_skan_indices)} 个待分配条目")
        
        # 更新media rate
        for media in mediaList:
            userDf[media + ' rate'] = 0
            userDf[media + ' rate'] = attributeDf[attributeDf['media'] == media].groupby('user index')['rate'].sum()
            userDf[media + ' rate'] = userDf[media + ' rate'].fillna(0)

        # 计算每个媒体的未分配的用户数
        pending_counts = skanDf.loc[pending_skan_indices].groupby('media')['user_count'].sum()
        print("每个媒体的未分配的用户数：")
        print(pending_counts)
        
        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('media')['user_count'].sum()
        print("每个媒体的总的skan用户数：")
        print(total_counts)
        
        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts
        print("每个媒体的未分配占比：")
        print(pending_ratios)
        
        gc.collect()

    # 拆分customer_user_id
    # userDf['customer_user_id'] = userDf['customer_user_id'].apply(lambda x: x.split('|'))
    # userDf = userDf.explode('customer_user_id')

    return userDf



def meanAttributionResult(userDf, mediaList=mediaList):
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date

    # Calculate media r7usd
    for media in mediaList:
        media_count_col = media + ' count'
        
        for i in daysList:
            userDf[media + ' r%dusd'%i] = userDf['r%dusd'%i] * userDf[media_count_col]

        userDf[media + ' user_count'] = userDf['user_count'] * userDf[media_count_col]

    # 分割userDf为两个子数据框，一个包含r1usd，另一个包含r7usd
    userDfList = []
    for i in daysList:
        userDfI = userDf[['install_date'] + [media + ' r%dusd'%i for media in mediaList]]
        userDfI = userDfI.melt(id_vars=['install_date'], var_name='media', value_name='r%dusd'%i)
        userDfI['media'] = userDfI['media'].str.replace(' r%dusd'%i, '')
        userDfI = userDfI.groupby(['install_date', 'media']).sum().reset_index()
        userDfList.append(userDfI)

    # 还需要统计每个媒体的首日用户数
    userDf_count = userDf[['install_date'] + [media + ' user_count' for media in mediaList]]
    userDf_count = userDf_count.melt(id_vars=['install_date'], var_name='media', value_name='count')
    userDf_count['media'] = userDf_count['media'].str.replace(' user_count', '')
    userDf_count = userDf_count.groupby(['install_date', 'media']).sum().reset_index()
    # userDf_count.to_csv(getFilename('userDf_count'), index=False )
    # print(userDf_count.head())
    # ，和付费用户数
    userDf_payCount = userDf.loc[userDf['r1usd'] >0,['install_date'] + [media + ' user_count' for media in mediaList]]
    userDf_payCount = userDf_payCount.melt(id_vars=['install_date'], var_name='media', value_name='payCount')
    userDf_payCount['media'] = userDf_payCount['media'].str.replace(' user_count', '')
    userDf_payCount = userDf_payCount.groupby(['install_date', 'media']).sum().reset_index()
    # userDf_payCount.to_csv(getFilename('userDf_payCount'), index=False )
    # print(userDf_payCount.head())

    userDf = userDf_count.merge(userDf_payCount, on=['install_date', 'media'])
    for userDf0 in userDfList:
        userDf = userDf.merge(userDf0, on=['install_date', 'media'])

    # Save to CSV
    # userDf.to_csv(getFilename('attribution1Ret48'), index=False)
    return userDf

from sklearn.metrics import r2_score
def checkRet(retDf):
    # 读取原始数据
    rawDf = loadData()
    # 只保留mediaList的用户
    rawDf = rawDf[rawDf['media'].isin(mediaList)]
    # 将install_timestamp转为install_date
    rawDf['install_date'] = pd.to_datetime(rawDf['install_timestamp'], unit='s').dt.date
    rawDf['user_count'] = 1
    # 按照media和install_date分组，计算r7usd的和
    aggDict = {
        'user_count':'sum'
    }
    for i in daysList:
        aggDict['r%dusd'%i] = 'sum'
    rawDf = rawDf.groupby(['media', 'install_date']).agg(aggDict).reset_index()

    # rawDf 和 retDf 进行合并
    # retDf.rename(columns={'r7usd':'r7usdp'}, inplace=True)
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['media', 'install_date'], how='left',suffixes=('', 'p'))
    # 计算MAPE
    for i in daysList:
        rawDf['MAPE%d'%i] = abs(rawDf['r%dusd'%i] - rawDf['r%dusdp'%i]) / rawDf['r%dusd'%i]

    rawDf.loc[rawDf['r7usd'] == 0,'MAPE7'] = 0
    # rawDf = rawDf.loc[rawDf['install_date']<'2023-02-01']
    rawDf = rawDf.loc[rawDf['MAPE7']>0]
    rawDf.to_csv(getFilename('attributionRetCheckGeo24V2Long'), index=False)
    # 计算整体的MAPE和R2
    MAPE = rawDf['MAPE7'].mean()
    # R2 = r2_score(rawDf['r7usd'], rawDf['r7usdp'])
    print('MAPE:', MAPE)
    # print('R2:', R2)
    # 分媒体计算MAPE和R2
    for media in mediaList:
        mediaDf = rawDf[rawDf['media'] == media]
        MAPE = mediaDf['MAPE7'].mean()
        # R2 = r2_score(mediaDf['r7usd'], mediaDf['r7usdp'])
        # print(f"Media: {media}, MAPE: {MAPE}, R2: {R2}")
        print(f"Media: {media}, MAPE: {MAPE}")

    df = pd.read_csv(getFilename('attributionRetCheckGeo24V2Long'))
    r7PR1 = df['r7usd'] / df['r1usd']
    print(r7PR1.mean())
    r7pPR1 = df['r7usdp'] / df['r1usd']
    # 排除r7pPR1 = inf的情况
    r7pPR1 = r7pPR1[r7pPR1 != np.inf]
    print(r7pPR1.mean())

def long():
    df = pd.read_csv(getFilename('attributionRetCheckGeo24V2Long'))
    df = df[['media','install_date','r30usd','r60usd','r90usd','r120usd','r30usdp','r60usdp','r90usdp','r120usdp']]
    # 原install_date是类似2023-01-01的字符串，截取为2023-01
    df['install_date'] = df['install_date'].str.slice(0,7)
    df = df.groupby(['media','install_date']).sum().reset_index()
    df['MAPE30'] = abs(df['r30usd'] - df['r30usdp']) / df['r30usd']
    df['MAPE60'] = abs(df['r60usd'] - df['r60usdp']) / df['r60usd']
    df['MAPE90'] = abs(df['r90usd'] - df['r90usdp']) / df['r90usd']
    df['MAPE120'] = abs(df['r120usd'] - df['r120usdp']) / df['r120usd']
    df.to_csv(getFilename('20230908LongRet'), index=False)

def debug():
    df = pd.read_csv(getFilename('attributionRetCheckGeo24V2Long'))
    print(df.head())
    df = df[['media','install_date','r7usd','r7usdp']]
    # 为了有效的计算MAPE，需要过滤掉r7usd为0的数据
    df = df.loc[df['r7usd'] > 0]

    df2 = df.groupby(by = ['install_date']).agg({
        'r7usd':'sum',
        'r7usdp':'sum',
    })
    df2['MAPE'] = abs(df2['r7usd'] - df2['r7usdp']) / df2['r7usd']
    print('原本MAPE:',df2['MAPE'].mean())

def debug2():
    # huiwen的需求，获得按月汇总的数据的7日回收与1日回收的比值的MAPE
    df = pd.read_csv(getFilename('attributionRetCheckGeo24V2Long'))
    df = df[['media','install_date','r1usd','r7usd','r7usdp']]
    df['install_date'] = df['install_date'].str.slice(0,7)
    df = df.groupby(['media','install_date']).sum().reset_index()
    df['r7/r1'] = df['r7usd'] / df['r1usd']
    df['r7p/r1'] = df['r7usdp'] / df['r1usd']
    df['MAPE'] = abs(df['r7/r1'] - df['r7p/r1']) / df['r7/r1']
    df.to_csv(getFilename('20230908LongRet2'), index=False)

if __name__ == '__main__':
    # getDataFromMC()
    # # getCountryFromCampaign()
    # # getCountryFromCampaign2()

    # dataStep1()
    # dataStep2()

    # # 对于只是改了用户付费金额数据，可以不重做skan部分
    # skanDf = makeSKAN()
    # skanDf = skanAddValidInstallDate(skanDf)
    # print('skan data len:',len(skanDf))
    # skanDf.to_csv(getFilename('skanAOS6'),index=False)
    # skanDf = pd.read_csv(getFilename('skanAOS6'), converters={'campaign_id': str})
    # skanDf = skanValidInstallDate2Min(skanDf,N = 7200)
    # skanDf = skanGroupby(skanDf)
    # skanDf.to_csv(getFilename('skanAOS6G'),index=False)
    # print('skan data group len:',len(skanDf))

    # skanDf = skanAddGeo()
    # skanDf.to_csv(getFilename('skanAOS6G24'), index=False)

    # userDf = makeUserDf()
    # print('user data len:',len(userDf))
    # userDf.to_csv(getFilename('userAOS6Long'),index=False)
    # # userDf = pd.read_csv(getFilename('userAOS6Long'))
    # userDf = userInstallDate2Min(userDf,N = 7200)
    # userDf = userGroupby(userDf)
    # userDf.to_csv(getFilename('userAOS6G24_7200'),index=False)
    # print('user data group len:',len(userDf))

    # skanDf = pd.read_csv(getFilename('skanAOS6G24'))
    # userDf = pd.read_csv(getFilename('userAOS6G24_7200'))
    
    # userDf = meanAttributionFastv2(userDf, skanDf)
    # # # print(userDf)
    # userDf.to_csv(getFilename('attribution1ReStep24hoursGeoV2Long'), index=False)
    # # userDf = pd.read_csv(getFilename('attribution1ReStep24hoursGeoV2Long'))
    # userDf.rename(columns={
    #     'Facebook Ads rate':'Facebook Ads count',
    #     'googleadwords_int rate':'googleadwords_int count',
    #     'bytedanceglobal_int rate':'bytedanceglobal_int count',
    #     'snapchat_int rate':'snapchat_int count'
    # },inplace=True)
    # userDf = meanAttributionResult(userDf)
    # userDf.to_csv(getFilename('attribution1Ret24GeoV2Long'), index=False)
    # # userDf = pd.read_csv(getFilename('attribution1Ret24GeoV2Long'))
    # checkRet(userDf)

    # long()
    # debug()
    debug2()

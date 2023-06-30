# 按照国家撞库
# 基础逻辑还是撞库，与其他的撞库逻辑一样
# 区别是，按照campaign获得国家信息
# 然后按照国家信息撞库，这个逻辑需要重新写，效率会比较低
# 所以考虑用一个比较短的时间段来做这个事情

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
                app_id = 'com.topwar.gp'
                AND zone = 0
                AND event_name = 'install'
                AND day BETWEEN '20221001'
                AND '20230308'
                AND to_date(install_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-10-01', "yyyy-mm-dd")
                AND to_date('2023-03-01', "yyyy-mm-dd")
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
                AND day BETWEEN '20221001'
                AND '20230308'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2022-10-01', "yyyy-mm-dd")
                AND to_date('2023-03-01', "yyyy-mm-dd")
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
    df = execSql(sql)
    df.to_csv(getFilename('androidFp07'), index=False)
    return df

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp07'))
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
            AND day BETWEEN '20220701' AND '20230301'
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
    df = pd.read_csv(getFilename('androidFp07'))
    df['media_source'] = df['media_source'].replace('restricted','Facebook Ads')
    # r1usd
    levels = makeLevels1(df,usd='r2usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r2usd',cv='cv')
    
    df.to_csv(getFilename('androidFpMergeDataStep1g2'), index=False)
    print('dataStep1 done')    

def dataStep2():
    df = pd.read_csv(getFilename('androidFpMergeDataStep1g2'))
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
    df.to_csv(getFilename('androidFpMergeDataStep2g2'), index=False)
    print('dataStep2 done')

# 暂时就只关心这4个媒体
mediaList = [
    'googleadwords_int',
    'Facebook Ads',
    'bytedanceglobal_int',
    'snapchat_int'
]

# 制作一个模拟的SKAN报告
def makeSKAN():
    df = pd.read_csv(getFilename('androidFpMergeDataStep2g2'))
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
    skanDf.to_csv(getFilename('skan2'), index=False)
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

def skanAddGeo():
    skanDf = pd.read_csv(getFilename('skanAOS6G'), converters={'campaign_id':str})
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
    userDf = pd.read_csv(getFilename('androidFpMergeDataStep2g2'))

    userDf = userDf[['uid','install_timestamp','r1usd','r2usd','r3usd','r7usd','cv','country_code','campaign_id','media']]
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
def meanAttribution(userDf, skanDf):
    for media in mediaList:
        userDf['%s count'%(media)] = 0

    unmatched_rows = 0
    unmatched_user_count = 0

    # 将country_code_list列的空值填充为空字符串
    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')

    for index, row in tqdm(skanDf.iterrows(), total=len(skanDf)):
        media = row['media']
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

            userDf.loc[condition, '%s count'%(media)] += count
        else:
            print(f"Unmatched row: {row}")
            unmatched_rows += 1
            unmatched_user_count += row['user_count']

    unmatched_ratio = unmatched_rows / len(skanDf)
    unmatched_user_count_ratio = unmatched_user_count / skanDf['user_count'].sum()
    print(f"Unmatched rows ratio: {unmatched_ratio:.2%}")
    print(f"Unmatched user count ratio: {unmatched_user_count_ratio:.2%}")

    # userDf.to_csv(getFilename('attribution1ReStep48hoursGeo'), index=False)
    # userDf.to_parquet(getFilename('attribution1ReStep48hoursGeo','parquet'), index=False)
    return userDf

def meanAttributionResult(userDf, mediaList=mediaList):
    userDf['install_date'] = pd.to_datetime(userDf['install_timestamp'], unit='s').dt.date

    # Calculate media r7usd
    for media in mediaList:
        media_count_col = media + ' count'
        userDf[media + ' r1usd'] = userDf['r1usd'] * userDf[media_count_col]
        userDf[media + ' r7usd'] = userDf['r7usd'] * userDf[media_count_col]
        userDf[media + ' user_count'] = userDf['user_count'] * userDf[media_count_col]

    # 分割userDf为两个子数据框，一个包含r1usd，另一个包含r7usd
    userDf_r1usd = userDf[['install_date'] + [media + ' r1usd' for media in mediaList]]
    userDf_r7usd = userDf[['install_date'] + [media + ' r7usd' for media in mediaList]]

    # 对两个子数据框分别进行melt操作
    userDf_r1usd = userDf_r1usd.melt(id_vars=['install_date'], var_name='media', value_name='r1usd')
    userDf_r1usd['media'] = userDf_r1usd['media'].str.replace(' r1usd', '')
    userDf_r1usd = userDf_r1usd.groupby(['install_date', 'media']).sum().reset_index()
    # userDf_r1usd.to_csv(getFilename('userDf_r1usd'), index=False )
    # print(userDf_r1usd.head())
    
    userDf_r7usd = userDf_r7usd.melt(id_vars=['install_date'], var_name='media', value_name='r7usd')
    userDf_r7usd['media'] = userDf_r7usd['media'].str.replace(' r7usd', '')
    userDf_r7usd = userDf_r7usd.groupby(['install_date', 'media']).sum().reset_index()
    # userDf_r7usd.to_csv(getFilename('userDf_r7usd'), index=False )
    # print(userDf_r7usd.head())

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

    # 将两个子数据框连接在一起
    userDf = userDf_r1usd.merge(userDf_r7usd, on=['install_date', 'media'])
    # print('merge1')
    userDf = userDf.merge(userDf_count, on=['install_date', 'media'])
    # print('merge2')
    userDf = userDf.merge(userDf_payCount, on=['install_date', 'media'])
    # print('merge3')

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
    rawDf = rawDf.groupby(['media', 'install_date']).agg({'r7usd': 'sum','user_count':'sum'}).reset_index()

    # rawDf 和 retDf 进行合并
    # retDf.rename(columns={'r7usd':'r7usdp'}, inplace=True)
    # 为了防止merge不成功，将install_date转成字符串
    rawDf['install_date'] = rawDf['install_date'].astype(str)
    retDf['install_date'] = retDf['install_date'].astype(str)
    rawDf = rawDf.merge(retDf, on=['media', 'install_date'], how='left',suffixes=('', 'p'))
    # 计算MAPE
    rawDf['MAPE'] = abs(rawDf['r7usd'] - rawDf['r7usdp']) / rawDf['r7usd']
    rawDf.loc[rawDf['r7usd'] == 0,'MAPE'] = 0
    # rawDf = rawDf.loc[rawDf['install_date']<'2023-02-01']
    rawDf = rawDf.loc[rawDf['MAPE']>0]
    rawDf.to_csv(getFilename('attribution1RetCheck'), index=False)
    # 计算整体的MAPE和R2
    MAPE = rawDf['MAPE'].mean()
    # R2 = r2_score(rawDf['r7usd'], rawDf['r7usdp'])
    print('MAPE:', MAPE)
    # print('R2:', R2)
    # 分媒体计算MAPE和R2
    for media in mediaList:
        mediaDf = rawDf[rawDf['media'] == media]
        MAPE = mediaDf['MAPE'].mean()
        # R2 = r2_score(mediaDf['r7usd'], mediaDf['r7usdp'])
        # print(f"Media: {media}, MAPE: {MAPE}, R2: {R2}")
        print(f"Media: {media}, MAPE: {MAPE}")

    df = pd.read_csv(getFilename('attribution1RetCheck'))
    r7PR1 = df['r7usd'] / df['r1usd']
    print(r7PR1.mean())
    r7pPR1 = df['r7usdp'] / df['r1usd']
    # 排除r7pPR1 = inf的情况
    r7pPR1 = r7pPR1[r7pPR1 != np.inf]
    print(r7pPR1.mean())


# 给用户信息按照他的campaign_id添加国家信息，然后查看用户真实国家与campaign_id的国家一致程度
def userAndCampaignGeo():
    userDf = pd.read_csv(getFilename('userAOS6'), converters={'campaign_id':str})
    # 过滤掉没有campaign_id的用户
    userDf = userDf[userDf['campaign_id'].notnull()]
    userDf = userDf[userDf['campaign_id'] != '']
    # 过滤掉cv <= 0的用户
    userDf = userDf[userDf['cv'] > 0]

    mediaList = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int'
    ]
    userDf = userDf[userDf['media'].isin(mediaList)]
    # 每种media分别进行取一些用户，暂定1000人，然后合并，恢复成原始的userDf格式
    sample_size = 1000
    sampled_users = []

    # 对每种media进行分组
    grouped_users = userDf.groupby('media')

    # 对每组抽取指定数量的用户
    for _, group in grouped_users:
        sampled_users.append(group.sample(n=sample_size, replace=False))

    # 将抽样后的用户数据合并为一个新的数据框
    sampled_userDf = pd.concat(sampled_users)
    # 重排索引
    sampled_userDf = sampled_userDf.reset_index(drop=True)

    campaignGeo2Df = pd.read_csv(getFilename('campaignGeo2'), converters={'campaign_id':str})

    campaignGeo2Df['day'] = pd.to_datetime(campaignGeo2Df['day'], format='%Y%m%d')

    # min_valid_install_timestamp 向前推7天，因为广告的转化窗口是7天
    # 但实际确实发现有部分转化时间超过7天的，这里放宽到8天
    sampled_userDf['min_valid_install_timestamp'] = sampled_userDf['install_timestamp'] - 8 * 24 * 60 * 60
    sampled_userDf['max_valid_install_timestamp'] = sampled_userDf['install_timestamp']

    # 将时间戳列转换为datetime格式
    sampled_userDf['min_valid_install_timestamp'] = pd.to_datetime(sampled_userDf['min_valid_install_timestamp'], unit='s')
    sampled_userDf['max_valid_install_timestamp'] = pd.to_datetime(sampled_userDf['max_valid_install_timestamp'], unit='s')

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
    sampled_userDf['country_code_list'] = sampled_userDf.progress_apply(get_country_code_list, axis=1)

    # 将min_valid_install_timestamp 和 max_valid_install_timestamp 重新转换为时间戳格式，单位秒
    sampled_userDf['min_valid_install_timestamp'] = sampled_userDf['min_valid_install_timestamp'].astype(np.int64) // 10 ** 9
    sampled_userDf['max_valid_install_timestamp'] = sampled_userDf['max_valid_install_timestamp'].astype(np.int64) // 10 ** 9

    # 计算未匹配的行数在总行数中的占比
    unmatched_rows_ratio = unmatched_rows / len(sampled_userDf)

    # 在函数结束时打印未匹配的行数以及未匹配的行数在总行数中的占比
    print(f"Unmatched rows: {unmatched_rows}")
    print(f"Unmatched rows ratio: {unmatched_rows_ratio:.2%}")

    # 重命名skanDf为mergeDf
    mergeDf = sampled_userDf

    mergeDf.to_csv(getFilename('userAOS6G2'), index=False)
    return mergeDf

def userAndCampaignGeoCheck(df):
    # df = pd.read_csv(getFilename('userAOS6G2'))
    # df = pd.read_csv(getFilename('skanAOS6G3'))
    
    # df 列 中 country_code 和 country_code_list 都是字符串
    # 其中 country_code_list 是以 | 分隔的字符串
    # 判断 country_code 是否在 country_code_list 中
    # 并将结果记录在 country_code_in_list 列中，0代表不在，1代表在
    # 然后再统计 country_code_in_list 列中 0 和 1 的数量以及比例
    df = df[df['country_code_list'].notnull()]
    df['country_code_in_list'] = df.apply(lambda row: 1 if row['country_code'] in row['country_code_list'].split('|') else 0, axis=1)

    # 计算country_code_in_list列中0和1的数量
    count_0 = (df['country_code_in_list'] == 0).sum()
    count_1 = (df['country_code_in_list'] == 1).sum()

    # 计算0和1的比例
    total_count = len(df)
    ratio_0 = count_0 / total_count
    ratio_1 = count_1 / total_count

    # 输出结果
    print(f"0的数量: {count_0}, 0的比例: {ratio_0}")
    print(f"1的数量: {count_1}, 1的比例: {ratio_1}")

    # 返回结果，以便于后续操作
    return df

def skanAndCampaignGeo():
    userDf = pd.read_csv(getFilename('skanAOS6'), converters={'campaign_id':str})
    # 过滤掉没有campaign_id的用户
    userDf = userDf[userDf['campaign_id'].notnull()]
    userDf = userDf[userDf['campaign_id'] != '']
    # 过滤掉cv <= 0的用户
    userDf = userDf[userDf['cv'] > 0]

    mediaList = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int'
    ]
    userDf = userDf[userDf['media'].isin(mediaList)]
    # 每种media分别进行取一些用户，暂定1000人，然后合并，恢复成原始的userDf格式
    sample_size = 1000
    sampled_users = []

    # 对每种media进行分组
    grouped_users = userDf.groupby('media')

    # 对每组抽取指定数量的用户
    for _, group in grouped_users:
        sampled_users.append(group.sample(n=sample_size, replace=False))

    # 将抽样后的用户数据合并为一个新的数据框
    sampled_userDf = pd.concat(sampled_users)
    # 重排索引
    sampled_userDf = sampled_userDf.reset_index(drop=True)

    campaignGeo2Df = pd.read_csv(getFilename('campaignGeo2'), converters={'campaign_id':str})

    campaignGeo2Df['day'] = pd.to_datetime(campaignGeo2Df['day'], format='%Y%m%d')

    # min_valid_install_timestamp 向前推7天，因为广告的转化窗口是7天
    # 但实际确实发现有部分转化时间超过7天的，这里放宽到8天
    sampled_userDf['min_valid_install_timestamp'] -= 8 * 24 * 60 * 60

    # 将时间戳列转换为datetime格式
    sampled_userDf['min_valid_install_timestamp'] = pd.to_datetime(sampled_userDf['min_valid_install_timestamp'], unit='s')
    sampled_userDf['max_valid_install_timestamp'] = pd.to_datetime(sampled_userDf['max_valid_install_timestamp'], unit='s')

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
    sampled_userDf['country_code_list'] = sampled_userDf.progress_apply(get_country_code_list, axis=1)

    # 将min_valid_install_timestamp 和 max_valid_install_timestamp 重新转换为时间戳格式，单位秒
    sampled_userDf['min_valid_install_timestamp'] = sampled_userDf['min_valid_install_timestamp'].astype(np.int64) // 10 ** 9
    sampled_userDf['max_valid_install_timestamp'] = sampled_userDf['max_valid_install_timestamp'].astype(np.int64) // 10 ** 9

    # 计算未匹配的行数在总行数中的占比
    unmatched_rows_ratio = unmatched_rows / len(sampled_userDf)

    # 在函数结束时打印未匹配的行数以及未匹配的行数在总行数中的占比
    print(f"Unmatched rows: {unmatched_rows}")
    print(f"Unmatched rows ratio: {unmatched_rows_ratio:.2%}")

    # 重命名skanDf为mergeDf
    mergeDf = sampled_userDf

    mergeDf.to_csv(getFilename('skanAOS6G3'), index=False)
    return mergeDf

import datetime
def d1():
    userDf = pd.read_csv(getFilename('userAOS6G'))
    skanDf = pd.read_csv(getFilename('skanAOS6G2'))
    
    # 这里过滤一下，加快速度
    timestamp = datetime.datetime(2023, 2, 1, 0, 0, 0).timestamp()

    # userDf中 install_timestamp 是unix时间戳，单位秒。过滤，只要 2023-02-01 00:00:00 之后的数据
    print('2023-02-01 00:00:00 timestamp:', timestamp)
    userDf = userDf[userDf['install_timestamp'] >= timestamp]

    skanDf = skanDf[skanDf['min_valid_install_timestamp'] >= timestamp]

    userDf = meanAttribution(userDf, skanDf)
    userDf.to_csv(getFilename('attribution1ReStep24hoursGeoFast'), index=False)
    userDf = meanAttributionResult(userDf)
    userDf.to_csv(getFilename('attribution1Ret24Fast'), index=False)
    userDf = pd.read_csv(getFilename('attribution1Ret24Fast'))
    checkRet(userDf)

def d2():
    userDf = pd.read_csv(getFilename('userAOS6G48'))
    skanDf = pd.read_csv(getFilename('skanAOS6G48'))
    
    # 这里过滤一下，加快速度
    timestamp = datetime.datetime(2023, 1, 1, 0, 0, 0).timestamp()

    # userDf中 install_timestamp 是unix时间戳，单位秒。过滤，只要 2023-02-01 00:00:00 之后的数据
    print('2023-02-01 00:00:00 timestamp:', timestamp)
    userDf = userDf[userDf['install_timestamp'] >= timestamp]

    skanDf = skanDf[skanDf['min_valid_install_timestamp'] >= timestamp]

    userDf = meanAttribution(userDf, skanDf)
    userDf.to_csv(getFilename('attribution1ReStep48hoursGeoFast'), index=False)
    userDf = meanAttributionResult(userDf)
    userDf.to_csv(getFilename('attribution1Ret48Fast'), index=False)
    userDf = pd.read_csv(getFilename('attribution1Ret48Fast'))
    checkRet(userDf)

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DateFormatter

def draw():
    df = pd.read_csv(getFilename('attribution1RetCheck'))
    # 将不同的媒体分开画图，图片宽一点
    # install_date作为x轴，每隔7天画一个点
    # 双y轴，y1是r7usd和r7usdp；y2是MAPE（用虚线）。
    # 图片保存到'/src/data/zk/att1_{media}.jpg'
    # Convert 'install_date' to datetime
    df['install_date'] = pd.to_datetime(df['install_date'])

    for media in mediaList:
        media_df = df[df['media'] == media]

        # Create the plot with the specified figure size
        fig, ax1 = plt.subplots(figsize=(24, 6))

        plt.title(media)

        # Plot r7usd and r7usdp on the left y-axis
        ax1.plot(media_df['install_date'], media_df['r7usd'], label='r7usd')
        ax1.plot(media_df['install_date'], media_df['r7usdp'], label='r7usdp')
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
        plt.savefig(f'/src/data/zk2/attGF_{media}.jpg', bbox_inches='tight')
        plt.close()

if __name__ == '__main__':
    # getDataFromMC()
    # getCountryFromCampaign()
    # getCountryFromCampaign2()

    # dataStep1()
    # dataStep2()

    # skanDf = makeSKAN()
    # skanDf = skanAddValidInstallDate(skanDf)

    # print('skan data len:',len(skanDf))
    # skanDf.to_csv(getFilename('skanAOS6'),index=False)
    # skanDf = pd.read_csv(getFilename('skanAOS6'), converters={'campaign_id': str})
    # skanDf = skanValidInstallDate2Min(skanDf,N = 600)
    # skanDf = skanGroupby(skanDf)
    # skanDf.to_csv(getFilename('skanAOS6G'),index=False)
    # print('skan data group len:',len(skanDf))

    # skanDf = skanAddGeo()
    # skanDf.to_csv(getFilename('skanAOS6G48'), index=False)

    # userDf = makeUserDf()
    # print('user data len:',len(userDf))
    # # userDf.to_csv(getFilename('userAOS6'),index=False)
    # # userDf = pd.read_csv(getFilename('userAOS6'))
    # userDf = userInstallDate2Min(userDf,N = 600)
    # userDf = userGroupby(userDf)
    # userDf.to_csv(getFilename('userAOS6G48'),index=False)
    # print('user data group len:',len(userDf))

    # userDf = pd.read_csv(getFilename('userAOS6G48'))
    # skanDf = pd.read_csv(getFilename('skanAOS6G48'))
    
    # userDf = meanAttribution(userDf, skanDf)
    # userDf.to_csv(getFilename('attribution1ReStep48hoursGeo'), index=False)
    userDf = pd.read_csv(getFilename('attribution1ReStep24hoursGeo'))
    userDf = meanAttributionResult(userDf)
    # userDf.to_csv(getFilename('attribution1Ret48Geo'), index=False)
    # userDf = pd.read_csv(getFilename('attribution1Ret'))
    checkRet(userDf)

    # d1()
    # d2()
    draw()
    
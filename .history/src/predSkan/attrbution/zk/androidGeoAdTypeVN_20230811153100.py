# 融合归因细分到Ad type
# 专门针对VN的版本
# VN的campaign应该不多，全都算上吧

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
                campaign_id,
                campaign
            FROM
                ods_platform_appsflyer_events
            WHERE
                app_id = 'com.topwar.gp.vn'
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
                event_name = 'af_purchase'
                AND zone = 0
                AND day BETWEEN '20230101'
                AND '20230730'
                AND to_date(event_time, "yyyy-mm-dd hh:mi:ss") BETWEEN to_date('2023-01-01', "yyyy-mm-dd")
                AND to_date('2023-07-01', "yyyy-mm-dd")
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
            installs.campaign_id,
            installs.campaign
        FROM
            installs
            LEFT JOIN purchases ON installs.uid = purchases.uid
        GROUP BY
            installs.uid,
            installs.install_date,
            installs.install_timestamp,
            installs.media_source,
            installs.country_code,
            installs.campaign_id,
            installs.campaign;
        ;
    '''
    df = execSql(sql)
    df.to_csv(getFilename('androidFp07Vn'), index=False)
    return df

adTypeMap = {}

def loadData():
    # 加载数据
    df = pd.read_csv(getFilename('androidFp07Vn'), converters={'campaign_id':str})
    # 列 media_source 改名 media
    df = df.rename(columns={'media_source':'media'})
    # 列 media 中 'restricted' 改为 'Facebook Ads'
    df['media'] = df['media'].replace('restricted','Facebook Ads')

    # 广告分类
    # 添加一列 ‘ad type’，默认是‘unknown’
    df['ad type'] = 'unknown'
    # media = ‘googleadwords_int’ 的广告，ad type = ‘GG UAC’
    df.loc[df['media']=='googleadwords_int', 'ad type'] = 'GG UAC'
    # media = ‘Facebook Ads’ 的广告，campaign 包含 ‘AAA’ 的，ad type = ‘FB AAA’
    df.loc[
        (df['media']=='Facebook Ads')
        & (df['campaign'].str.contains('AAA'))
    , 'ad type'] = 'FB AAA'
    # media = ‘Facebook Ads’ 的广告，campaign 包含 ‘BAU’ 的，ad type = ‘FB BAU’
    df.loc[
        (df['media']=='Facebook Ads')
        & (df['campaign'].str.contains('BAU'))
    , 'ad type'] = 'FB BAU'
    df.loc[
        (df['media']=='Facebook Ads')
        & (df['campaign'].str.contains('restricted'))
    , 'ad type'] = 'FB restricted'
    # media = ‘bytedanceglobal_int’ 的广告，campaign 包含 ‘VO’，ad type = ‘TT VO’
    df.loc[
        (df['media']=='bytedanceglobal_int')
        & (df['campaign'].str.contains('VO'))
    , 'ad type'] = 'TT VO'
    # media = ‘bytedanceglobal_int’ 的广告，campaign 包含 ‘AEO’，ad type = ‘TT AEO’
    df.loc[
        (df['media']=='bytedanceglobal_int')
        & (df['campaign'].str.contains('AEO'))
    , 'ad type'] = 'TT AEO'

    global adTypeMap
    adTypeList = df['ad type'].unique()
    for adType in adTypeList:
        adTypeDf = df[df['ad type']==adType]
        campaignList = adTypeDf['campaign'].unique()
        campaignIdList = adTypeDf['campaign_id'].unique()
        # print(adType)
        # print(campaignList)
        # print(campaignIdList)
        # print('\n')

        adTypeMap[adType] = campaignIdList

    # 用ad type替换campaign_id，后面直接用ad type来归因
    df['campaign_id'] = df['ad type']
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
            app_id = 'com.topwar.gp.vn'
            AND day BETWEEN '20230101' AND '20230701'
            AND app = '102'
            AND cost >= 1
        ;
    '''
    df = execSql(sql)
    df.to_csv(getFilename('campaignGeoVn'), index=False)
    return df

# 改一下格式
def getCountryFromCampaign2():
    df = pd.read_csv(getFilename('campaignGeoVn'))
    df['country_code'].fillna('unknown', inplace=True)

    # 对结果进行分组，并将country_code连接成逗号分隔的字符串
    groupedDf = df.groupby(['day', 'media_source', 'campaign_id', 'cost']).agg({
        'country_code': lambda x: '|'.join(sorted(set(x)))
    }).reset_index()

    # 重命名country_code列为country_code_list
    groupedDf.rename(columns={'country_code': 'country_code_list'}, inplace=True)

    groupedDf.to_csv(getFilename('campaignGeo2Vn'), index=False)
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
    df = loadData()
    # r1usd
    levels = makeLevels1(df,usd='r1usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r1usd',cv='cv')
    
    # df.to_csv(getFilename('androidFpMergeDataStep1Campaign24'), index=False)
    print('dataStep1 24h done')    
    return df

def dataStep1_48():
    df = loadData()
    # r1usd
    levels = makeLevels1(df,usd='r2usd',N=32)
    cvMapDf = makeCvMap(levels)
    df = addCv(df,cvMapDf,usd='r2usd',cv='cv')
    
    # df.to_csv(getFilename('androidFpMergeDataStep1Campaign48'), index=False)
    print('dataStep1 48h done')    
    return df

def dataStep2(df):
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
    # df.to_csv(getFilename('androidFpMergeDataStep2Campaign'), index=False)
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

    cvDf = df.copy(deep=True).reset_index(drop=True)

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
    campaignGeo2Df = pd.read_csv(getFilename('campaignGeo2Vn'), converters={'campaign_id':str})
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
    userDf = df.copy(deep=True)
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
def checkRet(retDf):
    # 读取原始数据
    rawDf = loadData()
    
    campaignList = getChooseCampaignList()
    rawDf = rawDf[rawDf['campaign_id'].isin(campaignList)]
    # 将install_timestamp转为install_date
    rawDf['install_date'] = pd.to_datetime(rawDf['install_timestamp'], unit='s').dt.date
    rawDf['user_count'] = 1
    # 按照media和install_date分组，计算r7usd的和
    rawDf = rawDf.groupby(['campaign_id', 'install_date']).agg({'r1usd': 'sum','r7usd': 'sum','user_count':'sum'}).reset_index()

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
    rawDf.to_csv(getFilename('attribution24RetCheckVn'), index=False)
    # 计算整体的MAPE和R2
    groupDf = rawDf.groupby(['install_date']).agg({'r7usd': 'sum','r7usdp':'sum'}).reset_index()
    groupDf['MAPE'] = abs(groupDf['r7usd'] - groupDf['r7usdp']) / groupDf['r7usd']
    MAPE = groupDf['MAPE'].mean()
    # r2 = r2_score(rawDf['r7usd'], rawDf['r7usdp'])
    print('MAPE:', MAPE)
    # print('R2:', r2)

    for campaignId in campaignList:
        mediaDf = rawDf[rawDf['campaign_id'] == campaignId].copy()
        MAPE = mediaDf['MAPE'].mean()
        print(f"campaignId: {campaignId}, MAPE: {MAPE}")
        # 计算r7usd和r7usdp的7日均线的MAPE
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']
        MAPE7 = mediaDf['MAPE7'].mean()
        print(f"campaignId: {campaignId}, MAPE7: {MAPE7}")

    df = pd.read_csv(getFilename('attribution24RetCheckVn'))
    # r7PR1 = df['r7usd'] / df['r1usd']
    # print(r7PR1.mean())
    # r7pPR1 = df['r7usdp'] / df['r1usd']
    # # 排除r7pPR1 = inf的情况
    # r7pPR1 = r7pPR1[r7pPR1 != np.inf]
    # print(r7pPR1.mean())

    return df


import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DateFormatter

def draw24(df,prefix='attCampaign24_'):
    # df = pd.read_csv(getFilename('attribution24RetCheckVn'), converters={'campaign_id':str})
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
        mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']

        plt.title(campaignId + 'rolling 7 days')

        ax3.plot(mediaDf['install_date'], mediaDf['r7usd7'], label='r7usd7')
        ax3.plot(mediaDf['install_date'], mediaDf['r7usdp7'], label='r7usdp7')
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


def chooseCampaign():
    # 选取campaign id，每个媒体选取2个花费最高的campaign。
    df = pd.read_csv(getFilename('campaignGeoVn'))
    df = df.groupby(['media_source','campaign_id']).agg({'cost':'sum'}).reset_index()
    df = df.sort_values(by=['media_source','cost'],ascending=False)
    # df = df.groupby(['media_source']).head(2)
    df = df.groupby(['media_source','campaign_id']).agg({'cost':'sum'}).reset_index()
    df.to_csv(getFilename('chooseCampaignVn'), index=False)
    print('chooseCampaign done')

# def getChooseCampaignList():
#     campaignDf = pd.read_csv(getFilename('chooseCampaignVn'))
#     campaignDf = campaignDf.loc[campaignDf['media_source'].isin(mediaList)]
#     # campaignDf['campaign_id'] 改为str类型
#     campaignDf['campaign_id'] = campaignDf['campaign_id'].astype(str)
#     campaignList = campaignDf['campaign_id'].tolist()
#     print('getChooseCampaignList:',campaignList)
#     return campaignList

def getChooseCampaignList():
    return list(adTypeMap.keys())

def main24(fast = False,onlyCheck = False):
    print('main24')
    if onlyCheck == False:
        if fast == False:
            # 24小时版本归因
            # getDataFromMC()
            # getCountryFromCampaign()
            # getCountryFromCampaign2()

            df = dataStep1_24()
            df2 = dataStep2(df)
            
            # 做一下简单的campaign过滤，只获得选中的campaign
            campaignList = getChooseCampaignList()
            df3 = df2.loc[df2['campaign_id'].isin(campaignList)].copy()
            skanDf = makeSKAN(df3)
            skanDf = skanAddValidInstallDate(skanDf)

            print('skan data len:',len(skanDf))
            
            skanDf = skanValidInstallDate2Min(skanDf,N = 600)
            skanDf = skanGroupby(skanDf)
            skanDf.to_csv(getFilename('skanAOSAdTypeG24Vn'),index=False)
            print('skan data group len:',len(skanDf))

            # 由于只有越南，就不搞国家匹配了
            # skanDf = skanAddGeo(skanDf)
            skanDf['country_code_list'] = ''
            skanDf.to_csv(getFilename('skanAOSAdTypeG24GeoVn'), index=False)

            userDf = makeUserDf(df2.copy())
            print('user data len:',len(userDf))
            
            userDf = userInstallDate2Min(userDf,N = 600)
            userDf = userGroupby(userDf)
            userDf.to_csv(getFilename('userAOSAdTypeG24Vn'),index=False)
            print('user data group len:',len(userDf))
        else:
            userDf = pd.read_csv(getFilename('userAOSAdTypeG24Vn'))
            skanDf = pd.read_csv(getFilename('skanAOSAdTypeG24GeoVn'))
        
        userDf = meanAttribution(userDf, skanDf)
        userDf.to_csv(getFilename('meanAttribution24Vn'), index=False)
        userDf = pd.read_csv(getFilename('meanAttribution24Vn'))
        userDf = meanAttributionResult(userDf)
        userDf.to_csv(getFilename('meanAttributionResult24Vn'), index=False)
    
    userDf = pd.read_csv(getFilename('meanAttributionResult24Vn'), converters={'campaign_id':str})
    df = checkRet(userDf)
    draw24(df,prefix='attAdTypeVn24_')

def main48(fast = False,onlyCheck = False):
    print('main48')
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
            
            skanDf = skanValidInstallDate2Min(skanDf,N = 600)
            skanDf = skanGroupby(skanDf)
            skanDf.to_csv(getFilename('skanAOSAdTypeG48Vn'),index=False)
            print('skan data group len:',len(skanDf))

            # 由于只有越南，就不搞国家匹配了
            # skanDf = skanAddGeo(skanDf)
            skanDf['country_code_list'] = ''
            skanDf.to_csv(getFilename('skanAOSAdTypeG48GeoVn'), index=False)

            userDf = makeUserDf(df2.copy())
            print('user data len:',len(userDf))
            
            userDf = userInstallDate2Min(userDf,N = 600)
            userDf = userGroupby(userDf)
            userDf.to_csv(getFilename('userAOSAdTypeG48Vn'),index=False)
            print('user data group len:',len(userDf))

        else:
            userDf = pd.read_csv(getFilename('userAOSAdTypeG48Vn'))
            skanDf = pd.read_csv(getFilename('skanAOSAdTypeG48GeoVn'))
        
        userDf = meanAttribution(userDf, skanDf)
        userDf.to_csv(getFilename('meanAttribution48Vn'), index=False)
        userDf = pd.read_csv(getFilename('meanAttribution48Vn'))
        userDf = meanAttributionResult(userDf)
        userDf.to_csv(getFilename('meanAttributionResult48Vn'), index=False)
    
    userDf = pd.read_csv(getFilename('meanAttributionResult48Vn'), converters={'campaign_id':str})
    df = checkRet(userDf)
    draw24(df,prefix='attCampaign48Vn_')

# 尝试制定一个针对r1usd的门槛，将r1usd较少的日期排除，重新计算MAPE
# 尝试用这种方式将MAPE的波动降低
def adv1():
    df = pd.read_csv(getFilename('attribution24RetCheckVn'), converters={'campaign_id':str})
    # 首先计算一下MAPE较高的日期的r1usd的均值
    dfHighMape = df.loc[df['MAPE']> 2]
    r1usdMean = dfHighMape['r1usd'].mean()
    r7usdMean = dfHighMape['r7usd'].mean()
    print('r1usdMean:',r1usdMean)
    print('r7usdMean:',r7usdMean)
    # 将r1usdMean作为门槛，将r1usd小于门槛的日期排除
    df2 = df.loc[df['r1usd'] > r1usdMean]
    # print('df2 len:',len(df2))

    groupDf = df2.groupby(['install_date']).agg({'r7usd': 'sum','r7usdp':'sum'}).reset_index()
    groupDf['MAPE'] = abs(groupDf['r7usd'] - groupDf['r7usdp']) / groupDf['r7usd']
    MAPE = groupDf['MAPE'].mean()
    print('整体MAPE:', MAPE)
    
    campaignList = getChooseCampaignList()
    for campaignId in campaignList:
        mediaDf = df2[df2['campaign_id'] == campaignId].copy()
        print('len:',len(mediaDf))
        MAPE = mediaDf['MAPE'].mean()
        print(f"campaignId: {campaignId}, MAPE: {MAPE}")
        # 计算r7usd和r7usdp的7日均线的MAPE
        # mediaDf['r7usd7'] = mediaDf['r7usd'].rolling(7).mean()
        # mediaDf['r7usdp7'] = mediaDf['r7usdp'].rolling(7).mean()
        # mediaDf['MAPE7'] = abs(mediaDf['r7usd7'] - mediaDf['r7usdp7']) / mediaDf['r7usd7']
        # MAPE7 = mediaDf['MAPE7'].mean()
        # print(f"campaignId: {campaignId}, MAPE7: {MAPE7}")

# 尝试加入更多的用户属性，比如用户等级，从数数获得
import requests
from urllib import parse
from src.config import ssToken
from requests.adapters import HTTPAdapter
def ssSql(sql):
    # url = 'http://bishushukeji.rivergame.net/querySql'
    url = 'http://123.56.188.109/querySql'
    url += '?token='+ssToken+'&timeoutSeconds=1800'+'&timeoutSecond=1800'
    headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
    # 通过字典方式定义请求body
    formData = {"sql": sql, "format": 'json','timeoutSeconds':1800,'timeoutSecond':1800}
    data = parse.urlencode(formData)
    # 请求方式
    s = requests.Session()
    s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
    s.mount('https://',HTTPAdapter(max_retries=3))
    # 由于事件可能会比较长，暂时不设置timeout
    r = s.post(url=url, headers=headers, data=data)
    # print(r.text)
    lines = r.text.split('\n')
    # print(lines[0])
    # 多一行头，多一行尾巴
    lines = lines[1:-1]
    return lines

import time
from src.tools import printProgressBar
# 异步分页
def ssSql2(sql):
    url = 'http://123.56.188.109/open/submit-sql'
    url += '?token='+ssToken

    headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
    # 通过字典方式定义请求body
    formData = {"sql": sql, "format": 'json','pageSize':'100000'}
    data = parse.urlencode(formData)
    # 请求方式
    s = requests.Session()
    s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
    s.mount('https://',HTTPAdapter(max_retries=3))
    
    r = s.post(url=url, headers=headers, data=data)
    try:
        j = json.loads(r.text)
        taskId = j['data']['taskId']
        print('taskId:',taskId)
    except Exception:
        print(r.text)
        return
    # taskId = '164f8187879e3000'
    printProgressBar(0, 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
    for _ in range(60):
        time.sleep(10)
        url2 = 'http://123.56.188.109/open/sql-task-info'
        url2 += '?token='+ssToken+'&taskId='+taskId
        s = requests.Session()
        s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
        s.mount('https://',HTTPAdapter(max_retries=3))
        r = s.get(url=url2)
        try:
            j = json.loads(r.text)
            status = j['data']['status']
            if status == 'FINISHED':
                rowCount = j['data']['resultStat']['rowCount']
                pageCount = j['data']['resultStat']['pageCount']
                print('rowCount:',rowCount,'pageCount:',pageCount)
                # print(j)
                lines = []
                for p in range(pageCount):
                    url3 = 'http://123.56.188.109/open/sql-result-page'
                    url3 += '?token='+ssToken+'&taskId='+taskId+'&pageId=%d'%p
                    s = requests.Session()
                    s.mount('http://',HTTPAdapter(max_retries=3))#设置重试次数为3次
                    s.mount('https://',HTTPAdapter(max_retries=3))
                    r = s.get(url=url3)
                    lines += r.text.split('\n')
                    # print('page:%d/%d'%(p,pageCount),'lines:',len(lines))
                    printProgressBar(p, pageCount-1, prefix = 'Progress:', suffix = 'page', length = 50)
                return lines
            else:
                # print('progress:',j['data']['progress'])
                printProgressBar(j['data']['progress'], 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
        except Exception as e:
            print('e:',e)
            print(r.text)
            continue
        # 查询太慢了，多等一会再尝试
        time.sleep(10)

import json
def getUserLevelFromSS():
    sql = '''
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (SELECT *, TIMESTAMP '1981-01-01' "$__Date_Time" from (select *, if("#vp@zone" is not null and "#vp@zone">=-12 and "#vp@zone"<=14, date_add('second', cast((0-"#vp@zone")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF(("platform" IS NULL), 8, 8)) as double) "#vp@zone",try_cast(try(date_diff('second', "internal_u@ctime", "#event_time")) as double) "#vp@lifetime_sec" from (select a.*, b."ctime" "internal_u@ctime" from (select "#event_name","#user_id","platform","#event_time","$part_date","$part_event" from v_event_2) a join (select * from (select "#update_time","#event_date","#user_id","ctime" from v_user_2) where "#event_date" > 20221225) b on a."#user_id"=b."#user_id")))) ta_ev inner join (select *, "#account_id" group_0,"@vpc_cluster_tag_20230809_2" group_1 from (select *, try_cast(try(date_add('hour', -8, cast("ctime" as timestamp(3)))) as timestamp(3)) "#vp@ctime_utc0" from (select a.*,"@vpc_cluster_tag_20230809_2" from (select * from (select "#account_id","#update_time","#event_date","#user_id","ctime","firstplatform" from v_user_2) where "#event_date" > 20221225) a left join (select "#user_id" "#user_id",tag_value "@vpc_cluster_tag_20230809_2" from user_result_cluster_2 where cluster_name = 'tag_20230809_2') b0 on a."#user_id"=b0."#user_id"))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'user_levelup' ) ) )) and (ta_ev."#vp@lifetime_sec" <= 8.64E+4)) and ((("$part_date" between '2022-12-31' and '2023-08-01') and ("@vpc_tz_#event_time" >= timestamp '2023-01-01' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2023-07-31'))) and ((ta_u."firstplatform" IN ('GooglePlay_VN')) and ((ta_u."#vp@ctime_utc0" >= cast('2023-01-01 00:00:00' as timestamp) AND ta_u."#vp@ctime_utc0" <= cast('2023-07-31 23:59:59' as timestamp))))) group by group_0,group_1,"$__Date_Time")) group by group_0,group_1)) ORDER BY total_amount DESC
    '''
    lines = ssSql2(sql)
    # print(len(lines))
    # print(lines[0:10])
    # ['["7749205068765","1683633089549-234762429262863568",{"1981-01-01 00:00:00":40.0},40.0,331282]\r', 
    # '["7858799250526","1688432106162-1501556147454260981",{"1981-01-01 00:00:00":40.0},40.0,331282]\r', 
    # '["917167757952","1675175109187-5880019622224933533",{"1981-01-01 00:00:00":40.0},40.0,331282]\r', 
    # '["7696922872748","1684603348158-2018870337015307338",{"1981-01-01 00:00:00":40.0},40.0,331282]\r', 
    # '["7716835331004","1685180697759-298381819776190520",{"1981-01-01 00:00:00":40.0},40.0,331282]\r', 
    # '["7636382616442","1682639103294-6910479557719482722",{"1981-01-01 00:00:00":39.0},39.0,331282]\r', 
    # '["7580339637073","1681385261489-8676261945416140460",{"1981-01-01 00:00:00":39.0},39.0,331282]\r', 
    # '["920710253189","1675911846785-8517902223497952203",{"1981-01-01 00:00:00":39.0},39.0,331282]\r', 
    # '["7852437441609","1689867852798-7399115984182334721",{"1981-01-01 00:00:00":38.0},38.0,331282]\r', 
    # '["7718038768572","1685204436490-9031172767240565842",{"1981-01-01 00:00:00":37.0},37.0,331282]\r']

    # 初始化一个空的DataFrame
    df = pd.DataFrame(columns=['uid','level'])

    for line in lines:
        try:
            lineJ = json.loads(line)
        except:
            continue

        # uid = lineJ[0]
        afId = lineJ[1]
        level = lineJ[3]

        # 创建一个新的DataFrame并添加到原始DataFrame中
        df_new = pd.DataFrame([[afId,level]], columns=df.columns)
        df = df.append(df_new, ignore_index=True)

    df.to_csv(getFilename('userLevelVn'), index=False)

def main24Adv(fast = False,onlyCheck = False):
    print('main24 adv')
    if onlyCheck == False:
        if fast == False:
            # 24小时版本归因
            # getDataFromMC()
            # getUserLevelFromSS()
            # userLevelDf = pd.read_csv(getFilename('userLevelVn'))
            # userDf = pd.read_csv(getFilename('androidFp07Vn'))

            # df = userDf.merge(userLevelDf, on=['uid'], how='left')
            # df.to_csv(getFilename('vnAdv2'), index=False)

            # getCountryFromCampaign()
            # getCountryFromCampaign2()


            df = dataStep1_24()
            df2 = dataStep2(df)

            # # 主要区别在这，将用户属性（level）加入到df2中，并合并到cv列中
            userLevelDf = pd.read_csv(getFilename('userLevelVn'))
            df2 = df2.merge(userLevelDf, on=['uid'], how='left')
            # # level补充，空值用0填充
            # df2['level'] = df2['level'].fillna(0)
            # df2['level cv'] = (df2['level']+9)//10
            # # 最大值为9
            # df2.loc[df2['level cv'] >= 10,'level cv'] = 9
            # # 将level cv加入到cv中，cv*10，再加上level cv，得到新的cv
            # df2['cv'] = df2['cv']*10 + df2['level cv']

            # 做一下简单的campaign过滤，只获得选中的campaign
            campaignList = getChooseCampaignList()
            df3 = df2.loc[df2['campaign_id'].isin(campaignList)].copy()
            skanDf = makeSKAN(df3)
            skanDf = skanAddValidInstallDate(skanDf)

            print('skan data len:',len(skanDf))
            
            skanDf = skanValidInstallDate2Min(skanDf,N = 600)
            skanDf = skanGroupby(skanDf)
            skanDf.to_csv(getFilename('skanAOSAdTypeAdvG24Vn'),index=False)
            print('skan data group len:',len(skanDf))

            # 由于只有越南，就不搞国家匹配了
            # skanDf = skanAddGeo(skanDf)
            skanDf['country_code_list'] = ''
            skanDf.to_csv(getFilename('skanAOSAdTypeAdvG24GeoVn'), index=False)

            userDf = makeUserDf(df2.copy())
            print('user data len:',len(userDf))
            
            userDf = userInstallDate2Min(userDf,N = 600)
            userDf = userGroupby(userDf)
            userDf.to_csv(getFilename('userAOSAdTypeAdvG24Vn'),index=False)
            print('user data group len:',len(userDf))
        else:
            userDf = pd.read_csv(getFilename('userAOSAdTypeAdvG24Vn'))
            skanDf = pd.read_csv(getFilename('skanAOSAdTypeAdvG24GeoVn'))
        
        userDf = meanAttribution(userDf, skanDf)
        userDf.to_csv(getFilename('meanAttribution24Vn'), index=False)
        userDf = pd.read_csv(getFilename('meanAttribution24Vn'))
        userDf = meanAttributionResult(userDf)
        userDf.to_csv(getFilename('meanAttributionResult24Vn'), index=False)
    
    userDf = pd.read_csv(getFilename('meanAttributionResult24Vn'), converters={'campaign_id':str})
    df = checkRet(userDf)
    draw24(df,prefix='attAdvTypeVn24_')


if __name__ == '__main__':
    # getDataFromMC()
    # loadData()
    # print(getChooseCampaignList())
    # main24(fast = False,onlyCheck = False)
    # main48(fast = False,onlyCheck = False)

    # getCountryFromCampaign()
    # getCountryFromCampaign2()
    # chooseCampaign()
    # adv1()
    main24Adv(fast = False,onlyCheck = False)
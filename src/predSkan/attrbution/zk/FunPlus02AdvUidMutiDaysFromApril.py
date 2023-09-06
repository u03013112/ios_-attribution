# 与FunPlus02AdvUidMutiDays.py的从4月1日至今版本
# 这个版本没有days参数，只有dayStr参数，是业务日期，格式为'20210404'
# 这个版本的运行效率会比较低，所以预计一周跑一次

import io

import numpy as np
import pandas as pd
from tqdm import tqdm

from datetime import datetime, timedelta


def init():
    global execSql
    global dayStr
    # 为了尽量少改代码，这里沿用days参数，只不过不再获取days的值
    # 而是计算出days的值
    global days

    if 'o' in globals():
        print('this is online version')

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']
        days = args['days']
    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local

        dayStr = '20230504'
        days = '10'

    # 计算dayStr 距离 20230401 的天数
    day = datetime.strptime(dayStr, '%Y%m%d')
    day20230401 = datetime.strptime('20230401', '%Y%m%d')
    days = (day - day20230401).days
    if days < 0:
        raise Exception('dayStr必须大于等于20230401')

    # 如果days不是整数，转成整数
    days = int(days)
    print('dayStr:', dayStr)
    print('days:', days)

# 只针对下面媒体进行归因，其他媒体不管
mediaList = [
    'Facebook Ads',
    'googleadwords_int',
    'bytedanceglobal_int',
    'other'
]

def getSKANDataFromMC(dayStr, days):
    dayBeforeStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')

    sql = f'''
        SELECT
            ad_network_campaign_id as campaign_id,
            media_source as media,
            skad_conversion_value as cv,
            timestamp as postback_timestamp
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day between '{dayBeforeStr}' and '{dayStr}'
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
    # 为时间字符串添加UTC时区标识
    skanDf['postback_timestamp'] = skanDf['postback_timestamp'] + '+00:00'
    # 将带有UTC时区标识的时间字符串转换为datetime
    skanDf['postback_timestamp'] = pd.to_datetime(skanDf['postback_timestamp'], utc=True)
    # 将datetime转换为Unix时间戳
    skanDf['postback_timestamp'] = skanDf['postback_timestamp'].view(np.int64) // 10 ** 9

    # 将cv转换为整数类型
    skanDf['cv'] = skanDf['cv'].astype(int)

    # 计算min_valid_install_timestamp和max_valid_install_timestamp
    # cv 小于 0 的 是 null 值，当做付费用户匹配，范围稍微大一点
    skanDf.loc[skanDf['cv'] < 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - 72*3600
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - 48*3600
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - 72*3600

    # min_valid_install_timestamp 类型改为 int
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].astype(int)

    skanDf.loc[:, 'max_valid_install_timestamp'] = skanDf['postback_timestamp'] - 24*3600

    return skanDf


def getCountryFromCampaign(minValidInstallTimestamp, maxValidInstallTimestamp):
    # minValidInstallTimestamp 向前推8天，为了让出广告的转化窗口
    minValidInstallTimestamp -= 24 * 8 * 3600
    maxValidInstallTimestamp += 24 * 3600
    # 另外minValidInstallTimestamp和maxValidInstallTimestamp转化成格式为'20230301'
    minValidInstallTimestampDayStr = datetime.fromtimestamp(minValidInstallTimestamp).strftime('%Y%m%d')
    maxValidInstallTimestampDayStr = datetime.fromtimestamp(maxValidInstallTimestamp).strftime('%Y%m%d')

    # 获得campaign对应的国家信息
    sql = f'''
        select
            day,
            media_source,
            campaign_id,
            country_code,
            cost
        from
            ods_platform_appsflyer_masters
        where
            app_id = 'id1479198816'
            AND day BETWEEN '{minValidInstallTimestampDayStr}' AND '{maxValidInstallTimestampDayStr}'
            AND app = '102'
            AND cost >= 1
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

# 改一下格式
def getCountryFromCampaign2(df):
    df['country_code'].fillna('unknown', inplace=True)

    # 对结果进行分组，并将country_code连接成逗号分隔的字符串
    groupedDf = df.groupby(['day', 'media_source', 'campaign_id']).agg({
        'country_code': lambda x: '|'.join(sorted(set(x)))
    }).reset_index()

    # 重命名country_code列为country_code_list
    groupedDf.rename(columns={'country_code': 'country_code_list'}, inplace=True)

    return groupedDf

def skanAddGeo(skanDf,campaignGeo2Df):
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
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].view(np.int64) // 10 ** 9
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'].view(np.int64) // 10 ** 9

    # min_valid_install_timestamp 恢复，将上面减去的8天加回来
    skanDf['min_valid_install_timestamp'] += 8*24*3600

    # 计算未匹配的行数在总行数中的占比
    unmatched_rows_ratio = unmatched_rows / len(skanDf)

    # 在函数结束时打印未匹配的行数以及未匹配的行数在总行数中的占比
    print(f"Unmatched rows: {unmatched_rows}")
    print(f"Unmatched rows ratio: {unmatched_rows_ratio:.2%}")

    return skanDf
    
def getAfDataFromMC(minValidInstallTimestamp, maxValidInstallTimestamp):
    # 修改后的SQL语句，r1usd用来计算cv，r2usd可能可以用来计算48小时cv，暂时不用r7usd，因为这个时间7日应该还没有完整。
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
            COALESCE(
                SUM(
                    CASE
                        WHEN event_timestamp <= install_timestamp + 48 * 3600 THEN revenue_value_usd
                        ELSE 0
                    END
                ),
                0
            ) as r2usd,
            COALESCE(
                SUM(
                    CASE
                        WHEN event_timestamp <= install_timestamp + 168 * 3600 THEN revenue_value_usd
                        ELSE 0
                    END
                ),
                0
            ) as r7usd,
            TO_CHAR(
                TO_DATE(install_timestamp, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            country as country_code
        FROM
            rg_bi.ads_topwar_ios_purchase_adv
        WHERE
            install_timestamp BETWEEN '{minValidInstallTimestamp}'
            AND '{maxValidInstallTimestamp}'
            AND game_uid IS NOT NULL
        GROUP BY
            game_uid,
            install_timestamp,
            country;
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
id1479198816,3,af_skad_revenue,0,1,3.24,5.35,0,24,2023-03-30 10:23:45,0,,,
id1479198816,4,af_skad_revenue,0,1,5.35,7.8,0,24,2023-03-30 10:23:45,0,,,
id1479198816,5,af_skad_revenue,0,1,7.8,10.71,0,24,2023-03-30 10:23:45,0,,,
id1479198816,6,af_skad_revenue,0,1,10.71,14.47,0,24,2023-03-30 10:23:45,0,,,
id1479198816,7,af_skad_revenue,0,1,14.47,18.99,0,24,2023-03-30 10:23:45,0,,,
id1479198816,8,af_skad_revenue,0,1,18.99,24.29,0,24,2023-03-30 10:23:45,0,,,
id1479198816,9,af_skad_revenue,0,1,24.29,31.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,10,af_skad_revenue,0,1,31.08,40.26,0,24,2023-03-30 10:23:45,0,,,
id1479198816,11,af_skad_revenue,0,1,40.26,51.52,0,24,2023-03-30 10:23:45,0,,,
id1479198816,12,af_skad_revenue,0,1,51.52,61.25,0,24,2023-03-30 10:23:45,0,,,
id1479198816,13,af_skad_revenue,0,1,61.25,70.16,0,24,2023-03-30 10:23:45,0,,,
id1479198816,14,af_skad_revenue,0,1,70.16,82.56,0,24,2023-03-30 10:23:45,0,,,
id1479198816,15,af_skad_revenue,0,1,82.56,97.38,0,24,2023-03-30 10:23:45,0,,,
id1479198816,16,af_skad_revenue,0,1,97.38,111.57,0,24,2023-03-30 10:23:45,0,,,
id1479198816,17,af_skad_revenue,0,1,111.57,125.27,0,24,2023-03-30 10:23:45,0,,,
id1479198816,18,af_skad_revenue,0,1,125.27,142.67,0,24,2023-03-30 10:23:45,0,,,
id1479198816,19,af_skad_revenue,0,1,142.67,161.66,0,24,2023-03-30 10:23:45,0,,,
id1479198816,20,af_skad_revenue,0,1,161.66,184.42,0,24,2023-03-30 10:23:45,0,,,
id1479198816,21,af_skad_revenue,0,1,184.42,204.85,0,24,2023-03-30 10:23:45,0,,,
id1479198816,22,af_skad_revenue,0,1,204.85,239.74,0,24,2023-03-30 10:23:45,0,,,
id1479198816,23,af_skad_revenue,0,1,239.74,264.97,0,24,2023-03-30 10:23:45,0,,,
id1479198816,24,af_skad_revenue,0,1,264.97,306.91,0,24,2023-03-30 10:23:45,0,,,
id1479198816,25,af_skad_revenue,0,1,306.91,355.15,0,24,2023-03-30 10:23:45,0,,,
id1479198816,26,af_skad_revenue,0,1,355.15,405.65,0,24,2023-03-30 10:23:45,0,,,
id1479198816,27,af_skad_revenue,0,1,405.65,458.36,0,24,2023-03-30 10:23:45,0,,,
id1479198816,28,af_skad_revenue,0,1,458.36,512.69,0,24,2023-03-30 10:23:45,0,,,
id1479198816,29,af_skad_revenue,0,1,512.69,817.08,0,24,2023-03-30 10:23:45,0,,,
id1479198816,30,af_skad_revenue,0,1,817.08,1819.03,0,24,2023-03-30 10:23:45,0,,,
id1479198816,31,af_skad_revenue,0,1,1819.03,2544.74,0,24,2023-03-30 10:23:45,0,,,
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
    return df# 暂时就只关心这3个媒体

# 优化效率，但是还是慢
def meanAttribution(userDf, skanDf):
    # 将country_code_list列的空值填充为空字符串
    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')

    # userDf['install_timestamp'] 原本是string类型，转换为int类型
    userDf['install_timestamp'] = pd.to_numeric(userDf['install_timestamp'], errors='coerce')

    # 将时间戳进行近似，每隔S秒为一个区间
    # S = 600
    # 10分钟运行还是太慢了
    S = 60 * 60

    # 对userDf进行汇总
    userDf['install_timestamp'] = (userDf['install_timestamp'] // S) * S
    userDf['count'] = 1
    # userDf install_date列的空值填充为空字符串
    userDf['install_date'] = userDf['install_date'].fillna('')
    
    userDf = userDf.groupby(['cv', 'country_code', 'install_timestamp','install_date']).agg({'customer_user_id': lambda x: '|'.join(x),'count': 'sum'}).reset_index()
    
    userDf['attribute'] = userDf.apply(lambda x: [], axis=1)

    skanDf['min_valid_install_timestamp'] = (skanDf['min_valid_install_timestamp'] // S) * S
    skanDf['max_valid_install_timestamp'] = (skanDf['max_valid_install_timestamp'] // S) * S
    skanDf['count'] = 1
    skanDf = skanDf.groupby(['cv', 'country_code_list', 'min_valid_install_timestamp', 'max_valid_install_timestamp','campaign_id','media']).agg({'count': 'sum'}).reset_index(drop = False)
    

    # 待分配的skan条目的索引
    pending_skan_indices = skanDf.index.tolist()

    N = 3 # 最多进行3次分配
    for i in range(N):  
        print(f"开始第 {i + 1} 次分配")

        new_pending_skan_indices = []

        # 使用过滤条件选择要处理的skanDf行
        skanDf_to_process = skanDf.loc[pending_skan_indices]
        print(f"待处理的skanDf行数：{len(skanDf_to_process)}")
        for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
            media = item['media']
            cv = item['cv']
            min_valid_install_timestamp = item['min_valid_install_timestamp']
            max_valid_install_timestamp = item['max_valid_install_timestamp']
            
            if i == N-2:
                min_valid_install_timestamp -= 24*3600
            if i == N-1:
                # 由于经常有分不出去的情况，所以最后一次分配，不考虑国家
                item_country_code_list = ''
                min_valid_install_timestamp -= 48*3600
                # print('最后一次分配，不考虑国家，且时间范围向前推一天')
                # print(item)
            else:
                item_country_code_list = item['country_code_list']

            if cv < 0:
                # print('cv is null')
                if item_country_code_list == '':
                    condition = (
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) 
                        & (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 0.95))
                    )
                else:
                    country_code_list = item_country_code_list.split('|')
                    condition = (
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) &
                        (userDf['country_code'].isin(country_code_list)) 
                        & (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 0.95))
                    )
            else:
                # 先检查item_country_code_list是否为空
                if item_country_code_list == '':
                    condition = (
                        (userDf['cv'] == cv) &
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) 
                        & (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 0.95))
                    )
                else:
                    country_code_list = item_country_code_list.split('|')
                    condition = (
                        (userDf['cv'] == cv) &
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) &
                        (userDf['country_code'].isin(country_code_list)) 
                        & (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 0.95))
                    )

            matching_rows = userDf[condition]
            total_matching_count = matching_rows['count'].sum()

            if total_matching_count > 0:
                rate = item['count'] / total_matching_count
                userDf.loc[condition, 'attribute'] = userDf.loc[condition, 'attribute'].apply(lambda x: x + [{'media': media, 'skan index': index, 'rate': rate}])
            else:
                new_pending_skan_indices.append(index)

        new_pending_skan_indices = list(set(new_pending_skan_indices))

        print(f"第 {i + 1} 次分配结束，还有 {len(new_pending_skan_indices)} 个待分配条目")
        pendingDf = skanDf.loc[new_pending_skan_indices]
        
        print('待分配的skan数量：')
        print(pendingDf.groupby('media').size())

        # 找出需要重新分配的行
        rows_to_redistribute = userDf[userDf['attribute'].apply(lambda x: sum([item['rate'] for item in x]) > 1)]
        print(f"需要重新分配的行数：{len(rows_to_redistribute)}")

        # 记录需要移除的skan index
        removed_skan_indices = set()

        # 对每一行，找出需要重新分配的skan条目，并将它们添加到new_pending_skan_indices列表中
        for index, row in tqdm(rows_to_redistribute.iterrows(), total=len(rows_to_redistribute)):
            attribute_list = row['attribute']

            attribute_list_sorted = sorted(attribute_list, key=lambda x: x['rate'], reverse=True)
            accumulated_rate = 0

            for item in attribute_list_sorted:
                accumulated_rate += item['rate']
                if accumulated_rate > 1:
                    removed_skan_indices.add(item['skan index'])

        print(f"需要移除的skan index数量：{len(removed_skan_indices)}")
        # 在userDf中删除涉及到需要移除的skan index的归因
        userDf['attribute'] = userDf['attribute'].apply(lambda x: [item for item in x if item['skan index'] not in removed_skan_indices])

        pending_skan_indices = new_pending_skan_indices
        # 更新待分配的skan索引列表
        pending_skan_indices = list(set(pending_skan_indices).union(removed_skan_indices))

        print(f"第 {i + 1} 次分配结束 2，将过分配skan排除掉，还有 {len(pending_skan_indices)} 个待分配条目")
        pendingDf = skanDf.loc[pending_skan_indices]
        
        # print('待分配的skan数量2：')
        # print(pendingDf.groupby('media').size())
        
        # 计算每个媒体的未分配的用户数
        pending_counts = pendingDf.groupby('media')['count'].sum()
        print("每个媒体的未分配的用户数：")
        print(pending_counts)
        
        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('media')['count'].sum()
        print("每个媒体的总的skan用户数：")
        print(total_counts)
        
        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts
        print("每个媒体的未分配占比：")
        print(pending_ratios)

    # 拆分customer_user_id
    userDf['customer_user_id'] = userDf['customer_user_id'].apply(lambda x: x.split('|'))
    userDf = userDf.explode('customer_user_id')

    for media in mediaList:
        userDf[media + ' rate'] = userDf['attribute'].apply(lambda x: sum([item['rate'] for item in x if item['media'] == media]))

    userDf = userDf.drop(columns=['count','attribute'])

    return userDf

# 加速版本，目前看起来效果还不错
def meanAttributionFast(userDf, skanDf):
    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')
    userDf['install_timestamp'] = pd.to_numeric(userDf['install_timestamp'], errors='coerce')
    S = 60 * 60
    # S = 600
    userDf['install_timestamp'] = (userDf['install_timestamp'] // S) * S
    userDf['count'] = 1
    userDf['install_date'] = userDf['install_date'].fillna('')
    userDf = userDf.groupby(['cv', 'country_code', 'install_timestamp','install_date']).agg({'customer_user_id': lambda x: '|'.join(x),'count': 'sum'}).reset_index()
    skanDf['min_valid_install_timestamp'] = (skanDf['min_valid_install_timestamp'] // S) * S
    skanDf['max_valid_install_timestamp'] = (skanDf['max_valid_install_timestamp'] // S) * S
    skanDf['count'] = 1
    skanDf = skanDf.groupby(['cv', 'country_code_list', 'min_valid_install_timestamp', 'max_valid_install_timestamp','campaign_id','media']).agg({'count': 'sum'}).reset_index(drop = False)
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
            total_matching_count = matching_rows['count'].sum()

            if total_matching_count > 0:
                rate = item['count'] / total_matching_count

                userDf.loc[condition, 'total media rate'] += rate
                user_indices.extend(matching_rows.index)
                medias.extend([media] * len(matching_rows))
                skan_indices.extend([index] * len(matching_rows))
                rates.extend([rate] * len(matching_rows))
            else:
                new_pending_skan_indices.append(index)

        print('未分配成功：', len(new_pending_skan_indices))
        attributeDf2 = pd.DataFrame({'user index': user_indices, 'media': medias, 'skan index': skan_indices, 'rate': rates})
        attributeDf = attributeDf.append(attributeDf2, ignore_index=True)

        # 找出需要重新分配的行
        grouped_attributeDf = attributeDf.groupby('user index')['rate'].sum()
        rows_to_redistribute = userDf[userDf.index.isin(grouped_attributeDf[grouped_attributeDf > 1].index)]

        # 记录需要移除的skan index
        removed_skan_indices = set()

        # 对每一行，找出需要重新分配的skan条目，并将它们添加到new_pending_skan_indices列表中
        for index, row in tqdm(rows_to_redistribute.iterrows(), total=len(rows_to_redistribute)):
            attribute_list = attributeDf[attributeDf['user index'] == index]
            attribute_list_sorted = attribute_list.sort_values('rate', ascending=False)

            accumulated_rate = 0

            for _, item in attribute_list_sorted.iterrows():
                accumulated_rate += item['rate']
                if accumulated_rate > 1:
                    removed_skan_indices.add(item['skan index'])

        # 打印attributeDf中的行数中不同skan index的数量
        attributeDf = attributeDf[~attributeDf['skan index'].isin(removed_skan_indices)]
        
        # 更新待分配的skan索引列表
        pending_skan_indices = list(set(new_pending_skan_indices).union(removed_skan_indices))
        print(f"第 {i + 1} 次分配结束，还有 {len(pending_skan_indices)} 个待分配条目")
        
        # 更新media rate
        for media in mediaList:
            userDf[media + ' rate'] = 0
            userDf[media + ' rate'] = attributeDf[attributeDf['media'] == media].groupby('user index')['rate'].sum()
            userDf[media + ' rate'] = userDf[media + ' rate'].fillna(0)

        # 计算每个媒体的未分配的用户数
        pending_counts = skanDf.loc[pending_skan_indices].groupby('media')['count'].sum()
        print("每个媒体的未分配的用户数：")
        print(pending_counts)
        
        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('media')['count'].sum()
        print("每个媒体的总的skan用户数：")
        print(total_counts)
        
        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts
        print("每个媒体的未分配占比：")
        print(pending_ratios)

    # 拆分customer_user_id
    userDf['customer_user_id'] = userDf['customer_user_id'].apply(lambda x: x.split('|'))
    userDf = userDf.explode('customer_user_id')

    return userDf

def meanAttributionFastv2(userDf, skanDf):
    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')
    userDf['install_timestamp'] = pd.to_numeric(userDf['install_timestamp'], errors='coerce')
    S = 60 * 60
    # S = 600
    userDf['install_timestamp'] = (userDf['install_timestamp'] // S) * S
    userDf['count'] = 1
    userDf['install_date'] = userDf['install_date'].fillna('')
    userDf = userDf.groupby(['cv', 'country_code', 'install_timestamp','install_date']).agg({'customer_user_id': lambda x: '|'.join(x),'count': 'sum'}).reset_index()
    skanDf['min_valid_install_timestamp'] = (skanDf['min_valid_install_timestamp'] // S) * S
    skanDf['max_valid_install_timestamp'] = (skanDf['max_valid_install_timestamp'] // S) * S
    skanDf['count'] = 1
    skanDf = skanDf.groupby(['cv', 'country_code_list', 'min_valid_install_timestamp', 'max_valid_install_timestamp','campaign_id','media']).agg({'count': 'sum'}).reset_index(drop = False)
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
            total_matching_count = matching_rows['count'].sum()

            if total_matching_count > 0:
                rate = item['count'] / total_matching_count

                userDf.loc[condition, 'total media rate'] += rate
                user_indices.extend(matching_rows.index)
                medias.extend([media] * len(matching_rows))
                skan_indices.extend([index] * len(matching_rows))
                rates.extend([rate] * len(matching_rows))
            else:
                new_pending_skan_indices.append(index)

        print('未分配成功：', len(new_pending_skan_indices))
        attributeDf2 = pd.DataFrame({'user index': user_indices, 'media': medias, 'skan index': skan_indices, 'rate': rates})
        attributeDf = attributeDf.append(attributeDf2, ignore_index=True)

        # 找出需要重新分配的行
        grouped_attributeDf = attributeDf.groupby('user index')['rate'].sum()

        index_to_redistribute = grouped_attributeDf[grouped_attributeDf > 1].index

        sorted_rows_to_redistribute = attributeDf[attributeDf['user index'].isin(index_to_redistribute)].sort_values(
            ['user index', 'rate'], ascending=[True, False])
        sorted_rows_to_redistribute['cumulative_rate'] = sorted_rows_to_redistribute.groupby('user index')['rate'].cumsum()

        # 找出需要移除的行
        rows_to_remove = sorted_rows_to_redistribute[sorted_rows_to_redistribute['cumulative_rate'] > 1]

        # 记录需要移除的skan index
        removed_skan_indices = set(rows_to_remove['skan index'])

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
        pending_counts = skanDf.loc[pending_skan_indices].groupby('media')['count'].sum()
        print("每个媒体的未分配的用户数：")
        print(pending_counts)
        
        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('media')['count'].sum()
        print("每个媒体的总的skan用户数：")
        print(total_counts)
        
        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts
        print("每个媒体的未分配占比：")
        print(pending_ratios)

    # 拆分customer_user_id
    userDf['customer_user_id'] = userDf['customer_user_id'].apply(lambda x: x.split('|'))
    userDf = userDf.explode('customer_user_id')

    return userDf



def main():
    init()
    # 1、获取skan数据
    skanDf = getSKANDataFromMC(dayStr,days)
    # 将skanDf中media不属于mediaList的media改为other
    skanDf.loc[~skanDf['media'].isin(mediaList),'media'] = 'other'
    # skanDf = skanDf[skanDf['media'].isin(mediaList)]
    # 对数据进行简单修正，将cv>=32 的数据 cv 减去 32，其他的数据不变
    skanDf['cv'] = pd.to_numeric(skanDf['cv'], errors='coerce')
    skanDf['cv'] = skanDf['cv'].fillna(-1)
    skanDf.loc[skanDf['cv']>=32,'cv'] -= 32
    # 2、计算合法的激活时间范围
    skanDf = skanAddValidInstallDate(skanDf)
    # 3、获取广告信息
    minValidInstallTimestamp = skanDf['min_valid_install_timestamp'].min()
    maxValidInstallTimestamp = skanDf['max_valid_install_timestamp'].max()
    minValidInstallTimestamp -= 72*3600
    print('minValidInstallTimestamp:',minValidInstallTimestamp)
    print('maxValidInstallTimestamp:',maxValidInstallTimestamp)
    campaignGeo2Df = getCountryFromCampaign(minValidInstallTimestamp, maxValidInstallTimestamp)
    campaignGeo2Df = getCountryFromCampaign2(campaignGeo2Df)
    # 4、将skan数据和广告信息合并，获得skan中的国家信息
    skanDf = skanAddGeo(skanDf,campaignGeo2Df)
    print('skanDf (head 5):')
    print(skanDf.head(5))
    # 5、获取af数据
    afDf = getAfDataFromMC(minValidInstallTimestamp, maxValidInstallTimestamp)
    userDf = addCv(afDf,getCvMap())

    # userDf = pd.read_csv('/src/data/zk/userDf2.csv',dtype={'customer_user_id':str})
    # skanDf = pd.read_csv('/src/data/zk/skanDf2.csv')

    # 进行归因
    attDf = meanAttributionFastv2(userDf,skanDf)
    # print('attDf (head 5):')
    # print(attDf.head(5))
    return attDf

# 下面部分就只有线上环境可以用了
from odps.models import Schema, Column, Partition
def createTable():
    if 'o' in globals():
        columns = [
            Column(name='customer_user_id', type='string', comment='from ods_platform_appsflyer_events.customer_user_id'),
            Column(name='install_date', type='string', comment='install date,like 2023-05-31'),
        ]
        for media in mediaList:
            # media里面有空格，将空格替换为下划线
            media = media.replace(' ','_')
            columns.append(Column(name='%s_rate'%(media), type='double', comment='%s媒体归因值概率'%(media)))

        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('topwar_ios_funplus02_adv_uid_mutidays', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def writeTable(df,dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('topwar_ios_funplus02_adv_uid_mutidays')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')

attDf = main()
# 将所有media的归因值相加，得到总归因值，总归因值为0的，丢掉
attDf['total rate'] = attDf[['%s rate'%(media) for media in mediaList]].sum(axis=1)
attDf = attDf[attDf['total rate'] > 0]

# install_timestamp 列是一个unix s时间戳，需要转换为日期，并存入install_date列
attDf['install_date'] = attDf['install_timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
# day是将install_timestamp转换为日期，格式为20230531
attDf['day'] = attDf['install_timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y%m%d'))
attDf = attDf[['customer_user_id','install_date'] + ['%s rate'%(media) for media in mediaList] + ['day']]

# attDf 列改名 所有列名改为 小写
attDf.columns = [col.lower() for col in attDf.columns]
# attDf 列改名 
# 'facebook ads rate' -> 'facebook_ads_rate'	
# 'googleadwords_int rate' -> 'googleadwords_int_rate'	
# 'bytedanceglobal_int rate' - > 'bytedanceglobal_int_rate'
attDf.columns = [col.replace(' ','_') for col in attDf.columns]

# print('try to write table:')
# print(attDf.head(5))

createTable()

# 这里计算所有可以更新的安装日期，简单的说就是获取最小skan的前一天，在至少获取5天的前提下，这一天是完整的
dayBeforeStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=days+1)).strftime('%Y%m%d')
print('写入开始日期:',dayBeforeStr)
attDf = attDf[attDf['day'] >= dayBeforeStr]
# 要按照day分区，所以要先按照day分组，然后再写入表
# 先找到所有的day，升序排列
days = attDf['day'].unique()
days.sort()
for dayStr in days:
    # 找到dayStr对应的数据
    dayDf = attDf[attDf['day'] == dayStr]
    # 将day列丢掉
    dayDf = dayDf.drop(columns=['day'])
    # 写入表
    writeTable(dayDf,dayStr)

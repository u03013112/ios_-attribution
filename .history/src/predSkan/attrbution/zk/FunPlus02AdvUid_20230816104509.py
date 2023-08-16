# FunPlus02Adv uid版 不再使用af id，而是使用uid
# 主要针对归因时的过分配问题
# 当一个用户被分配到多个媒体的概率超过1的时候代表这个用户被过分配了
# 这时需要针对这个用户进行重分配

# 步骤如下
# 1、给userDf添加列，列名为attribute,默认值是[]。里面用于存储归因的媒体名，skan index，和对应的count。
# 格式是{'media': 'media1', 'skan index': 1, 'count': 0.5},attribute列存储的是这种结构的列表。
# 2、遍历skanDf，每行做如下处理：获取media，cv，min_valid_install_timestamp和max_valid_install_timestamp。
# 在userDf中找到userDf.cv == skanDf.cv，并且 skanDf.max_valid_install_timestamp >= userDf.install_timestamp >= skanDf.min_valid_install_timestamp 的行。
# 该行的attribute列的列表添加一个字典，media，skan index都取自skanDf这一行。
# count的值是1/N，N是符合上面条件的行数。比如通过cv与时间戳过滤找到符合的行是2，则'count'值就是1/2。
# 3、找到userDf中attribute列中count的和大于1的行，对这些行做如下处理：
# 找到attribute列中count最小的M个元素。使得剩下的元素的count的和小于等于1。
# 从attribute列中删除这M个元素。
# 找到usdDf所有attribute列中包含此skan index的行。重新计算count，count为1/N，N为剩余usdDf所有attribute列中包含此skan index的行数。
# 4、检查userDf中是否还有行的attribute列中count的和大于1，如果有重复步骤3。
# 5、汇总userDf中attribute列中的media，计算每个media的count的和，作为最终的归因结果。
# 格式采用之前归因方案1的格式，即 给userDf添加列，按照mediaList中的媒体顺序，添加列，列名为mediaList中的媒体名+' count'
# count值是从attribute列中的按照media汇总count的值。
# 6、返回userDf

import io

import numpy as np
import pandas as pd
from tqdm import tqdm

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
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
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

        dayStr = '20230601'
    
    print('dayStr:', dayStr)

# 只针对下面媒体进行归因，其他媒体不管
mediaList = [
    'Facebook Ads',
    'googleadwords_int',
    'bytedanceglobal_int',
]

def getSKANDataFromMC(dayStr):
    sql = f'''
        SELECT
            ad_network_campaign_id as campaign_id,
            media_source as media,
            skad_conversion_value as cv,
            timestamp as postback_timestamp
        FROM 
            ods_platform_appsflyer_skad_details
        WHERE
            day = '{dayStr}'
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
    # cv 小于 0 的 是 null 值，当做付费用户匹配，范围稍微大一点
    skanDf.loc[skanDf['cv'] < 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=72)
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=48)
    skanDf.loc[skanDf['cv'] > 0, 'min_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=72)

    skanDf.loc[:, 'max_valid_install_timestamp'] = skanDf['postback_timestamp'] - pd.Timedelta(hours=24)
    # 将时间戳转换为秒
    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'].view(np.int64) // 10 ** 9
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'].view(np.int64) // 10 ** 9
    return skanDf

def getCountryFromCampaign(minValidInstallTimestamp, maxValidInstallTimestamp):
    # minValidInstallTimestamp 向前推8天，为了让出广告的转化窗口
    minValidInstallTimestamp -= 24 * 8 * 3600
    # maxValidInstallTimestamp += 24 * 3600
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
    groupedDf = df.groupby(['day', 'media_source', 'campaign_id', 'cost']).agg({
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
    # 将minValidInstallTimestamp和maxValidInstallTimestamp转换为字符串
    minValidInstallTimestampStr = datetime.fromtimestamp(minValidInstallTimestamp).strftime('%Y-%m-%d %H:%M:%S')
    maxValidInstallTimestampStr = datetime.fromtimestamp(maxValidInstallTimestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    # 放宽条件，将minValidInstallTimestampStr和maxValidInstallTimestampStr分别向前向后推一天
    minValidInstallTimestamp -= 24 * 3600
    maxValidInstallTimestamp += 24 * 3600
    # 另外minValidInstallTimestamp和maxValidInstallTimestamp转化成格式为'20230301'
    minValidInstallTimestampDayStr = datetime.fromtimestamp(minValidInstallTimestamp).strftime('%Y%m%d')
    maxValidInstallTimestampDayStr = datetime.fromtimestamp(maxValidInstallTimestamp).strftime('%Y%m%d')

    # 修改后的SQL语句，r1usd用来计算cv，r2usd可能可以用来计算48小时cv，暂时不用r7usd，因为这个时间7日应该还没有完整。
    sql = f'''
        SELECT
            sub.customer_user_id,
            sub.install_timestamp,
            SUM(
                CASE
                    WHEN e.event_timestamp <= sub.install_timestamp + 24 * 3600 THEN e.event_revenue_usd
                    ELSE 0
                END
            ) as r1usd,
            SUM(
                CASE
                    WHEN e.event_timestamp <= sub.install_timestamp + 48 * 3600 THEN e.event_revenue_usd
                    ELSE 0
                END
            ) as r2usd,
            SUM(
                CASE
                    WHEN e.event_timestamp <= sub.install_timestamp + 168 * 3600 THEN e.event_revenue_usd
                    ELSE 0
                END
            ) as r7usd,
            to_char(
                to_date(sub.install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sub.country_code
        FROM
            (
                SELECT
                    customer_user_id,
                    MIN(install_timestamp) as install_timestamp,
                    MIN(install_time) as install_time,
                    MIN(country_code) OVER (
                        PARTITION BY customer_user_id
                        ORDER BY
                            install_timestamp ROWS UNBOUNDED PRECEDING
                    ) as country_code
                FROM
                    ods_platform_appsflyer_events
                WHERE
                    app_id = 'id1479198816'
                    AND zone = 0
                    AND day BETWEEN '{minValidInstallTimestampDayStr}'
                    AND '{maxValidInstallTimestampDayStr}'
                    AND install_time BETWEEN '{minValidInstallTimestampStr}'
                    AND '{maxValidInstallTimestampStr}'
                    AND customer_user_id IS NOT NULL
                GROUP BY
                    customer_user_id,
                    install_timestamp,
                    install_time,
                    country_code
            ) AS sub
            JOIN ods_platform_appsflyer_events e ON sub.customer_user_id = e.customer_user_id
        WHERE
            day BETWEEN '{minValidInstallTimestampDayStr}'
                AND '{maxValidInstallTimestampDayStr}'
        GROUP BY
            sub.customer_user_id,
            sub.install_timestamp,
            sub.install_time,
            sub.country_code;
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

    # 对userDf进行汇总
    userDf['install_timestamp'] = userDf['install_timestamp'] // 600
    userDf['count'] = 1
    userDf = userDf.groupby(['cv', 'country_code', 'install_timestamp','install_date']).agg({'customer_user_id': lambda x: '|'.join(x),'count': 'sum'}).reset_index()
    userDf['attribute'] = userDf.apply(lambda x: [], axis=1)

    skanDf['min_valid_install_timestamp'] = skanDf['min_valid_install_timestamp'] // 600
    skanDf['max_valid_install_timestamp'] = skanDf['max_valid_install_timestamp'] // 600
    skanDf['count'] = 1
    skanDf = skanDf.groupby(['cv', 'country_code_list', 'min_valid_install_timestamp', 'max_valid_install_timestamp','campaign_id','media']).agg({'count': 'sum'}).reset_index()

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
            if i == N-1:
                # 由于经常有分不出去的情况，所以最后一次分配，不考虑国家
                item_country_code_list = ''
                print('最后一次分配，不考虑国家')
                min_valid_install_timestamp -= 24*3600//600
            else:
                item_country_code_list = item['country_code_list']

            if cv < 0:
                # print('cv is null')
                if item_country_code_list == '':
                    condition = (
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) &
                        (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
                    )
                else:
                    country_code_list = item_country_code_list.split('|')
                    condition = (
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) &
                        (userDf['country_code'].isin(country_code_list)) &
                        (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
                    )
            else:
                # 先检查item_country_code_list是否为空
                if item_country_code_list == '':
                    condition = (
                        (userDf['cv'] == cv) &
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) &
                        (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
                    )
                else:
                    country_code_list = item_country_code_list.split('|')
                    condition = (
                        (userDf['cv'] == cv) &
                        (userDf['install_timestamp'] >= min_valid_install_timestamp) &
                        (userDf['install_timestamp'] <= max_valid_install_timestamp) &
                        (userDf['country_code'].isin(country_code_list)) &
                        (userDf['attribute'].apply(lambda x: sum([elem['rate'] for elem in x]) < 1))
                    )

            matching_rows = userDf[condition]
            total_matching_count = matching_rows['count'].sum()

            if total_matching_count > 0:
                rate = item['count'] / total_matching_count
                userDf.loc[condition, 'attribute'] = userDf.loc[condition, 'attribute'].apply(lambda x: x + [{'media': media, 'skan index': index, 'rate': rate}])
            else:
                new_pending_skan_indices.append(index)

        if i < N-1:
            # 这部分是将过分配的部分重新分配
            # 最后一次分配不需要重新分配，否则会有一定的低估

            # 找出需要重新分配的行
            rows_to_redistribute = userDf[userDf['attribute'].apply(lambda x: sum([item['rate'] for item in x]) > 1)]

            # 对每一行，找出需要重新分配的skan条目，并将它们添加到new_pending_skan_indices列表中
            for _, row in tqdm(rows_to_redistribute.iterrows(), total=len(rows_to_redistribute)):
                attribute_list = row['attribute']
                total_rate = sum([item['rate'] for item in attribute_list])
                max_rate_to_remove = total_rate - 1

                attribute_list_sorted = sorted(attribute_list, key=lambda x: x['rate'])
                removed_items = []
                removed_rate = 0

                for item in attribute_list_sorted:
                    if removed_rate + item['rate'] <= max_rate_to_remove:
                        removed_rate += item['rate']
                        removed_items.append(item)
                    else:
                        break

                for item in removed_items:
                    attribute_list.remove(item)
                    new_pending_skan_indices.append(item['skan index'])

            pending_skan_indices = new_pending_skan_indices
            # pending_skan_indices 要进行排重
            pending_skan_indices = list(set(pending_skan_indices))

        print(f"第 {i + 1} 次分配结束，还有 {len(pending_skan_indices)} 个待分配条目")
        pendingDf = skanDf.loc[pending_skan_indices]
        
        print('待分配的skan数量：')
        print(pendingDf.groupby('media').size())

    # 拆分customer_user_id
    userDf['customer_user_id'] = userDf['customer_user_id'].apply(lambda x: x.split('|'))
    userDf = userDf.explode('customer_user_id')

    for media in mediaList:
        userDf[media + ' rate'] = userDf['attribute'].apply(lambda x: sum([item['rate'] for item in x if item['media'] == media]))

    userDf = userDf.drop(columns=['count','attribute'])

    return userDf

def main():
    init()
    # 1、获取skan数据
    skanDf = getSKANDataFromMC(dayStr)
    skanDf = skanDf[skanDf['media'].isin(mediaList)]
    # 对数据进行简单修正，将cv>=32 的数据 cv 减去 32，其他的数据不变
    skanDf['cv'] = pd.to_numeric(skanDf['cv'], errors='coerce')
    skanDf['cv'] = skanDf['cv'].fillna(-1)
    skanDf.loc[skanDf['cv']>=32,'cv'] -= 32
    # 2、计算合法的激活时间范围
    skanDf = skanAddValidInstallDate(skanDf)
    # 3、获取广告信息
    minValidInstallTimestamp = skanDf['min_valid_install_timestamp'].min()
    maxValidInstallTimestamp = skanDf['max_valid_install_timestamp'].max()
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
    # 进行归因
    attDf = meanAttribution(userDf,skanDf)
    print('attDf (head 5):')
    print(attDf.head(5))
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
            columns.append(Column(name='%s rate'%(media), type='double', comment='%s媒体归因值概率'%(media)))

        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('topwar_ios_funplus02_adv_uid', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def writeTable(df,dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('topwar_ios_funplus02_adv_uid')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')

attDf = main()
# 将所有media的归因值相加，得到总归因值，总归因值为0的，丢掉
attDf['total rate'] = attDf[['%s rate'%(media) for media in mediaList]].sum(axis=1)
attDf = attDf[attDf['total rate'] > 0]

# # 将所有media的归因值大于1的，归因值设置为1
# for media in mediaList:
#     attDf.loc[attDf['%s count'%(media)] > 1,'%s count'%(media)] = 1

# # 总归因值大于1的，按照比例缩小，使得总归因值等于1
# attDf['total count'] = attDf[['%s count'%(media) for media in mediaList]].sum(axis=1)
# # 打印影响的行（前5行）
# print(attDf.loc[attDf['total count'] > 1,['%s count'%(media) for media in mediaList]].head(5))
# attDf.loc[attDf['total count'] > 1,['%s count'%(media) for media in mediaList]] = attDf.loc[attDf['total count'] > 1,['%s count'%(media) for media in mediaList]].div(attDf['total count'], axis=0)
# # 打印修改后的行（前5行）
# print(attDf.loc[attDf['total count'] > 1,['%s count'%(media) for media in mediaList]].head(5))

attDf = attDf[['customer_user_id','install_date'] + ['%s rate'%(media) for media in mediaList]]

# attDf 列改名 所有列名改为 小写
attDf.columns = [col.lower() for col in attDf.columns]
# attDf 列改名 
# 'facebook ads rate' -> 'facebook_ads_rate'	
# 'googleadwords_int rate' -> 'googleadwords_int_rate'	
# 'bytedanceglobal_int rate' - > 'bytedanceglobal_int_rate'
attDf.columns = [col.replace(' ','_') for col in attDf.columns]

createTable()
writeTable(attDf,dayStr)

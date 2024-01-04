# FunPlus02AdvUidMutiDays Campaign版本2
# 将国家分组成几个大区，分别是GCC、KR、US、JP、other，这是iOS海外KPI分组
# 不再放宽国家限制，这样至少大范围上用户不会再出现KPI国家错误，即只投放了US的campaign缺匹配到一些别的国家的用户的情况
# 为了增加匹配率，将时间范围向前推5天，分10次匹配，每次向前推12小时。之前版本是只匹配3次，每次向前推24小时。

import io

import numpy as np
import pandas as pd
from tqdm import tqdm

from datetime import datetime, timedelta

# 参数dayStr，是业务日期，格式为'20210404'
# 参数days，是要处理多少天的数据，是一个整数，至少要大于等于5

# 为了兼容本地调试，要在所有代码钱调用此方法
def init():
    global execSql
    global dayStr
    global days

    if 'o' in globals():
        print('this is online version')

        from odps import options
        # UTC+0
        options.sql.settings = {'odps.sql.timezone':'Africa/Accra'}

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

        dayStr = '20231110'
        days = '15'

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
    # 在这里做一些过滤处理
    # 其中postback_timestamp应该最大值小于dayStr的23:59:59，但是目前发现有个别的大于这个值
    # 所以将大于这个值的postback_timestamp改为dayStr的23:59:59
    # 这里原来认为postback_timestamp是unix时间戳，其实是类似'2023-09-01 15:54:22'这样的字符串，另外他的时区不是utc0
    # 需要先将他转成utc0的unix时间戳，然后最大值过滤，然后再转回原来的字符串格式
    # 为时间字符串添加UTC时区标识
    # 在新列中进行操作
    df['temp_timestamp'] = df['postback_timestamp'] + '+00:00'
    df['temp_timestamp'] = pd.to_datetime(df['temp_timestamp'], utc=True)
    df['temp_timestamp'] = df['temp_timestamp'].view(np.int64) // 10 ** 9

    max_timestamp_str = dayStr + '235959' + '+00:00'
    max_timestamp = pd.to_datetime(max_timestamp_str, utc=True)
    print('max_timestamp:', max_timestamp)
    max_timestamp = max_timestamp.to_pydatetime().timestamp()
    print('max_timestamp:', max_timestamp)

    affected_rows = df[df['temp_timestamp'] > max_timestamp]
    print('尝试纠正postback_timestamp，影响到的行数：', len(affected_rows))

    # 显示修改前的示例数据
    print('\n修改前的示例数据:')
    print(affected_rows.head(5))

    df.loc[df['temp_timestamp'] > max_timestamp, 'temp_timestamp'] = max_timestamp

    # 将修改后的值赋回到 'postback_timestamp' 列
    # df['postback_timestamp'] = pd.to_datetime(df['temp_timestamp'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
    # 只对影响到的行做 'postback_timestamp' 的覆盖
    df.loc[affected_rows.index, 'postback_timestamp'] = pd.to_datetime(df.loc[affected_rows.index, 'temp_timestamp'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

    # 显示修改后的示例数据
    print('\n修改后的示例数据:')
    print(df.loc[affected_rows.index].head(5))

    # 删除临时列
    df.drop(columns=['temp_timestamp'], inplace=True)

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

geoMap = [
    # 沙特、阿联酋、科威特、卡塔尔、阿曼、巴林
    {'name':'GCC','codeList':['SA','AE','KW','QA','OM','BH']},
    {'name':'KR','codeList':['KR']},
    {'name':'US','codeList':['US']},
    {'name':'JP','codeList':['JP']},
]

# 改一下格式
def getCountryFromCampaign2(df):
    df['country_code'].fillna('unknown', inplace=True)

    # 这里要做一定的忽略，有些国家花费过小，可能是统计误差
    # 将同一支campaign中花费不足1%的国家忽略掉
    # 按照campaign和国家进行分组，计算每个国家在每个campaign中的总花费
    country_cost = df.groupby(['campaign_id', 'country_code'])['cost'].sum().reset_index()

    # 计算每个campaign的总花费
    total_cost = country_cost.groupby('campaign_id')['cost'].sum()

    # 将总花费合并到国家花费数据中
    country_cost = country_cost.merge(total_cost, on='campaign_id', suffixes=('', '_total'))

    # 计算每个国家在每个campaign中的花费占比
    country_cost['cost_ratio'] = country_cost['cost'] / country_cost['cost_total']

    # 将花费占比信息合并回原始的df
    df = df.merge(country_cost[['campaign_id', 'country_code', 'cost_ratio']], on=['campaign_id', 'country_code'])

    # 筛选掉花费占比不足1%的国家
    df = df[df['cost_ratio'] >= 0.03]

    df['geo'] = 'other'
    for geo in geoMap:
        df.loc[df['country_code'].isin(geo['codeList']), 'geo'] = geo['name']

    df = df.groupby(['day', 'media_source', 'campaign_id','geo']).sum().reset_index()

    # 对结果进行分组，并将country_code连接成逗号分隔的字符串
    groupedDf = df.groupby(['day', 'media_source', 'campaign_id']).agg({
        'geo': lambda x: '|'.join(sorted(set(x)))
    }).reset_index()

    # 重命名country_code列为country_code_list
    groupedDf.rename(columns={'geo': 'country_code_list'}, inplace=True)

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
            AND mediasource <> 'Apple Search Ads'
        GROUP BY
            game_uid,
            install_timestamp,
            country;
    '''

    print(sql)
    df = execSql(sql)
    return df

# 获得ASA用户数据，这里直接从二次归因表中获得，只需要uid，安装时间
def getAsaDataFromMC(minValidInstallTimestamp, maxValidInstallTimestamp):
    sql = f'''
        SELECT
            game_uid as customer_user_id,
            campaign_id,
            TO_CHAR(
                from_unixtime(cast(install_timestamp as bigint)),
                "yyyymmdd"
            ) as day,
            TO_CHAR(
                from_unixtime(cast(install_timestamp as bigint)),
                "yyyy-mm-dd hh:mi:ss"
            ) as install_date
        FROM
            rg_bi.tmp_unique_id
        WHERE
            app = 102
            AND app_id = 'id1479198816' 
            AND install_timestamp BETWEEN '{minValidInstallTimestamp}'
            AND '{maxValidInstallTimestamp}'
            AND game_uid IS NOT NULL
            AND mediasource = 'Apple Search Ads'
        GROUP BY
            game_uid,
            campaign_id,
            day,
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

import gc
def meanAttributionFastv2(userDf, skanDf):
    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')
    userDf['install_timestamp'] = pd.to_numeric(userDf['install_timestamp'], errors='coerce')
    S = 60 * 60
    userDf['install_timestamp'] = (userDf['install_timestamp'] // S) * S
    userDf['count'] = 1
    userDf['install_date'] = userDf['install_date'].fillna('')
    userDf = userDf.groupby(['cv', 'country_code', 'install_timestamp','install_date']).agg({'customer_user_id': lambda x: '|'.join(x),'count': 'sum'}).reset_index()

    skanDf['min_valid_install_timestamp'] = (skanDf['min_valid_install_timestamp'] // S) * S
    skanDf['max_valid_install_timestamp'] = (skanDf['max_valid_install_timestamp'] // S) * S
    skanDf['count'] = 1
    skanDf = skanDf.groupby(['cv', 'country_code_list', 'min_valid_install_timestamp', 'max_valid_install_timestamp','campaign_id','media','usd']).agg({'count': 'sum'}).reset_index(drop = False)
    skanDf['usd x count'] = skanDf['usd'] * skanDf['count']

    print('skanDf:')
    print(skanDf.head(10))

    pending_skan_indices = skanDf.index.tolist()
    N = 10
    attributeDf = pd.DataFrame(columns=['user index', 'campaignId', 'skan index', 'rate'])

    campaignList = skanDf.loc[~skanDf['campaign_id'].isnull()]['campaign_id'].unique().tolist()
    # print('campaignList:', campaignList)
    for campaignId in campaignList:
        userDf['%s rate'%(campaignId)] = 0

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
        
        print('第%d次分配，时间范围向前推%d天'%(i+1,i))
        
        for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
            campaignId = str(item['campaign_id'])
            cv = item['cv']
            min_valid_install_timestamp = item['min_valid_install_timestamp']
            max_valid_install_timestamp = item['max_valid_install_timestamp']
            
            min_valid_install_timestamp -= i*12*3600
            item_country_code_list = item['country_code_list']
            # 最后一次分配，忽略国家限制
            if i == N-1:
                item_country_code_list = ''

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
                campaignIds.extend([campaignId] * len(matching_rows))
                skan_indices.extend([index] * len(matching_rows))
                rates.extend([rate] * len(matching_rows))
                # print(user_indices)
            else:
                new_pending_skan_indices.append(index)

        print('未分配成功：', len(new_pending_skan_indices))
        attributeDf2 = pd.DataFrame({'user index': user_indices, 'campaignId': campaignIds, 'skan index': skan_indices, 'rate': rates})
        
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
        for campaignId in campaignList:
            userDf[campaignId + ' rate'] = 0
            userDf[campaignId + ' rate'] = attributeDf[attributeDf['campaignId'] == campaignId].groupby('user index')['rate'].sum()
            userDf[campaignId + ' rate'] = userDf[campaignId + ' rate'].fillna(0)
        
        # 计算每个媒体的未分配的用户数
        pending_counts = skanDf.loc[pending_skan_indices].groupby('campaign_id')['count'].sum()
        pending_counts = pending_counts.fillna(0)
        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('campaign_id')['count'].sum()
        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts
        # 将三个计算结果合并为一个DataFrame
        result_df = pd.concat([total_counts, pending_counts, pending_ratios], axis=1)
        # 设置列名和索引
        result_df.columns = ['总skan用户数', '未分配用户数', '未分配比例']
        result_df.index.name = 'campaign_id'
        # 将未分配比例转换为2位小数的百分比
        result_df['未分配比例'] = result_df['未分配比例'].apply(lambda x: f"{x*100:.2f}%")

        pending_usd = skanDf.loc[pending_skan_indices]['usd x count'].sum()
        total_usd = skanDf['usd x count'].sum()
        pending_usd_ratio = pending_usd / total_usd
        print('所有的未分配金额占比：')
        print(pending_usd_ratio)

        # 打印结果
        # print(result_df.sort_values('未分配用户数', ascending=False))

        # 计算所有的未分配用户占比
        total_pending_ratio = pending_counts.sum() / total_counts.sum()
        print("所有的未分配用户占比：")
        print(total_pending_ratio)

        gc.collect()
    
    # 拆分customer_user_id
    userDf['customer_user_id'] = userDf['customer_user_id'].apply(lambda x: x.split('|'))
    userDf = userDf.explode('customer_user_id')

    userDf = userDf[userDf.filter(like='rate', axis=1).sum(axis=1) > 0]
    # install_timestamp 列是一个unix s时间戳，需要转换为日期，并存入install_date列
    userDf['install_date'] = userDf['install_timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    # day是将install_timestamp转换为日期，格式为20230531
    userDf['day'] = userDf['install_timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y%m%d'))
    # 只保留需要的列
    campaignIdRateList = userDf.filter(like='rate', axis=1).columns.tolist()
    # 如果campaignIdRateList中包含'total media rate'，则删除
    if 'total media rate' in campaignIdRateList:
        campaignIdRateList.remove('total media rate')
    userDf = userDf[['customer_user_id', 'install_date', 'day'] + campaignIdRateList]
    # 类型优化
    for col in userDf.iloc[:, 3:].columns:
        # 原本是float64，转换为float32，精度足够
        userDf[col] = userDf[col].astype('float32')

    return userDf.reset_index(drop=True)

# 检查是否已经获得了af数据
def check(dayStr):
    sql = f'''
        select
            *
        from 
            ods_platform_appsflyer_skad_details
        where
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
    if len(df) <= 0:
        raise Exception('没有有效的获得af skan数据，请稍后重试')
    return

# 下面部分就只有线上环境可以用了
from odps.models import Schema, Column, Partition
def createTable():
    if 'o' in globals():
        columns = [
            Column(name='customer_user_id', type='string', comment='from ods_platform_appsflyer_events.customer_user_id'),
            Column(name='install_date', type='string', comment='install date,like 2023-05-31'),
            Column(name='campaign_id', type='string', comment='campaign_id,like 1772649174232113'),
            Column(name='rate', type='double', comment='rate,lile 0.1'),
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('topwar_ios_funplus02_adv_uid_mutidays_campaign2', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

def deleteTable(dayStr):
    print('try to delete table:',dayStr)
    if 'o' in globals():
        t = o.get_table('topwar_ios_funplus02_adv_uid_mutidays_campaign2')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)

def writeTable(df,dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('topwar_ios_funplus02_adv_uid_mutidays_campaign2')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')
        print('try to write csv file')
        df.to_csv('/src/data/zk2/funplus02AdvUidMutiDaysCampaignId_%s.csv'%(dayStr),index=False)

def main():
    check(dayStr)
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
    minValidInstallTimestamp -= 10*24*3600
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

    userDf['geo'] = 'other'
    for geo in geoMap:
        userDf.loc[userDf['country_code'].isin(geo['codeList']), 'geo'] = geo['name']

    userDf.rename(columns={'country_code': 'country_code1'}, inplace=True)
    userDf.rename(columns={'geo': 'country_code'}, inplace=True)

    cvMap = getCvMap()[['conversion_value','min_event_revenue','max_event_revenue']].fillna(0)
    cvMap['avg_event_revenue'] = (cvMap['min_event_revenue'] + cvMap['max_event_revenue']) / 2
    cvMap.rename(columns={'conversion_value': 'cv','avg_event_revenue':'usd'}, inplace=True)
    cvMap = cvMap[['cv','usd']]
    
    skanDf = skanDf.merge(cvMap,on='cv',how='left')

    # userDf.to_csv('/src/data/zk/userDf2.csv',index=False)
    # skanDf.to_csv('/src/data/zk/skanDf2.csv',index=False)

    # userDf = pd.read_csv('/src/data/zk/userDf2.csv',dtype={'customer_user_id':str})
    # skanDf = pd.read_csv('/src/data/zk/skanDf2.csv')

    # 进行归因
    userDf = meanAttributionFastv2(userDf,skanDf)
    
    asaDf = getAsaDataFromMC(minValidInstallTimestamp, maxValidInstallTimestamp)
    print('asaDf:')
    print(asaDf.head(5))

    # 分天处理，解决内存问题
    # 这里计算所有可以更新的安装日期，简单的说就是获取最小skan的前一天，在至少获取5天的前提下，这一天是完整的
    dayBeforeStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=days+1)).strftime('%Y%m%d')
    print('写入开始日期:',dayBeforeStr)
    userDf = userDf[userDf['day'] >= dayBeforeStr]
    # 要按照day分区，所以要先按照day分组，然后再写入表
    # 先找到所有的day，升序排列
    daysInUserDf = userDf['day'].unique()
    daysInUserDf.sort()
    for dayStr0 in daysInUserDf:
        print('处理日期:',dayStr0)
        # 找到dayStr对应的数据
        dayDf = userDf[userDf['day'] == dayStr0]
        # 将day列丢掉
        dayDf = dayDf.drop(columns=['day'])
        # print('melt之前：')
        # dayDf.info(memory_usage='deep')
        attDf_melted = dayDf.melt(
            id_vars=['customer_user_id', 'install_date'],
            var_name='campaign_id',
            value_name='rate'
        )
        # print('melt之后：')
        # attDf_melted.info(memory_usage='deep')
        # 改用这种比较简单的方式，更加省内存
        attDf_melted['campaign_id'] = attDf_melted['campaign_id'].str[:-5]

        dayDf = attDf_melted.loc[attDf_melted['rate'] > 0]

        # 追加ASA数据
        asaDayDf = asaDf[asaDf['day'] == dayStr0].copy()
        asaDayDf['rate'] = 1
        asaDayDf['campaign_id'] = asaDayDf['campaign_id'].astype(str)
        asaDayDf = asaDayDf[['customer_user_id', 'install_date', 'campaign_id', 'rate']]
        print('追加ASA数据：')
        print(asaDayDf.head(5))

        dayDf = dayDf.append(asaDayDf, ignore_index=True)
        
        # 写入表
        writeTable(dayDf,dayStr0)

        # 释放内存
        del dayDf

init()
createTable()

main()

# 删除额外的分区，为了确保AF修改postback时间不会导致任何提前数据，双保险，与上面的postback时间戳过滤一起使用
deleteTable(dayStr)

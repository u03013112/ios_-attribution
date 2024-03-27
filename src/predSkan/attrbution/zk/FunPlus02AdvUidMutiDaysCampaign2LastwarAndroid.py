# 沿用线上算法代码，用安卓数据进行验证

# FunPlus02AdvUidMutiDays Campaign版本2 lastwar 版本
# 将国家分组成几个大区，分别是GCC、KR、US、JP、other，这是iOS海外KPI分组
# 不再放宽国家限制，这样至少大范围上用户不会再出现KPI国家错误，即只投放了US的campaign缺匹配到一些别的国家的用户的情况
# 为了增加匹配率，将时间范围向前推5天，分10次匹配，每次向前推12小时。之前版本是只匹配3次，每次向前推24小时。

import io
import os

import numpy as np
import pandas as pd
from tqdm import tqdm

from datetime import datetime, timedelta
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def makeLevels1(userDf, usd='r1usd', N=32):
    filtered_df = userDf[userDf[usd] > 0]
    df = filtered_df.sort_values([usd])
    levels = [0] * (N - 1)
    total_usd = df[usd].sum()
    target_usd = total_usd / (N)
    current_usd = 0
    group_index = 0
    for index, row in df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            levels[group_index] = row[usd]
            current_usd = 0
            group_index += 1
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



def makeSKAN(df):
    levels = makeLevels1(df,usd='r1usd',N=32)
    cvMapDf = makeCvMap(levels)
    cvDf = addCv(df,cvMapDf,usd='r1usd',cv='cv')
    
    # print(cvDf.head(10))

    # 添加postback_timestamp
    # 如果用户的r1usd == 0，postback_timestamp = install_timestamp + 24小时 + 0~24小时之间随机时间
    # 如果用户的r1usd > 0，postback_timestamp = last_timestamp + 24小时 + 0~24小时之间随机时间
    # 添加postback_timestamp
    zero_r1usd_mask = cvDf['r1usd'] == 0
    non_zero_r1usd_mask = cvDf['r1usd'] > 0

    cvDf.loc[zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[zero_r1usd_mask, 'install_timestamp'] + 24 * 3600 + np.random.uniform(0, 24 * 3600, size=zero_r1usd_mask.sum())
    cvDf.loc[non_zero_r1usd_mask, 'postback_timestamp'] = cvDf.loc[non_zero_r1usd_mask, 'install_timestamp'] + 24 * 3600 + np.random.uniform(0, 2 * 24 * 3600, size=non_zero_r1usd_mask.sum())

    # print(cvDf.head(30))

    skanDf = cvDf[['postback_timestamp','media','campaign_id','cv']]

    # postback_timestamp 转成 int
    # cv转成 int
    skanDf['postback_timestamp'] = skanDf['postback_timestamp'].astype(int)
    skanDf['cv'] = skanDf['cv'].astype(int)

    return skanDf



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
        SELECT
            install_day as day,
            campaign_id,
            mediasource as media_source,
            country as country_code,
            sum(cost_value_usd) as cost
        FROM
            rg_bi.dwd_overseas_cost_allproject
        WHERE
            app = 502
            AND app_package = 'com.fun.lastwar.gp'
            AND cost_value_usd >= 1 
            AND install_day BETWEEN '{minValidInstallTimestampDayStr}' AND '{maxValidInstallTimestampDayStr}'
        group by
            install_day,
            campaign_id,
            mediasource,
            country
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
    
def getAfDataFromMC(startDayStr, endDayStr):
    filename = f'/src/data/zk/lw_android_userDf_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print('read from file:',filename)
        return pd.read_csv(filename)
    else:
        sql1 = f'''
            SELECT
                game_uid as customer_user_id,
                install_timestamp,
                COALESCE(
                    SUM(
                    CASE
                        WHEN event_time - install_timestamp between 0
                        and 24 * 3600 THEN revenue_value_usd
                        ELSE 0
                    END
                    ),
                    0
                ) as r1usd,
                COALESCE(
                    SUM(
                    CASE
                        WHEN event_time - install_timestamp between 0
                        and 2 * 24 * 3600 THEN revenue_value_usd
                        ELSE 0
                    END
                    ),
                    0
                ) as r2usd,
                COALESCE(
                    SUM(
                    CASE
                        WHEN event_time - install_timestamp between 0
                        and 7 * 24 * 3600 THEN revenue_value_usd
                        ELSE 0
                    END
                    ),
                    0
                ) as r7usd,
                TO_CHAR(
                    from_unixtime(cast (install_timestamp as bigint)),
                    "yyyy-mm-dd"
                ) as install_date,
                country as country_code
            FROM
                rg_bi.dwd_overseas_revenue_allproject
            WHERE
                zone = '0'
                and app = 502
                and app_package = 'com.fun.lastwar.gp'
                and day BETWEEN {startDayStr}
                AND {endDayStr}
                AND game_uid IS NOT NULL
            GROUP BY
                game_uid,
                install_timestamp,
                country
            ;
        '''


        sql = f'''
        SELECT
            appsflyer_id as customer_user_id,
            install_timestamp,
            COALESCE(
            SUM(
                CASE
                WHEN event_time - install_timestamp between 0
                and 24 * 3600 THEN event_revenue_usd
                ELSE 0
                END
            ),
            0
            ) as r1usd,
            COALESCE(
            SUM(
                CASE
                WHEN event_time - install_timestamp between 0
                and 2 * 24 * 3600 THEN event_revenue_usd
                ELSE 0
                END
            ),
            0
            ) as r2usd,
            COALESCE(
            SUM(
                CASE
                WHEN event_time - install_timestamp between 0
                and 7 * 24 * 3600 THEN event_revenue_usd
                ELSE 0
                END
            ),
            0
            ) as r7usd,
            TO_CHAR(
            from_unixtime(cast (install_timestamp as bigint)),
            "yyyy-mm-dd"
            ) as install_date,
            country_code as country_code,
            media_source as media,
            campaign_id
        FROM
            rg_bi.ods_platform_appsflyer_events
        WHERE
            zone = '0'
            and app = 502
            and app_id = 'com.fun.lastwar.gp'
            and day BETWEEN '{startDayStr}'
            AND '{endDayStr}'
        GROUP BY
            appsflyer_id,
            install_timestamp,
            country_code,
            media_source,
            campaign_id
        ;
'''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
        return df

def addCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = cv1
        
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = cv1
    return userDfCopy

import gc
def meanAttributionFastv2(userDf, skanDf):
    skanDf['campaign_id'] = skanDf['campaign_id'].astype(str)

    skanDf['country_code_list'] = skanDf['country_code_list'].fillna('')
    userDf['install_timestamp'] = pd.to_numeric(userDf['install_timestamp'], errors='coerce')
    S = 60 * 60
    S2 = 24 * S
    userDf['install_timestamp'] = (userDf['install_timestamp'] // S) * S
    userDf.loc[userDf['cv'] == 0, 'install_timestamp'] = (userDf.loc[userDf['cv'] == 0, 'install_timestamp'] // S2) * S2
    userDf['count'] = 1
    userDf['install_date'] = userDf['install_date'].fillna('')
    userDf = userDf.groupby(['cv', 'country_code', 'install_timestamp','install_date']).agg({'customer_user_id': lambda x: '|'.join(x),'count': 'sum'}).reset_index()

    skanDf['min_valid_install_timestamp'] = (skanDf['min_valid_install_timestamp'] // S) * S
    skanDf['max_valid_install_timestamp'] = (skanDf['max_valid_install_timestamp'] // S) * S
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = (skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] // S2) * S2
    skanDf.loc[skanDf['cv'] == 0, 'max_valid_install_timestamp'] = (skanDf.loc[skanDf['cv'] == 0, 'max_valid_install_timestamp'] // S2) * S2
    
    skanDf['count'] = 1
    skanDf['usd'] = skanDf['usd'].fillna(0)

    skanDf = skanDf.groupby(['campaign_id','media','cv', 'country_code_list', 'min_valid_install_timestamp', 'max_valid_install_timestamp','usd','day']).agg({'count': 'sum'}).reset_index(drop = False)

    skanDf['usd x count'] = skanDf['usd'] * skanDf['count']

    # print('skanDf:')
    # print(skanDf.head(10))

    pending_skan_indices = skanDf.index.tolist()
    N = 10
    attributeDf = pd.DataFrame(columns=['user index', 'campaignId', 'skan index', 'rate'])

    campaignList = skanDf.loc[~skanDf['campaign_id'].isnull()]['campaign_id'].unique().tolist()
    # print('campaignList:', sorted(campaignList))
    print('campaignList length:', len(campaignList))

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
        
        print('第%d次分配，时间范围向前推%d小时'%(i+1,i*12))
        
        for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
            campaignId = str(item['campaign_id'])
            cv = item['cv']
            min_valid_install_timestamp = item['min_valid_install_timestamp']
            max_valid_install_timestamp = item['max_valid_install_timestamp']
            
            min_valid_install_timestamp -= i*12*3600
            item_country_code_list = item['country_code_list']
            # 最后两次分配，忽略国家限制
            if i == N-1 or i == N-2:
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
        # # 将未分配成功的skan的前10条打印出来
        # print(skanDf_to_process.loc[new_pending_skan_indices].head(10))

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

        if len(pending_skan_indices) == 0:
            print('所有的skan都已经分配完毕')
            break
    

    skanFailedDf = skanDf.loc[pending_skan_indices]
    skanFailedDf['postback_timestamp'] = 0
    writeSkanToDB(skanFailedDf,'lastwar_ios_rh_skan_failed')

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


def main():
    
    startDayStr = '20240201'
    endDayStr = '20240315'

    userDf = getAfDataFromMC(startDayStr, endDayStr)

    # 为了获得完整的7日回收，需要将最后7天的注册用户去掉
    userDf = userDf.loc[
        (userDf['install_date'] >= '2024-02-01') &
        (userDf['install_date'] <= '2024-03-07')
    ]

    skanDf = makeSKAN(userDf)
    skanDf.to_csv('/src/data/zk/lw_android_skanDf.csv',index=False)
    return
    skanDf = pd.read_csv('/src/data/zk/lw_android_skanDf.csv')
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
    skanDf.to_csv('/src/data/zk/lw_android_skanDf2.csv',index=False)
    return

    afDf = userDf[['customer_user_id','install_timestamp','r1usd','country_code']]
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

    # 将skanDf2存档
    skanDf['count'] = 1
    writeSkanToDB(skanDf,'lastwar_ios_rh_skan')

    # 进行归因
    userDf = meanAttributionFastv2(userDf,skanDf)
    print('归因完成，结果表info：')
    userDf.info(memory_usage='deep')

    asaDf = getAsaDataFromMC(minValidInstallTimestamp, maxValidInstallTimestamp)
    print('asaDf:')
    print(asaDf.head(5))

    
    userDf = userDf[userDf['day'] >= uploadDateStartStr]
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

    return

if __name__ == '__main__':
    main()



# 针对lastwar的融合归因
# 采用skan的原始表，只归因至媒体层级
# 与归因到campaign的主要区别
# 1.不再读取ods_platform_appsflyer_skad_details表，而是读取ods_platform_appsflyer_skad_postbacks_copy表
# 2.不再分国家，归因过程得到一定的简化
# 3.结论格式改变，之前是uid，install_date，campaign_id，rate。现在campaign_id改为media。media按照ods_platform_appsflyer_skad_details表中命名规则命名。
import io

import numpy as np
import pandas as pd
from tqdm import tqdm

from datetime import datetime, timedelta

def init():
    global execSql
    global dayStr
    global days
    # 指定上传开始日期
    global uploadDateStartStr

    if 'o' in globals():
        print('this is online version')

        from odps import options
        # UTC+0
        options.sql.settings = {
            'odps.sql.timezone':'Africa/Accra',
            "odps.sql.submit.mode" : "script"
        }

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

        dayStr = '20240415'
        days = '15'

    # 如果days不是整数，转成整数
    days = int(days)
    uploadDateStartStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=(days - 14))).strftime('%Y%m%d')

def getSKANDataFromMC(dayStr, days):
    dayBeforeStr = (datetime.strptime(dayStr, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')
    # 实时的获取AF翻译结论，而非按照配置文件，所以
    # 注意，当days比较小的时候，可能会出现数据不准确的情况
    sql = f'''
SET
  odps.sql.timezone = Africa / Accra;

set
  odps.sql.hive.compatible = true;

set
  odps.sql.executionengine.enable.rand.time.seed = true;

@skad :=
SELECT
  skad_ad_network_id,
  skad_conversion_value as cv,
  timestamp as postback_timestamp,
  day
FROM
  ods_platform_appsflyer_skad_postbacks_copy
WHERE
  day between '{dayBeforeStr}'
  and '{dayStr}'
  AND app_id = '6448786147';

@media :=
select
  max (media_source) as media,
  skad_ad_network_id
from
  ods_platform_appsflyer_skad_details
where
  day between '{dayBeforeStr}'
  and '{dayStr}'
  and app_id = 'id6448786147'
group by
  skad_ad_network_id;

@ret :=
select
  media.media,
  skad.skad_ad_network_id,
  skad.cv,
  skad.postback_timestamp,
  skad.day
from
  @skad as skad
  left join @media as media on skad.skad_ad_network_id = media.skad_ad_network_id;

select
  COALESCE(ret.media, media2.media) AS media,
  ret.skad_ad_network_id,
  ret.cv,
  ret.postback_timestamp,
  ret.day
from
  @ret as ret
  left join skad_network_id_map as media2 on ret.skad_ad_network_id = media2.skad_ad_network_id
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
            rg_bi.ads_lastwar_ios_purchase_adv
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
            uid as customer_user_id,
            campaign_id,
            TO_CHAR(
            from_unixtime(install_timestamp),
            "yyyymmdd"
            ) as day,
            TO_CHAR(
            from_unixtime(install_timestamp),
            "yyyy-mm-dd hh:mi:ss"
            ) as install_date
        FROM
            rg_bi.dws_overseas_lastwar_unique_uid
        WHERE
            app_package = 'id6448786147'
            AND install_timestamp BETWEEN {minValidInstallTimestamp}
            AND {maxValidInstallTimestamp}
            AND uid IS NOT NULL
            AND mediasource = 'Apple Search Ads'
        GROUP BY
            uid,
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
    return df# 暂时就只关心这3个媒体

import gc
def meanAttributionFastv2(userDf, skanDf):
    userDf['install_timestamp'] = pd.to_numeric(userDf['install_timestamp'], errors='coerce')
    S = 60 * 60
    S2 = 24 * S
    userDf['install_timestamp'] = (userDf['install_timestamp'] // S) * S
    userDf.loc[userDf['cv'] == 0, 'install_timestamp'] = (userDf.loc[userDf['cv'] == 0, 'install_timestamp'] // S2) * S2
    userDf['count'] = 1
    userDf['install_date'] = userDf['install_date'].fillna('')
    userDf = userDf.groupby(['cv', 'install_timestamp','install_date']).agg({'customer_user_id': lambda x: '|'.join(x),'count': 'sum'}).reset_index()

    skanDf['min_valid_install_timestamp'] = (skanDf['min_valid_install_timestamp'] // S) * S
    skanDf['max_valid_install_timestamp'] = (skanDf['max_valid_install_timestamp'] // S) * S
    skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] = (skanDf.loc[skanDf['cv'] == 0, 'min_valid_install_timestamp'] // S2) * S2
    skanDf.loc[skanDf['cv'] == 0, 'max_valid_install_timestamp'] = (skanDf.loc[skanDf['cv'] == 0, 'max_valid_install_timestamp'] // S2) * S2
    
    skanDf['count'] = 1
    skanDf['usd'] = skanDf['usd'].fillna(0)

    skanDf = skanDf.groupby(['media','cv', 'min_valid_install_timestamp', 'max_valid_install_timestamp','usd','day']).agg({'count': 'sum'}).reset_index(drop = False)

    skanDf['usd x count'] = skanDf['usd'] * skanDf['count']

    # print('skanDf:')
    # print(skanDf.head(10))

    pending_skan_indices = skanDf.index.tolist()
    N = 10
    attributeDf = pd.DataFrame(columns=['user index', 'media', 'skan index', 'rate'])

    mediaList = skanDf.loc[~skanDf['media'].isnull()]['media'].unique().tolist()
    

    for media in mediaList:
        userDf['%s rate'%(media)] = 0

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
        
        print('第%d次分配，时间范围向前推%d小时'%(i+1,i*12))
        
        for index, item in tqdm(skanDf_to_process.iterrows(), total=len(skanDf_to_process)):
            media = str(item['media'])
            cv = item['cv']
            min_valid_install_timestamp = item['min_valid_install_timestamp']
            max_valid_install_timestamp = item['max_valid_install_timestamp']
            
            min_valid_install_timestamp -= i*12*3600

            condition_rate = userDf['total media rate'] < 0.95
            condition_time = (userDf['install_timestamp'] >= min_valid_install_timestamp) & (userDf['install_timestamp'] <= max_valid_install_timestamp)
            condition_cv = userDf['cv'] == cv if cv >= 0 else pd.Series([True] * len(userDf))

            if cv < 0:
                    condition = condition_rate & condition_time
            else:
                condition = condition_rate & condition_time & condition_cv
                
            matching_rows = userDf[condition]
            total_matching_count = matching_rows['count'].sum()

            if total_matching_count > 0:
                rate = item['count'] / total_matching_count

                userDf.loc[condition, 'total media rate'] += rate
                user_indices.extend(matching_rows.index)
                medias.extend([media] * len(matching_rows))
                skan_indices.extend([index] * len(matching_rows))
                rates.extend([rate] * len(matching_rows))
                # print(user_indices)
            else:
                new_pending_skan_indices.append(index)

        print('未分配成功：', len(new_pending_skan_indices))
        # # 将未分配成功的skan的前10条打印出来
        # print(skanDf_to_process.loc[new_pending_skan_indices].head(10))

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
        pending_counts = pending_counts.fillna(0)
        # 计算每个媒体的总的skan用户数
        total_counts = skanDf.groupby('media')['count'].sum()
        # 计算每个媒体的未分配占比
        pending_ratios = pending_counts / total_counts
        # 将三个计算结果合并为一个DataFrame
        result_df = pd.concat([total_counts, pending_counts, pending_ratios], axis=1)
        # 设置列名和索引
        result_df.columns = ['总skan用户数', '未分配用户数', '未分配比例']
        result_df.index.name = 'media'
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
    writeSkanToDB(skanFailedDf,'lastwar_ios_rh_skan_raw_failed')

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
            ods_platform_appsflyer_skad_postbacks_copy
        where
            day = '{dayStr}'
            AND app_id = '6448786147'
        limit 10
        ;
    '''
    print(sql)
    df = execSql(sql)
    if len(df) <= 0:
        raise Exception('没有有效的获得skan raw数据，请稍后重试')
    return

# 下面部分就只有线上环境可以用了
from odps.models import Schema, Column, Partition
def createTable():
    if 'o' in globals():
        columns = [
            Column(name='customer_user_id', type='string', comment='from ods_platform_appsflyer_events.customer_user_id'),
            Column(name='install_date', type='string', comment='install date,like 2023-05-31'),
            Column(name='media', type='string', comment='media,like Facebook Ads'),
            Column(name='rate', type='double', comment='rate,lile 0.1'),
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table('lastwar_ios_funplus02_adv_uid_mutidays_media', schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')

# 将处理好的skan存入表中
def createSkanTable(table_name = 'lastwar_ios_rh_skan_raw'):
    if 'o' in globals():
        columns = [
            Column(name='media', type='string', comment='media,like Facebook Ads'),
            Column(name='cv', type='string', comment='conversion value,like 1'),
            Column(name='postback_timestamp', type='bigint', comment='postback time,like 1650000000'),
            Column(name='min_valid_install_timestamp', type='bigint', comment='min valid install time,like 1650000000'),
            Column(name='max_valid_install_timestamp', type='bigint', comment='max valid install time,like 1650000000'),
            Column(name='usd', type='double', comment='usd,like 1.0'),
            Column(name='count', type='bigint', comment='count,like 1'),
        ]
        
        partitions = [
            Partition(name='day', type='string', comment='postback time,like 20221018')
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table = o.create_table(table_name, schema, if_not_exists=True)
        return table
    else:
        print('createTable failed, o is not defined')


def deleteTable(dayStr):
    print('try to delete table:',dayStr)
    if 'o' in globals():
        t = o.get_table('lastwar_ios_funplus02_adv_uid_mutidays_media')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)

def writeTable(df,dayStr):
    print('try to write table:')
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table('lastwar_ios_funplus02_adv_uid_mutidays_media')
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')
        print('try to write csv file')
        df.to_csv('/src/data/zk2/funplus02AdvUidMutiDaysMedia_%s.csv'%(dayStr),index=False)

def writeSkanTable(df1,dayStr,table_name = 'lastwar_ios_rh_skan_raw'):
    df = df1.copy()
    # 格式整理
    df['postback_timestamp'] = df['postback_timestamp'].astype('int64')
    df['min_valid_install_timestamp'] = df['min_valid_install_timestamp'].astype('int64')
    df['max_valid_install_timestamp'] = df['max_valid_install_timestamp'].astype('int64')
    df['usd'] = df['usd'].astype('float64')

    print('try to write table:',table_name,dayStr)
    print(df.head(5))
    if 'o' in globals():
        t = o.get_table(table_name)
        t.delete_partition('day=%s'%(dayStr), if_exists=True)
        with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
            writer.write(df)
    else:
        print('writeTable failed, o is not defined')
        print('try to write csv file')
        df.to_csv(f'/src/data/zk2/{table_name}_%s.csv'%(dayStr),index=False)

def writeSkanToDB(skanDf,tabelName):
    skanDf = skanDf[skanDf['day'] >= uploadDateStartStr].copy()
    days = skanDf['day'].unique().tolist()
    days.sort()
    for dayStr in days:
        dayDf = skanDf[skanDf['day'] == dayStr]
        writeSkanTable(dayDf,dayStr,tabelName)

def main():
    print('dayStr:', dayStr)
    print('days:', days)
    print('写入开始日期:',uploadDateStartStr)
    check(dayStr)
    # 1、获取skan数据
    skanDf = getSKANDataFromMC(dayStr,days)
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
    
    # 5、获取af数据
    afDf = getAfDataFromMC(minValidInstallTimestamp, maxValidInstallTimestamp)
    userDf = addCv(afDf,getCvMap())

    cvMap = getCvMap()[['conversion_value','min_event_revenue','max_event_revenue']].fillna(0)
    cvMap['avg_event_revenue'] = (cvMap['min_event_revenue'] + cvMap['max_event_revenue']) / 2
    cvMap.rename(columns={'conversion_value': 'cv','avg_event_revenue':'usd'}, inplace=True)
    cvMap = cvMap[['cv','usd']]
    
    skanDf = skanDf.merge(cvMap,on='cv',how='left')

    # userDf.to_csv('/src/data/zk/userDf3.csv',index=False)
    # skanDf.to_csv('/src/data/zk/skanDf3.csv',index=False)

    # userDf = pd.read_csv('/src/data/zk/userDf3.csv',dtype={'customer_user_id':str})
    # skanDf = pd.read_csv('/src/data/zk/skanDf3.csv',dtype={'day':str})

    # 将skanDf2存档
    skanDf['count'] = 1
    writeSkanToDB(skanDf,'lastwar_ios_rh_skan_raw')

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
            var_name='media',
            value_name='rate'
        )
        # print('melt之后：')
        # attDf_melted.info(memory_usage='deep')
        # 改用这种比较简单的方式，更加省内存
        attDf_melted['media'] = attDf_melted['media'].str[:-5]

        dayDf = attDf_melted.loc[attDf_melted['rate'] > 0]

        # 追加ASA数据
        asaDayDf = asaDf[asaDf['day'] == dayStr0].copy()
        asaDayDf['rate'] = 1
        asaDayDf['media'] = 'Apple Search Ads'
        asaDayDf = asaDayDf[['customer_user_id', 'install_date', 'media', 'rate']]
        print('追加ASA数据：')
        print(asaDayDf.head(5))

        dayDf = dayDf.append(asaDayDf, ignore_index=True)
        
        # 写入表
        writeTable(dayDf,dayStr0)

        # 释放内存
        del dayDf

    return

init()
createTable()
createSkanTable('lastwar_ios_rh_skan_raw')
createSkanTable('lastwar_ios_rh_skan_raw_failed')

main()

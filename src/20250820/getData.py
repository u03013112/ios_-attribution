# 数据获得，暂定获取20250101至今数据
# 暂时只获取安卓GPIR数据


import os
import datetime
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.dataBricks import execSql, execSql2


def createAosGpirUidRevenueView():
    sql = """
CREATE OR REPLACE VIEW lw_20250820_aos_gpir_uid_revenue_view_by_j AS
select
	uid,
	install_day,
	country,
	country_group,
	mediasource,
	campaign_id,
	revenue_24h as revenue_d1,
	revenue_72h as revenue_d3,
	revenue_168h as revenue_d7
from
	(
		select
			t1.uid,
			t1.install_timestamp,
			date_format(from_unixtime(t1.install_timestamp), 'yyyyMMdd') as install_day,
			t1.country,
			COALESCE(cg.country_group, 'other') AS country_group,
			t1.mediasource,
			t1.campaign_id,
			sum(t2.revenue_value_usd) as total_revenue,
			sum(
				case
					when (t2.event_time / 1000 - t1.install_timestamp) between 0
					and 24 * 60 * 60 then t2.revenue_value_usd
					else 0
				end
			) as revenue_24h,
			sum(
				case
					when (t2.event_time / 1000 - t1.install_timestamp) between 0
					and 72 * 60 * 60 then t2.revenue_value_usd
					else 0
				end
			) as revenue_72h,
			sum(
				case
					when (t2.event_time / 1000 - t1.install_timestamp) between 0
					and 168 * 60 * 60 then t2.revenue_value_usd
					else 0
				end
			) as revenue_168h
		from
			marketing.attribution.dws_overseas_gpir_unique_uid t1
			left join marketing.attribution.dwd_overseas_revenue_allproject t2 on t1.app = t2.app
			and t1.uid = t2.uid
			LEFT JOIN lw_country_group_table_by_j_20250703 cg ON t1.country = cg.country
		where
			t1.app = 502
			and t1.app_package = 'com.fun.lastwar.gp'
		group by
			t1.uid,
			t1.install_timestamp,
			t1.country,
			COALESCE(cg.country_group, 'other'),
			t1.mediasource,
			t1.campaign_id
	)
where
	total_revenue > 0
order by
	total_revenue desc;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return


# 为了后续给用户收入分档，将用户的收入进行汇总
# 从startDay~endDay的时间段内，按照3日收入金额进行分组，添加一列pay_users记录相同付费金额用户数
def getAosGpir3dRevenueGroupData(startDay='20250101', endDay='20250810'):
    filename = f'/src/data/20250820_aosGpir3dRevenueGroupData_{startDay}_{endDay}.csv'
    if os.path.exists(filename):
        print(f"File {filename} already exists, loading from file.")
        df = pd.read_csv(filename)
    else:
        sql = f"""
SELECT
    ROUND(revenue_d3, 1) as revenue_d3,
    count(uid) as pay_users
FROM
    lw_20250820_aos_gpir_uid_revenue_view_by_j
WHERE
    install_day BETWEEN '{startDay}' AND '{endDay}'
GROUP BY
    ROUND(revenue_d3, 1)
;
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

    df = df.sort_values(by='revenue_d3', ascending=False).reset_index(drop=True)
    return df

# 对3日付费用户数据进行分组，获取分组的分界金额，方便后续对用户数据的汇总。
def makeLevels(userDf, N=8):
    userDf = userDf.copy()

    userDf['sumUsd'] = userDf['revenue_d3'] * userDf['pay_users']
    
    filtered_df = userDf[(userDf['revenue_d3'] > 0) & (userDf['sumUsd'] > 0)]

    df = filtered_df.sort_values(['revenue_d3'])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df['sumUsd'].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N)
    df['sum'] = df['sumUsd'].cumsum()
    
    for i in range(1,N):
        target = target_usd*(i)
        # 找到第一个大于target的行
        rows = df[df['sum']>=target]
        if len(rows) > 0:
            row = rows.iloc[0]
            levels[i-1] = row['revenue_d3']

    # levels 排重
    levels = list(set(levels))
    # levels 中如果有0，去掉0
    if 0 in levels:
        levels.remove(0)
    # levels 排序
    levels.sort()

    return levels


# 按照制定levels分组，并汇总收入数据
# 其他维度不变，将逐个uid的收入汇总，
# 最终列：
# app_package, install_day, country_group, mediasource, campaign_id,
# revenue_d3_min, revenue_d3_max, users_count, total_revenue
def getAosGpirData3dGroup(levels,startDay = '20250101',endDay = '20250810'):
    filename = f'/src/data/20250820_aosGpirData_{startDay}_{endDay}.csv'
    if os.path.exists(filename):
        print(f"File {filename} already exists, loading from file.")
        df = pd.read_csv(filename)
    else:
        # 将levels拆开，左开右闭
        # 比如我的levels = [4.1, 18.6, 43.9, 83.9, 155.9, 337.8, 736.9]
        # 那么levels_min = [0, 4.1, 18.6, 43.9, 83.9, 155.9, 337.8, 736.9]
        # levels_max = [4.1, 18.6, 43.9, 83.9, 155.9, 337.8, 736.9, 9999999.9]
        
        levels_min = [0] + levels
        levels_max = levels + [9999999.9]
        
        # 构建CASE WHEN语句来分组
        case_when_min = []
        case_when_max = []
        
        for i in range(len(levels_min)):
            min_val = levels_min[i]
            max_val = levels_max[i]
            case_when_min.append(f"WHEN revenue_d3 > {min_val} AND revenue_d3 <= {max_val} THEN {min_val}")
            case_when_max.append(f"WHEN revenue_d3 > {min_val} AND revenue_d3 <= {max_val} THEN {max_val}")
        
        case_when_min_str = "\n            ".join(case_when_min)
        case_when_max_str = "\n            ".join(case_when_max)

        sql = f"""
SELECT
    'com.fun.lastwar.gp' as app_package,
    install_day,
    country_group,
    mediasource,
    campaign_id,
    CASE
        {case_when_min_str}
        ELSE 0
    END as revenue_d3_min,
    CASE
        {case_when_max_str}
        ELSE 9999999.9
    END as revenue_d3_max,
    COUNT(uid) as users_count,
    SUM(revenue_d1) as total_revenue_d1,
    SUM(revenue_d3) as total_revenue_d3,
    SUM(revenue_d7) as total_revenue_d7
FROM
    lw_20250820_aos_gpir_uid_revenue_view_by_j
WHERE
    install_day BETWEEN '{startDay}' AND '{endDay}'
    AND revenue_d3 > 0
GROUP BY
    install_day,
    country_group,
    mediasource,
    campaign_id,
    CASE
        {case_when_min_str}
        ELSE 0
    END,
    CASE
        {case_when_max_str}
        ELSE 9999999.9
    END
ORDER BY
    install_day,
    country_group,
    mediasource,
    campaign_id,
    revenue_d3_min
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    return df


# 合并到分国家，为了对总数，简单确认数据正确
def forDebug(df):
    retDf = df.copy()
    retDf = retDf.groupby(['app_package', 'install_day', 'country_group']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    return retDf

# 所有分档合并
# 返回3组值
# 第一组分国家
# 第二组，分国家+分媒体
# 第三组，分媒体+分campaign
def getRawData(df = None):
    if df is None:
        # 如果没有传入df，则从文件中读取
        filename = '/src/data/20250820_getData_df2.csv'
        if os.path.exists(filename):
            print(f"File {filename} already exists, loading from file.")
            df = pd.read_csv(filename, dtype={
                'install_day': str,
            })
        else:
            raise FileNotFoundError(f"File {filename} does not exist. Please run getAosGpirData3dGroup first.")
    df0 = df.copy()
    df0 = df0.groupby(['app_package', 'install_day', 'country_group']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    
    # 媒体太多了，做一个基础过滤，累计付费人数少于100的媒体不要
    mediaDf = df.groupby(['mediasource']).agg({
        'users_count': 'sum'
    }).reset_index()
    mediaDf = mediaDf[mediaDf['users_count'] >= 500]
    medias = mediaDf['mediasource'].unique()    
    df = df[df['mediasource'].isin(medias)]

    df1 = df.copy()
    df1 = df1.groupby(['app_package', 'install_day', 'country_group', 'mediasource']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()

    df2 = df.copy()
    df2 = df2.groupby(['app_package', 'install_day', 'mediasource', 'campaign_id']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    # 将campaign_id 行数少于 28 的行删除
    # 统计每个campaign_id的行数
    campaign_counts = df2.groupby(['mediasource', 'campaign_id']).size().reset_index(name='row_count')
    # 筛选出行数大于等于28的campaign
    valid_campaigns = campaign_counts[campaign_counts['row_count'] >= 28][['mediasource', 'campaign_id']]
    # 将df2与valid_campaigns进行内连接，保留行数足够的campaign
    df2 = df2.merge(valid_campaigns, on=['mediasource', 'campaign_id'], how='inner')


    return df0, df1, df2

# 按照分档数据进行分组，比rawData多了一个分档
def getGroupData(df = None):
    if df is None:
        # 如果没有传入df，则从文件中读取
        filename = '/src/data/20250820_getData_df2.csv'
        if os.path.exists(filename):
            print(f"File {filename} already exists, loading from file.")
            df = pd.read_csv(filename)
        else:
            raise FileNotFoundError(f"File {filename} does not exist. Please run getAosGpirData3dGroup first.")
    df0 = df.copy()
    df0 = df0.groupby(['app_package', 'install_day', 'country_group', 'revenue_d3_min', 'revenue_d3_max']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()

    df1 = df.copy()
    df1 = df1.groupby(['app_package', 'install_day', 'country_group', 'mediasource', 'revenue_d3_min', 'revenue_d3_max']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    
    df2 = df.copy()
    df2 = df2.groupby(['app_package', 'install_day', 'mediasource', 'campaign_id', 'revenue_d3_min', 'revenue_d3_max']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    return df0, df1, df2


# 削弱大R的影响，获取NerfR数据
# 目前的levels中最大档位为2000.0
# 将revenue_d3_min = 2000.0 的 数据去掉
def getNerfRData(df = None):
    if df is None:
        # 如果没有传入df，则从文件中读取
        filename = '/src/data/20250820_getData_df2.csv'
        if os.path.exists(filename):
            print(f"File {filename} already exists, loading from file.")
            df = pd.read_csv(filename)
        else:
            raise FileNotFoundError(f"File {filename} does not exist. Please run getAosGpirData3dGroup first.")
        
    # # for debug
    # tmpDf = df.copy()
    # tmpDf = tmpDf[tmpDf['revenue_d3_min'] >= 2000.0]
    # tmpDf = tmpDf.groupby(['app_package']).agg({
    #     'users_count': 'sum',
    #     'total_revenue_d1': 'sum',
    #     'total_revenue_d3': 'sum',
    #     'total_revenue_d7': 'sum'
    # }).reset_index()
    # tmpDf['avg_revenue_d1'] = tmpDf['total_revenue_d1'] / tmpDf['users_count']
    # tmpDf['avg_revenue_d3'] = tmpDf['total_revenue_d3'] / tmpDf['users_count']
    # tmpDf['avg_revenue_d7'] = tmpDf['total_revenue_d7'] / tmpDf['users_count']
    # print("NerfR DataFrame:")
    # print(tmpDf.head())

    # 初步得到结论，超过2000的大R，平均1日收入1300,3日收入3400,7日收入6300
    # 所以进行一定削弱，将1日统一削弱至500，3日削弱至2000，7日削弱至4000
    # 即将 revenue_d3_min = 2000.0 的total_revenue_d1 = 500* users_count,
    # total_revenue_d3 = 2000 * users_count, total_revenue_d7 = 4000 * users_count
    # 其他行不变，同样是计算df0，df1，df2
    
    # 复制原始数据
    nerfDf = df.copy()
    
    # 对大R用户进行削弱处理
    # 找到 revenue_d3_min >= 2000.0 的行，调整其收入
    mask = nerfDf['revenue_d3_min'] >= 2000.0
    nerfDf.loc[mask, 'total_revenue_d1'] = nerfDf.loc[mask, 'users_count'] * 500
    nerfDf.loc[mask, 'total_revenue_d3'] = nerfDf.loc[mask, 'users_count'] * 2000
    nerfDf.loc[mask, 'total_revenue_d7'] = nerfDf.loc[mask, 'users_count'] * 4000
    
    # 按照分国家分组
    df0 = nerfDf.copy()
    df0 = df0.groupby(['app_package', 'install_day', 'country_group']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    
    # 按照分国家+分媒体分组
    df1 = nerfDf.copy()
    df1 = df1.groupby(['app_package', 'install_day', 'country_group', 'mediasource']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()

    # 按照分媒体+分campaign分组
    df2 = nerfDf.copy()
    df2 = df2.groupby(['app_package', 'install_day', 'mediasource', 'campaign_id']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()

    return df0, df1, df2


# 分档位+ nerfR 数据
# getGroupData和getNerfRData的结合
# 先将数据进行削弱大R操作，然后分档
def getNerfRGroupData(df = None):
    if df is None:
        # 如果没有传入df，则从文件中读取
        filename = '/src/data/20250820_getData_df2.csv'
        if os.path.exists(filename):
            print(f"File {filename} already exists, loading from file.")
            df = pd.read_csv(filename)
        else:
            raise FileNotFoundError(f"File {filename} does not exist. Please run getAosGpirData3dGroup first.")
    # 复制原始数据
    nerfDf = df.copy()
    
    # 对大R用户进行削弱处理
    # 找到 revenue_d3_min >= 2000.0 的行，调整其收入
    mask = nerfDf['revenue_d3_min'] >= 2000.0
    nerfDf.loc[mask, 'total_revenue_d1'] = nerfDf.loc[mask, 'users_count'] * 500
    nerfDf.loc[mask, 'total_revenue_d3'] = nerfDf.loc[mask, 'users_count'] * 2000
    nerfDf.loc[mask, 'total_revenue_d7'] = nerfDf.loc[mask, 'users_count'] * 4000
    
    # 按照分档数据进行分组，保持分档信息
    df0 = nerfDf.copy()
    df0 = df0.groupby(['app_package', 'install_day', 'country_group', 'revenue_d3_min', 'revenue_d3_max']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()

    df1 = nerfDf.copy()
    df1 = df1.groupby(['app_package', 'install_day', 'country_group', 'mediasource', 'revenue_d3_min', 'revenue_d3_max']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    
    df2 = nerfDf.copy()
    df2 = df2.groupby(['app_package', 'install_day', 'mediasource', 'campaign_id', 'revenue_d3_min', 'revenue_d3_max']).agg({
        'users_count': 'sum',
        'total_revenue_d1': 'sum',
        'total_revenue_d3': 'sum',
        'total_revenue_d7': 'sum'
    }).reset_index()
    
    return df0, df1, df2

def main():
    # # 创建视图
    # createAosGpirUidRevenueView()

    # 获取3日收入分组数据
    startDay = '20250101'
    endDay = '20250810'
    df = getAosGpir3dRevenueGroupData(startDay, endDay)
    # print(df.head())

    levels = makeLevels(df, N=8)
    # 为了可以削弱大R，额外添加一个较大的分界金额
    levels.append(2000.0)
    print("分组的分界金额：", levels)

    # 获取其他数据
    df2 = getAosGpirData3dGroup(levels,startDay, endDay)
    print(df2.head())
    df2.to_csv(f'/src/data/20250820_getData_df2.csv', index=False)

    # debugDf = forDebug(df2)
    # print("Debug DataFrame:")
    # print(debugDf[debugDf['install_day'] == 20250801])
    # print(debugDf[debugDf['install_day'] == 20250801]['total_revenue_d1'].sum())
    # print(debugDf[debugDf['install_day'] == 20250801]['total_revenue_d3'].sum())
    # print(debugDf[debugDf['install_day'] == 20250801]['total_revenue_d7'].sum())

    # 获取原始数据
    rawDf0, rawDf1, rawDf2 = getRawData(df2)
    print("Raw DataFrame 0:")
    print(rawDf0.head())
    print("Raw DataFrame 1:")
    print(rawDf1.head())
    print("Raw DataFrame 2:")
    print(rawDf2.head())

    # 获取分档数据
    groupDf0, groupDf1, groupDf2 = getGroupData(df2)
    print("Grouped DataFrame 0:")
    print(groupDf0.head())
    print("Grouped DataFrame 1:")
    print(groupDf1.head())
    print("Grouped DataFrame 2:")
    print(groupDf2.head())

    # 获取NerfR数据
    nerfDf0, nerfDf1, nerfDf2 = getNerfRData(df2)
    print("NerfR DataFrame 0 (按国家分组):")
    print(nerfDf0.head())
    print("NerfR DataFrame 1 (按国家+媒体分组):")
    print(nerfDf1.head())
    print("NerfR DataFrame 2 (按媒体+campaign分组):")
    print(nerfDf2.head())

    # 获取NerfR分档数据
    nerfGroupDf0, nerfGroupDf1, nerfGroupDf2 = getNerfRGroupData(df2)
    print("NerfR Group DataFrame 0 (按国家+分档分组):")
    print(nerfGroupDf0.head())
    print("NerfR Group DataFrame 1 (按国家+媒体+分档分组):")
    print(nerfGroupDf1.head())
    print("NerfR Group DataFrame 2 (按媒体+campaign+分档分组):")
    print(nerfGroupDf2.head())


if __name__ == '__main__':
    main()

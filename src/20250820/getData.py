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


        sql = f"""
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    return df


def main():
    # # 创建视图
    # createAosGpirUidRevenueView()

    # 获取3日收入分组数据
    startDay = '20250101'
    endDay = '20250810'
    df = getAosGpir3dRevenueGroupData(startDay, endDay)
    print(df.head())

    levels = makeLevels(df, N=8)
    print("分组的分界金额：", levels)

    # 获取其他数据
    # df2 = getAosGpirData3dGroup(levels,startDay, endDay)
    # print(df2.head())


if __name__ == '__main__':
    main()
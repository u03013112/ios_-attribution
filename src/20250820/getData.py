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
    return df

# 对3日付费用户数据进行分组，获取分组的分界金额，方便后续对用户数据的汇总。



def getAosGpirData(startDay = '20250101',endDay = '20250810'):
    filename = f'/src/data/20250820_aosGpirData_{startDay}_{endDay}.csv'
    if os.path.exists(filename):
        print(f"File {filename} already exists, loading from file.")
        df = pd.read_csv(filename)
    else:
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

    # 获取其他数据
    # df2 = getAosGpirData(startDay, endDay)
    # print(df2.head())


if __name__ == '__main__':
    main()
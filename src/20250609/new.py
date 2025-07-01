# https://qianxian.feishu.cn/docx/Uqk7do0lGodS0yxXGoKc6OMrnbh
# 基于上述文档进行开发
import os
import datetime
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

def createRealDailyView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_real_cost_roi_country_group_ad_type_view_by_j AS
select
	install_day,
	CASE
		WHEN country IN (
			'AD',
			'AT',
			'AU',
			'BE',
			'CA',
			'CH',
			'DE',
			'DK',
			'FI',
			'FR',
			'HK',
			'IE',
			'IS',
			'IT',
			'LI',
			'LU',
			'MC',
			'NL',
			'NO',
			'NZ',
			'SE',
			'SG',
			'UK',
			'MO',
			'IL',
			'TW'
		) THEN 'T1'
		WHEN country = 'US' THEN 'US'
		WHEN country = 'JP' THEN 'JP'
		WHEN country = 'KR' THEN 'KR'
		WHEN country IN ('SA', 'AE', 'QA', 'KW', 'BH', 'OM') THEN 'GCC'
		ELSE 'other'
	END AS country_group,
	mediasource,
	CASE
		WHEN mediasource = 'Facebook Ads' THEN CASE
			WHEN campaign_name LIKE '%BAU%' THEN 'BAU'
			WHEN campaign_name LIKE '%AAA%'
			OR campaign_name LIKE '%3A%' THEN 'AAA'
			ELSE 'other'
		END
		WHEN mediasource = 'googleadwords_int' THEN CASE
			WHEN campaign_name LIKE '%3.0%' THEN '3.0'
			WHEN campaign_name LIKE '%2.5%' THEN '2.5'
			ELSE 'other'
		END
		ELSE 'other'
	END AS ad_type,
	sum(cost_value_usd) as cost,
	sum(revenue_d1) as revenue_d1,
	sum(revenue_d3) as revenue_d3,
	sum(revenue_d7) as revenue_d7,
	sum(revenue_d30) as revenue_d30,
	sum(revenue_d60) as revenue_d60,
	sum(revenue_d90) as revenue_d90,
	sum(revenue_d120) as revenue_d120
from
	dws_overseas_public_roi
where
	app = '502'
	and app_package = 'com.fun.lastwar.gp'
	and facebook_segment in ('country', ' ')
group by
	install_day,
	country_group,
	mediasource,
	ad_type;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createRealMonthyView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_real_cost_roi_country_group_ad_type_month_view_by_j AS
select
	SUBSTR(install_day, 1, 6) AS install_month,
	CASE
		WHEN country IN (
			'AD',
			'AT',
			'AU',
			'BE',
			'CA',
			'CH',
			'DE',
			'DK',
			'FI',
			'FR',
			'HK',
			'IE',
			'IS',
			'IT',
			'LI',
			'LU',
			'MC',
			'NL',
			'NO',
			'NZ',
			'SE',
			'SG',
			'UK',
			'MO',
			'IL',
			'TW'
		) THEN 'T1'
		WHEN country = 'US' THEN 'US'
		WHEN country = 'JP' THEN 'JP'
		WHEN country = 'KR' THEN 'KR'
		WHEN country IN ('SA', 'AE', 'QA', 'KW', 'BH', 'OM') THEN 'GCC'
		ELSE 'other'
	END AS country_group,
	mediasource,
	CASE
		WHEN mediasource = 'Facebook Ads' THEN CASE
			WHEN campaign_name LIKE '%BAU%' THEN 'BAU'
			WHEN campaign_name LIKE '%AAA%'
			OR campaign_name LIKE '%3A%' THEN 'AAA'
			ELSE 'other'
		END
		WHEN mediasource = 'googleadwords_int' THEN CASE
			WHEN campaign_name LIKE '%3.0%' THEN '3.0'
			WHEN campaign_name LIKE '%2.5%' THEN '2.5'
			ELSE 'other'
		END
		ELSE 'other'
	END AS ad_type,
	sum(cost_value_usd) as cost,
	sum(revenue_d1) as revenue_d1,
	sum(revenue_d3) as revenue_d3,
	sum(revenue_d7) as revenue_d7,
	sum(revenue_d30) as revenue_d30,
	sum(revenue_d60) as revenue_d60,
	sum(revenue_d90) as revenue_d90,
	sum(revenue_d120) as revenue_d120
from
	dws_overseas_public_roi
where
	app = '502'
	and app_package = 'com.fun.lastwar.gp'
	and facebook_segment in ('country', ' ')
group by
	install_month,
	country_group,
	mediasource,
	ad_type;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 收入增长率
def createRevenueRiseRatioView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_revenue_rise_ratio_country_group_ad_type_month_view_by_j AS
WITH ratios AS (
	SELECT
		install_month,
		country_group,
		mediasource,
		ad_type,
		CASE
			WHEN revenue_d1 = 0 THEN 0
			ELSE revenue_d3 / revenue_d1
		END AS r3_r1,
		CASE
			WHEN revenue_d3 = 0 THEN 0
			ELSE revenue_d7 / revenue_d3
		END AS r7_r3,
		CASE
			WHEN revenue_d7 = 0 THEN 0
			ELSE revenue_d30 / revenue_d7
		END AS r30_r7,
		CASE
			WHEN revenue_d30 = 0 THEN 0
			ELSE revenue_d60 / revenue_d30
		END AS r60_r30,
		CASE
			WHEN revenue_d60 = 0 THEN 0
			ELSE revenue_d90 / revenue_d60
		END AS r90_r60,
		CASE
			WHEN revenue_d90 = 0 THEN 0
			ELSE revenue_d120 / revenue_d90
		END AS r120_r90
	FROM
		lw_real_cost_roi_country_group_ad_type_month_view_by_j
),
ratios_with_rownum AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY country_group,
			mediasource,
			ad_type
			ORDER BY
				install_month
		) AS row_num
	FROM
		ratios
),
predict_ratios AS (
	SELECT
		cur.install_month,
		cur.country_group,
		cur.mediasource,
		cur.ad_type,
		cur.r3_r1,
		cur.r7_r3,
		cur.r30_r7,
		cur.r60_r30,
		cur.r90_r60,
		cur.r120_r90,
		-- 计算平均值时排除0值
		COALESCE(
			AVG(
				CASE
					WHEN prev.r3_r1 <> 0 THEN prev.r3_r1
				END
			),
			0
		) AS predict_r3_r1,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r7_r3 <> 0 THEN prev.r7_r3
				END
			),
			0
		) AS predict_r7_r3,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r30_r7 <> 0 THEN prev.r30_r7
				END
			),
			0
		) AS predict_r30_r7,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r60_r30 <> 0 THEN prev.r60_r30
				END
			),
			0
		) AS predict_r60_r30,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r90_r60 <> 0 THEN prev.r90_r60
				END
			),
			0
		) AS predict_r90_r60,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r120_r90 <> 0 THEN prev.r120_r90
				END
			),
			0
		) AS predict_r120_r90
	FROM
		ratios_with_rownum cur
		LEFT JOIN ratios_with_rownum prev ON cur.country_group = prev.country_group
		AND cur.mediasource = prev.mediasource
		AND cur.ad_type = prev.ad_type
		AND prev.row_num BETWEEN cur.row_num - 3
		AND cur.row_num - 1
	GROUP BY
		cur.install_month,
		cur.country_group,
		cur.mediasource,
		cur.ad_type,
		cur.r3_r1,
		cur.r7_r3,
		cur.r30_r7,
		cur.r60_r30,
		cur.r90_r60,
		cur.r120_r90
)
SELECT
	install_month,
	country_group,
	mediasource,
	ad_type,
	r3_r1,
	r7_r3,
	r30_r7,
	r60_r30,
	r90_r60,
	r120_r90,
	predict_r3_r1,
	predict_r7_r3,
	predict_r30_r7,
	predict_r60_r30,
	predict_r90_r60,
	predict_r120_r90
FROM
	predict_ratios
ORDER BY
	country_group,
	mediasource,
	ad_type,
	install_month
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def main():
    # # 创建每月数据视图
    # createRealMonthyView()

    # 创建收入增长率视图
    createRevenueRiseRatioView()



if __name__ == "__main__":
    main()
# KPI 观察
# 按照里程碑，将里程碑制定期间的付费增长率和里程碑期间的付费增长率进行对比

import os
import datetime
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.dataBricks import execSql, execSql2

def createMilestonePredRevenueGrowthrateView():
	sql = """
CREATE OR REPLACE VIEW lw_20250822_milestone_pred_revenue_growthrate_view_by_j AS
WITH milestone AS (
	SELECT
		startday,
		COALESCE(endday, date_format(date_sub(current_date(), 8), 'yyyyMMdd')) AS endday,
		substr(startday, 1, 6) AS startmonth
	FROM marketing.attribution.cdm_ext_milestone_config
	WHERE
		app = 502
	GROUP BY
		startday,
		COALESCE(endday, date_format(date_sub(current_date(), 8), 'yyyyMMdd'))
)
SELECT
	t.app_package,
	t.country_group,
	t.mediasource,
	m.startday,
	t.predict_r3_r1,
	t.predict_r7_r3,
	t.predict_r14_r7,
	t.predict_r30_r14,
	t.predict_r60_r30,
	t.predict_r90_r60,
	t.predict_r120_r90,
	t.predict_r150_r120
FROM milestone m
JOIN `data_science`.`default`.`lw_20250703_af_revenue_rise_ratio_predict2_month_view_by_j` t
	ON m.startmonth = t.install_month
WHERE
		t.tag = 'onlyprofit_forpayback_cohort'
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# Gpir
def createAosOnlyprofitCohortCostRevenueDailyForCheckView():
	sql = """
CREATE OR REPLACE VIEW lw_20250822_aos_onlyprofit_cohort_cost_revenue_day_forpaycheck_view_by_j AS
SELECT
	case
		when roi.app_package in ('com.fun.lastwar.gp','com.fun.lastwar.vn.gp') then 'com.fun.lastwar.gp'
	end AS app_package,
	roi.install_day,
	COALESCE(cg.country_group, 'other') AS country_group,
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		WHEN mediasource IN ('googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'moloco_int') THEN mediasource
		ELSE 'other'
	END as mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_h24) AS revenue_d1,
	SUM(revenue_h72) AS revenue_d3,
	SUM(revenue_h168) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_gpir_roi_profit roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	LEFT JOIN (
		SELECT campaign_id, MAX(campaign_name) AS campaign_name
		FROM prodb.public.applovin_campaign_info_new
		GROUP BY campaign_id
	) pub ON 
    roi.campaign_id = pub.campaign_id
WHERE
	roi.app_package in ('com.fun.lastwar.gp','com.fun.lastwar.vn.gp')
GROUP BY
	case
		when roi.app_package in ('com.fun.lastwar.gp','com.fun.lastwar.vn.gp') then 'com.fun.lastwar.gp'
	end,
	roi.install_day,
	COALESCE(cg.country_group, 'other'),
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		WHEN mediasource IN ('googleadwords_int', 'Facebook Ads', 'bytedanceglobal_int', 'snapchat_int', 'moloco_int') THEN mediasource
		ELSE 'other'
	END,
	ad_type
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createIosOnlyprofitCohortCostRevenueDailyForCheckView():
	sql = """
CREATE OR REPLACE VIEW lw_20250822_ios_onlyprofit_cohort_cost_revenue_day_forcheck_view_by_j AS
SELECT
	case
		when roi.app_package in ('id6448786147', 'id6736925794') then 'id6448786147'
	end AS app_package,
	roi.install_day,
	COALESCE(cg.country_group, 'other') AS country_group,
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		WHEN mediasource = 'tiktokglobal_int' THEN 'bytedanceglobal_int'
		WHEN mediasource IN ('Facebook Ads', 'bytedanceglobal_int','moloco_int','snapchat_int','twitter') THEN mediasource
		ELSE 'other'
	END as mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_h24) AS revenue_d1,
	SUM(revenue_h72) AS revenue_d3,
	SUM(revenue_h168) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_roi_profit roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	LEFT JOIN (
		SELECT campaign_id, MAX(campaign_name) AS campaign_name
		FROM prodb.public.applovin_campaign_info_new
		GROUP BY campaign_id
	) pub ON 
    roi.campaign_id = pub.campaign_id
WHERE
	roi.app_package in ('id6448786147', 'id6736925794')
GROUP BY
	case
		when roi.app_package in ('id6448786147', 'id6736925794') then 'id6448786147'
	end,
	roi.install_day,
	COALESCE(cg.country_group, 'other'),
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(pub.campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		WHEN mediasource = 'tiktokglobal_int' THEN 'bytedanceglobal_int'
		WHEN mediasource IN ('Facebook Ads', 'bytedanceglobal_int','moloco_int','snapchat_int','twitter') THEN mediasource
		ELSE 'other'
	END,
	ad_type
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 将上述两个视图合并为一个视图
def createOnlyprofitCohortCostRevenueDailyForCheckView():
	sql = """
CREATE OR REPLACE VIEW lw_20250822_onlyprofit_cohort_cost_revenue_day_forcheck_view_by_j AS
SELECT * FROM lw_20250822_aos_onlyprofit_cohort_cost_revenue_day_forpaycheck_view_by_j
UNION ALL
SELECT * FROM lw_20250822_ios_onlyprofit_cohort_cost_revenue_day_forcheck_view_by_j
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


def createOnlyprofitCohortCostRevenueWeeklyForCheckView():
	# 将 @`data_science`.`default`.`lw_20250822_onlyprofit_cohort_cost_revenue_day_forcheck_view_by_j` 按周聚合，
	# 要求添加两列 monday,sunday,其中monday作为周的开始，sunday作为周的结束，均使用格式 'yyyyMMdd'。
	# 按照app_package，country_group，mediasource，ad_type，monday，sunday进行分组，分组内所有数据SUM
	# 只要满日数据，不满日的收入 为NULL
	sql = """
CREATE OR REPLACE VIEW lw_20250822_onlyprofit_cohort_cost_revenue_weekly_forcheck_view_by_j AS
with calculated_dates as (
	select
		app_package,
		country_group,
		mediasource,
		ad_type,
		date_format(
			date_trunc('week', to_date(install_day, 'yyyyMMdd')),
			'yyyyMMdd'
		) as monday,
		date_format(
			date_add(
				date_trunc('week', to_date(install_day, 'yyyyMMdd')),
				6
			),
			'yyyyMMdd'
		) as sunday,
		datediff(
			current_date(),
			date_add(
				date_trunc('week', to_date(install_day, 'yyyyMMdd')),
				6
			)
		) as days_since_sunday,
		cost,
		revenue_d1,
		revenue_d3,
		revenue_d7,
		revenue_d14,
		revenue_d30,
		revenue_d60,
		revenue_d90,
		revenue_d120,
		revenue_d150
	from
		`data_science`.`default`.`lw_20250822_onlyprofit_cohort_cost_revenue_day_forcheck_view_by_j`
)
select
	app_package,
	country_group,
	mediasource,
	ad_type,
	monday,
	sunday,
	sum(cost) as total_cost,
	case
		when any_value(days_since_sunday) >= 1 then sum(revenue_d1)
		else NULL
	end as total_revenue_d1,
	case
		when any_value(days_since_sunday) >= 3 then sum(revenue_d3)
		else NULL
	end as total_revenue_d3,
	case
		when any_value(days_since_sunday) >= 7 then sum(revenue_d7)
		else NULL
	end as total_revenue_d7,
	case
		when any_value(days_since_sunday) >= 14 then sum(revenue_d14)
		else NULL
	end as total_revenue_d14,
	case
		when any_value(days_since_sunday) >= 30 then sum(revenue_d30)
		else NULL
	end as total_revenue_d30,
	case
		when any_value(days_since_sunday) >= 60 then sum(revenue_d60)
		else NULL
	end as total_revenue_d60,
	case
		when any_value(days_since_sunday) >= 90 then sum(revenue_d90)
		else NULL
	end as total_revenue_d90,
	case
		when any_value(days_since_sunday) >= 120 then sum(revenue_d120)
		else NULL
	end as total_revenue_d120,
	case
		when any_value(days_since_sunday) >= 150 then sum(revenue_d150)
		else NULL
	end as total_revenue_d150
from
	calculated_dates
group by
	app_package,
	country_group,
	mediasource,
	ad_type,
	monday,
	sunday
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createMilestoneRealRevenueGrowthrateView():
	sql = """
CREATE OR REPLACE VIEW lw_20250822_milestone_real_revenue_growthrate_view_by_j AS
WITH milestone AS (
	SELECT
		startday,
		COALESCE(endday, date_format(date_sub(current_date(), 8), 'yyyyMMdd')) AS endday
	FROM marketing.attribution.cdm_ext_milestone_config
	WHERE
		app = 502
	GROUP BY
		startday,
		COALESCE(endday, date_format(date_sub(current_date(), 8), 'yyyyMMdd'))
)
SELECT
	m.startday,
	m.endday,
	j.*,
	try_divide(j.total_revenue_d3, j.total_revenue_d1) as r3_r1,
	try_divide(j.total_revenue_d7, j.total_revenue_d3) as r7_r3,
	try_divide(j.total_revenue_d14, j.total_revenue_d7) as r14_r7,
	try_divide(j.total_revenue_d30, j.total_revenue_d14) as r30_r14,
	try_divide(j.total_revenue_d60, j.total_revenue_d30) as r60_r30,
	try_divide(j.total_revenue_d90, j.total_revenue_d60) as r90_r60,
	try_divide(j.total_revenue_d120, j.total_revenue_d90) as r120_r90,
	try_divide(j.total_revenue_d150, j.total_revenue_d120) as r150_r120
FROM milestone m
LEFT JOIN lw_20250822_onlyprofit_cohort_cost_revenue_weekly_forcheck_view_by_j j
ON (j.monday <= m.endday AND j.sunday >= m.startday)
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createMilestoneCheckView():
	sql = """
CREATE OR REPLACE TEMP VIEW lw_20250822_milestone_check_view_by_j AS

	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return	


def main():
	createMilestonePredRevenueGrowthrateView()
	createAosOnlyprofitCohortCostRevenueDailyForCheckView()
	createIosOnlyprofitCohortCostRevenueDailyForCheckView()
	createOnlyprofitCohortCostRevenueDailyForCheckView()
	createOnlyprofitCohortCostRevenueWeeklyForCheckView()
	createMilestoneRealRevenueGrowthrateView()

if __name__ == "__main__":
	main()
	print("All views created successfully.")

# 每天执行
# 计算分国家的里程碑回本情况

import os
import datetime
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.dataBricks import execSql, execSql2


def createCostAndRevenueView():
	sql = """
CREATE OR REPLACE VIEW lw_20250815_country_milestone_profit_cost_revenue_view_by_j AS
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
),
roi AS (
	SELECT
		case
			when app_package in ('com.fun.lastwar.gp','com.fun.lastwar.vn.gp') then 'com.fun.lastwar.gp'
			when app_package in ('id6448786147','id6736925794') then 'id6448786147'
			else app_package
		end AS app_package,
		COALESCE(cg.country_group, 'other') AS country_group,
		install_day,
		cost_value_usd,
		revenue_d1,
		revenue_d3,
		revenue_d7,
		revenue_d14,
		revenue_d30,
		revenue_d60,
		revenue_d90,
		revenue_d120,
		revenue_d150
	FROM marketing.attribution.dws_overseas_roi_profit roi
	LEFT JOIN data_science.default.lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
)
SELECT
	r.app_package,
	r.country_group,
	m.startday,
	m.endday,
	SUM(r.cost_value_usd) AS cost,
	SUM(r.revenue_d1) AS revenue_d1,
	SUM(r.revenue_d3) AS revenue_d3,
	SUM(r.revenue_d7) AS revenue_d7,
	SUM(r.revenue_d14) AS revenue_d14,
	SUM(r.revenue_d30) AS revenue_d30,
	SUM(r.revenue_d60) AS revenue_d60,
	SUM(r.revenue_d90) AS revenue_d90,
	SUM(r.revenue_d120) AS revenue_d120,
	SUM(r.revenue_d150) AS revenue_d150
FROM roi r
JOIN milestone m
	ON r.install_day >= m.startday AND r.install_day <= m.endday
GROUP BY
	r.app_package,
	r.country_group,
	m.startday,
	m.endday
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)

def createPaybackView():
	sql = """
CREATE OR REPLACE VIEW lw_20250815_country_milestone_profit_payback_view_by_j AS
WITH roi_base AS (
	SELECT
		app_package,
		startday,
		endday,
		SUBSTR(startday, 1, 6) as install_month,
		country_group,
		cost,
		revenue_d1,
		revenue_d3,
		revenue_d7,
		revenue_d30,
		revenue_d60,
		revenue_d90,
		revenue_d120,
		revenue_d150,
		-- 计算ROI
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d1 / cost
		END AS roi1,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d3 / cost
		END AS roi3,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d7 / cost
		END AS roi7,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d30 / cost
		END AS roi30,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d60 / cost
		END AS roi60,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d90 / cost
		END AS roi90,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d120 / cost
		END AS roi120,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d150 / cost
		END AS roi150
	FROM
		lw_20250815_country_milestone_profit_cost_revenue_view_by_j
),
predict_base AS (
	SELECT
		*
	FROM
		lw_20250703_af_revenue_rise_ratio_predict2_month_view_by_j
	WHERE
		tag = 'af_onlyprofit_only_country_cohort'
),
predict AS (
	SELECT
		r.app_package,
		r.startday,
		r.endday,
		r.install_month,
		r.country_group,
		1 as kpi_target,
		r.roi60,
		r.roi90,
		r.roi120,
		r.roi150,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 as roi_7_predict_60,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 as roi_7_predict_90,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 as roi_7_predict_120,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 * p.predict_r150_r120 as roi_7_predict_150,
		r.roi30 * p.predict_r60_r30 as roi_30_predict_60,
		r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 as roi_30_predict_90,
		r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 as roi_30_predict_120,
		r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 * p.predict_r150_r120 as roi_30_predict_150,
		r.roi60 * p.predict_r90_r60 as roi_60_predict_90,
		r.roi60 * p.predict_r90_r60 * p.predict_r120_r90 as roi_60_predict_120,
		r.roi60 * p.predict_r90_r60 * p.predict_r120_r90 * p.predict_r150_r120 as roi_60_predict_150,
		r.roi90 * p.predict_r120_r90 as roi_90_predict_120,
		r.roi90 * p.predict_r120_r90 * p.predict_r150_r120 as roi_90_predict_150,
		r.roi120 * p.predict_r150_r120 as roi_120_predict_150
	FROM
		roi_base r
		LEFT JOIN predict_base p ON 
		r.app_package = p.app_package
		AND r.install_month = p.install_month
		AND r.country_group = p.country_group
)
SELECT
	app_package,
	startday,
	endday,
	install_month,
	country_group,
	CASE
		WHEN roi_7_predict_60 >= kpi_target THEN 2.0
		WHEN roi_7_predict_90 >= kpi_target THEN 2 + try_divide(kpi_target - roi_7_predict_60, roi_7_predict_90 - roi_7_predict_60)
		WHEN roi_7_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi_7_predict_90, roi_7_predict_120 - roi_7_predict_90)
		WHEN roi_7_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_7_predict_120, roi_7_predict_150 - roi_7_predict_120)
		ELSE 5.0
	END AS payback_7_p_150,
	CASE
		WHEN roi_30_predict_60 >= kpi_target THEN 2.0
		WHEN roi_30_predict_90 >= kpi_target THEN 2 + try_divide(kpi_target - roi_30_predict_60, roi_30_predict_90 - roi_30_predict_60)
		WHEN roi_30_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi_30_predict_90, roi_30_predict_120 - roi_30_predict_90)
		WHEN roi_30_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_30_predict_120, roi_30_predict_150 - roi_30_predict_120)
		ELSE 5.0
	END AS payback_30_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi_60_predict_90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi_60_predict_90 - roi60)
		WHEN roi_60_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi_60_predict_90, roi_60_predict_120 - roi_60_predict_90)
		WHEN roi_60_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_60_predict_120, roi_60_predict_150 - roi_60_predict_120)
		ELSE 5.0
	END AS payback_60_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi90 - roi60)
		WHEN roi_90_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi90, roi_90_predict_120 - roi90)
		WHEN roi_90_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_90_predict_120, roi_90_predict_150 - roi_90_predict_120)
		ELSE 5.0
	END AS payback_90_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi90 - roi60)
		WHEN roi120 >= kpi_target THEN 3 + try_divide(kpi_target - roi90, roi120 - roi90)
		WHEN roi150 >= kpi_target THEN 4 + try_divide(kpi_target - roi120, roi_120_predict_150 - roi120)
		ELSE 5.0
	END AS payback_120_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi90 - roi60)
		WHEN roi120 >= kpi_target THEN 3 + try_divide(kpi_target - roi90, roi120 - roi90)
		WHEN roi150 >= kpi_target THEN 4 + try_divide(kpi_target - roi120, roi150 - roi120)
		ELSE 5.0
	END AS payback_150
FROM
	predict;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 真实回本周期，是根据90日、120日、150日的ROI来计算的
def createRealPaybackView():
	sql = """
CREATE OR REPLACE VIEW lw_20250815_country_milestone_profit_real_payback_view_by_j AS
WITH roi_base AS (
	SELECT
		app_package,
		startday,
		endday,
		SUBSTR(startday, 1, 6) as install_month,
		country_group,
		cost,
		revenue_d1,
		revenue_d3,
		revenue_d7,
		revenue_d30,
		revenue_d60,
		revenue_d90,
		revenue_d120,
		revenue_d150,
		-- 计算ROI
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d1 / cost
		END AS roi1,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d3 / cost
		END AS roi3,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d7 / cost
		END AS roi7,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d30 / cost
		END AS roi30,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d60 / cost
		END AS roi60,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d90 / cost
		END AS roi90,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d120 / cost
		END AS roi120,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d150 / cost
		END AS roi150
	FROM
		lw_20250815_country_milestone_profit_cost_revenue_view_by_j
),
predict_base AS (
	SELECT
		*
	FROM
		lw_20250703_af_revenue_rise_ratio_predict2_month_view_by_j
	WHERE
		tag = 'af_onlyprofit_only_country_cohort'
)
SELECT
	r.app_package,
	r.startday,
	r.endday,
	r.install_month,
	r.country_group,
	1 as kpi_target,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		ELSE NULL
	END AS payback_60,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + (kpi_target - roi60) /(roi90 - roi60)
		ELSE NULL
	END AS payback_90,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + (kpi_target - roi60) /(roi90 - roi60)
		WHEN roi120 >= kpi_target THEN 3 +(kpi_target - roi90) /(roi120 - roi90)
		ELSE NULL
	END AS payback_120,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + (kpi_target - roi60) /(roi90 - roi60)
		WHEN roi120 >= kpi_target THEN 3 +(kpi_target - roi90) /(roi120 - roi90)
		WHEN roi150 >= kpi_target THEN 4 +(kpi_target - roi120) /(roi150 - roi120)
		ELSE 5.0
	END AS payback_150
FROM
	roi_base r
	LEFT JOIN predict_base p ON r.app_package = p.app_package
	AND r.install_month = p.install_month
	AND r.country_group = p.country_group
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return



# 分开获得Aos
def createAosCostAndRevenueView():
	sql = """
CREATE OR REPLACE VIEW lw_20250815_aos_profit_cost_revenue_view_by_j AS
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
		FROM prodb.public.applovin_data_v3
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
	END
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 分开获得Ios
def createIosCostAndRevenueView():
	sql = """
CREATE OR REPLACE VIEW lw_20250815_ios_profit_cost_revenue_view_by_j AS
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
		WHEN mediasource IN ('Facebook Ads', 'bytedanceglobal_int','moloco_int') THEN mediasource
		ELSE 'other'
	END as mediasource,
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
		FROM prodb.public.applovin_data_v3
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
		WHEN mediasource IN ('Facebook Ads', 'bytedanceglobal_int','moloco_int') THEN mediasource
		ELSE 'other'
	END
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 合并Aos和Ios的花费和收入
def createAosAndIosCostAndRevenueView():
	sql = """
CREATE OR REPLACE VIEW lw_20250815_aos_ios_profit_cost_revenue_view_by_j AS
SELECT
	*
FROM lw_20250815_aos_profit_cost_revenue_view_by_j
UNION ALL
SELECT
	*
FROM lw_20250815_ios_profit_cost_revenue_view_by_j
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 按照分媒体，获得花费和收入
# 其中花费是不分媒体的
# 收入是分媒体的
def createCostAndRevenueView2():
	sql = """
CREATE
OR REPLACE VIEW lw_20250815_country_milestone_profit_cost_revenue_view2_by_j AS WITH milestone AS (
	SELECT
		startday,
		COALESCE(
			endday,
			date_format(date_sub(current_date(), 8), 'yyyyMMdd')
		) AS endday
	FROM
		marketing.attribution.cdm_ext_milestone_config
	WHERE
		app = 502
	GROUP BY
		startday,
		COALESCE(
			endday,
			date_format(date_sub(current_date(), 8), 'yyyyMMdd')
		)
),
cost AS (
	SELECT
		app_package,
		country_group,
		startday,
		endday,
		sum(cost) as cost
	FROM
		lw_20250815_aos_ios_profit_cost_revenue_view_by_j t
		JOIN milestone m ON t.install_day >= m.startday
		AND t.install_day <= m.endday
	GROUP BY
		app_package,
		country_group,
		startday,
		endday
),
revenue as (
	SELECT
		app_package,
		country_group,
		startday,
		endday,
		mediasource,
		sum(revenue_d1) as revenue_d1,
		sum(revenue_d3) as revenue_d3,
		sum(revenue_d7) as revenue_d7,
		sum(revenue_d14) as revenue_d14,
		sum(revenue_d30) as revenue_d30,
		sum(revenue_d60) as revenue_d60,
		sum(revenue_d90) as revenue_d90,
		sum(revenue_d120) as revenue_d120,
		sum(revenue_d150) as revenue_d150
	FROM
		lw_20250815_aos_ios_profit_cost_revenue_view_by_j t
		JOIN milestone m ON t.install_day >= m.startday
		AND t.install_day <= m.endday
	GROUP BY
		app_package,
		country_group,
		startday,
		endday,
		mediasource
)
SELECT
	c.app_package,
	c.country_group,
	r.mediasource,
	c.startday,
	c.endday,
	MAX(c.cost) AS cost,
	SUM(r.revenue_d1) AS revenue_d1,
	SUM(r.revenue_d3) AS revenue_d3,
	SUM(r.revenue_d7) AS revenue_d7,
	SUM(r.revenue_d14) AS revenue_d14,
	SUM(r.revenue_d30) AS revenue_d30,
	SUM(r.revenue_d60) AS revenue_d60,
	SUM(r.revenue_d90) AS revenue_d90,
	SUM(r.revenue_d120) AS revenue_d120,
	SUM(r.revenue_d150) AS revenue_d150
FROM
	cost c
	LEFT JOIN revenue r ON c.app_package = r.app_package
	AND c.country_group = r.country_group
	AND c.startday = r.startday
	AND c.endday = r.endday
GROUP BY
	c.app_package,
	c.country_group,
	r.mediasource,
	c.startday,
	c.endday
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)

def createPaybackView2():
	sql = """
CREATE OR REPLACE VIEW lw_20250815_country_milestone_profit_payback_view2_by_j AS
WITH roi_base AS (
	SELECT
		app_package,
		startday,
		endday,
		SUBSTR(startday, 1, 6) as install_month,
		country_group,
		mediasource,
		cost,
		revenue_d1,
		revenue_d3,
		revenue_d7,
		revenue_d30,
		revenue_d60,
		revenue_d90,
		revenue_d120,
		revenue_d150,
		-- 计算ROI
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d1 / cost
		END AS roi1,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d3 / cost
		END AS roi3,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d7 / cost
		END AS roi7,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d30 / cost
		END AS roi30,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d60 / cost
		END AS roi60,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d90 / cost
		END AS roi90,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d120 / cost
		END AS roi120,
		CASE
			WHEN cost = 0 THEN 0
			ELSE revenue_d150 / cost
		END AS roi150
	FROM
		lw_20250815_country_milestone_profit_cost_revenue_view2_by_j
),
predict_base AS (
	SELECT
		*
	FROM
		lw_20250703_af_revenue_rise_ratio_predict2_month_view_by_j
	WHERE
		tag = 'onlyprofit_forpayback_cohort'
),
predict AS (
	SELECT
		r.app_package,
		r.startday,
		r.endday,
		r.install_month,
		r.country_group,
		r.mediasource,
		1 as kpi_target,
		r.roi60,
		r.roi90,
		r.roi120,
		r.roi150,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 as roi_7_predict_60,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 as roi_7_predict_90,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 as roi_7_predict_120,
		r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 * p.predict_r150_r120 as roi_7_predict_150,
		r.roi30 * p.predict_r60_r30 as roi_30_predict_60,
		r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 as roi_30_predict_90,
		r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 as roi_30_predict_120,
		r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 * p.predict_r150_r120 as roi_30_predict_150,
		r.roi60 * p.predict_r90_r60 as roi_60_predict_90,
		r.roi60 * p.predict_r90_r60 * p.predict_r120_r90 as roi_60_predict_120,
		r.roi60 * p.predict_r90_r60 * p.predict_r120_r90 * p.predict_r150_r120 as roi_60_predict_150,
		r.roi90 * p.predict_r120_r90 as roi_90_predict_120,
		r.roi90 * p.predict_r120_r90 * p.predict_r150_r120 as roi_90_predict_150,
		r.roi120 * p.predict_r150_r120 as roi_120_predict_150
	FROM
		roi_base r
		LEFT JOIN predict_base p ON 
		r.app_package = p.app_package
		AND r.install_month = p.install_month
		AND r.country_group = p.country_group
		AND r.mediasource = p.mediasource
)
SELECT
	app_package,
	startday,
	endday,
	install_month,
	country_group,
	CASE
		WHEN roi_7_predict_60 >= kpi_target THEN 2.0
		WHEN roi_7_predict_90 >= kpi_target THEN 2 + try_divide(kpi_target - roi_7_predict_60, roi_7_predict_90 - roi_7_predict_60)
		WHEN roi_7_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi_7_predict_90, roi_7_predict_120 - roi_7_predict_90)
		WHEN roi_7_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_7_predict_120, roi_7_predict_150 - roi_7_predict_120)
		ELSE 5.0
	END AS payback_7_p_150,
	CASE
		WHEN roi_30_predict_60 >= kpi_target THEN 2.0
		WHEN roi_30_predict_90 >= kpi_target THEN 2 + try_divide(kpi_target - roi_30_predict_60, roi_30_predict_90 - roi_30_predict_60)
		WHEN roi_30_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi_30_predict_90, roi_30_predict_120 - roi_30_predict_90)
		WHEN roi_30_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_30_predict_120, roi_30_predict_150 - roi_30_predict_120)
		ELSE 5.0
	END AS payback_30_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi_60_predict_90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi_60_predict_90 - roi60)
		WHEN roi_60_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi_60_predict_90, roi_60_predict_120 - roi_60_predict_90)
		WHEN roi_60_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_60_predict_120, roi_60_predict_150 - roi_60_predict_120)
		ELSE 5.0
	END AS payback_60_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi90 - roi60)
		WHEN roi_90_predict_120 >= kpi_target THEN 3 + try_divide(kpi_target - roi90, roi_90_predict_120 - roi90)
		WHEN roi_90_predict_150 >= kpi_target THEN 4 + try_divide(kpi_target - roi_90_predict_120, roi_90_predict_150 - roi_90_predict_120)
		ELSE 5.0
	END AS payback_90_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi90 - roi60)
		WHEN roi120 >= kpi_target THEN 3 + try_divide(kpi_target - roi90, roi120 - roi90)
		WHEN roi150 >= kpi_target THEN 4 + try_divide(kpi_target - roi120, roi_120_predict_150 - roi120)
		ELSE 5.0
	END AS payback_120_p_150,
	CASE
		WHEN roi60 >= kpi_target THEN 2.0
		WHEN roi90 >= kpi_target THEN 2 + try_divide(kpi_target - roi60, roi90 - roi60)
		WHEN roi120 >= kpi_target THEN 3 + try_divide(kpi_target - roi90, roi120 - roi90)
		WHEN roi150 >= kpi_target THEN 4 + try_divide(kpi_target - roi120, roi150 - roi120)
		ELSE 5.0
	END AS payback_150
FROM
	(
	select 
		app_package,
		startday,
		endday,
		install_month,
		country_group,
		1 as kpi_target,
		sum(roi60) as roi60,
		sum(roi90) as roi90,
		sum(roi120) as roi120,
		sum(roi150) as roi150,
		sum(roi_7_predict_60) as roi_7_predict_60,
		sum(roi_7_predict_90) as roi_7_predict_90,
		sum(roi_7_predict_120) as roi_7_predict_120,
		sum(roi_7_predict_150) as roi_7_predict_150,
		sum(roi_30_predict_60) as roi_30_predict_60,
		sum(roi_30_predict_90) as roi_30_predict_90,
		sum(roi_30_predict_120) as roi_30_predict_120,
		sum(roi_30_predict_150) as roi_30_predict_150,
		sum(roi_60_predict_90) as roi_60_predict_90,
		sum(roi_60_predict_120) as roi_60_predict_120,
		sum(roi_60_predict_150) as roi_60_predict_150,
		sum(roi_90_predict_120) as roi_90_predict_120,
		sum(roi_90_predict_150) as roi_90_predict_150,
		sum(roi_120_predict_150) as roi_120_predict_150
	from predict
	group by
		app_package,
		startday,
		endday,
		install_month,
		country_group
	);
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


def createPaybackTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250815_country_milestone_profit_payback_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250815_country_milestone_profit_payback_table_by_j (
select
	pred.app_package,
	pred.country_group,
	pred.startday,
	case
		when datediff(current_date(), to_date(pred.endday, 'yyyyMMdd')) > 150 then pred.payback_150
		when datediff(current_date(), to_date(pred.endday, 'yyyyMMdd')) > 120 then pred.payback_120_p_150
		when datediff(current_date(), to_date(pred.endday, 'yyyyMMdd')) > 90 then pred.payback_90_p_150
		when datediff(current_date(), to_date(pred.endday, 'yyyyMMdd')) > 60 then pred.payback_60_p_150
		when datediff(current_date(), to_date(pred.endday, 'yyyyMMdd')) > 30 then pred.payback_30_p_150
		when datediff(current_date(), to_date(pred.endday, 'yyyyMMdd')) > 7 then pred.payback_7_p_150
		else null
	end as payback_month,
	case
		when datediff(current_date(), to_date(pred2.endday, 'yyyyMMdd')) > 150 then pred2.payback_150
		when datediff(current_date(), to_date(pred2.endday, 'yyyyMMdd')) > 120 then pred2.payback_120_p_150
		when datediff(current_date(), to_date(pred2.endday, 'yyyyMMdd')) > 90 then pred2.payback_90_p_150
		when datediff(current_date(), to_date(pred2.endday, 'yyyyMMdd')) > 60 then pred2.payback_60_p_150
		when datediff(current_date(), to_date(pred2.endday, 'yyyyMMdd')) > 30 then pred2.payback_30_p_150
		when datediff(current_date(), to_date(pred2.endday, 'yyyyMMdd')) > 7 then pred2.payback_7_p_150
		else null
	end as payback_month2,
	case
		when datediff(current_date(), to_date(real.endday, 'yyyyMMdd')) > 150 then real.payback_150
		when datediff(current_date(), to_date(real.endday, 'yyyyMMdd')) > 120 then real.payback_120
		when datediff(current_date(), to_date(real.endday, 'yyyyMMdd')) > 90 then real.payback_90
		when datediff(current_date(), to_date(real.endday, 'yyyyMMdd')) > 60 then real.payback_60
		else null
	end as real_payback_month
from data_science.default.lw_20250815_country_milestone_profit_payback_view_by_j pred
left join data_science.default.lw_20250815_country_milestone_profit_real_payback_view_by_j real
on pred.app_package = real.app_package
	and pred.country_group = real.country_group
	and pred.startday = real.startday
left join data_science.default.lw_20250815_country_milestone_profit_payback_view2_by_j pred2
on pred.app_package = pred2.app_package
	and pred.country_group = pred2.country_group
	and pred.startday = pred2.startday
)
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return



def main():
	# createCostAndRevenueView()
	# createPaybackView()
	# createRealPaybackView()
	

	# createAosCostAndRevenueView()
	# createIosCostAndRevenueView()
	# createAosAndIosCostAndRevenueView()
	createCostAndRevenueView2()
	createPaybackView2()

	createPaybackTable()

if __name__ == "__main__":
	main()
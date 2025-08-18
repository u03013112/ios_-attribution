import os
import datetime
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.dataBricks import execSql, execSql2


def createCountryGroupTable():
	sql0 = """
DROP TABLE IF EXISTS lw_country_group_table_by_j_20250703;
	"""
	print(f"Executing SQL: {sql0}")
	execSql2(sql0)


	sql1 = """
-- 创建国家分组表 lw_country_group_table_by_j_20250703
CREATE TABLE IF NOT EXISTS lw_country_group_table_by_j_20250703 (
	country STRING,
	country_group STRING
); """
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)

	sql2 = """
-- 插入数据
INSERT INTO lw_country_group_table_by_j_20250703 (country, country_group)
VALUES
('AD','T1'),('AT','T1'),('AU','T1'),('BE','T1'),('CA','T1'),('CH','T1'),('DE','T1'),
('DK','T1'),('FI','T1'),('FR','T1'),('HK','T1'),('IE','T1'),('IS','T1'),('IT','T1'),
('LI','T1'),('LU','T1'),('MC','T1'),('NL','T1'),('NO','T1'),('NZ','T1'),('SE','T1'),
('SG','T1'),('UK','T1'),('MO','T1'),('IL','T1'),('TW','T1'),('GB','T1'),
('US','US'),
('JP','JP'),
('KR','KR'),
('SA','GCC'),('AE','GCC'),('QA','GCC'),('KW','GCC'),('BH','GCC'),('OM','GCC');
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

def createMonthView():
	sql = """
CREATE OR REPLACE VIEW month_view_by_j AS
SELECT
	install_month,
	(
		CAST(
			SUBSTR(TO_CHAR(getdate(), 'yyyymm'), 1, 4) AS BIGINT
		) * 12 + CAST(
			SUBSTR(TO_CHAR(getdate(), 'yyyymm'), 5, 2) AS BIGINT
		)
	) - (
		CAST(SUBSTR(install_month, 1, 4) AS BIGINT) * 12 + CAST(SUBSTR(install_month, 5, 2) AS BIGINT)
	) AS month_diff
FROM
	(
		SELECT
			DISTINCT SUBSTR(install_day, 1, 6) AS install_month
		FROM
			marketing.attribution.dws_overseas_public_roi
		WHERE
			app = '502'
	) t
ORDER BY
	install_month;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


#####################################################
# AF 花费、收入24小时cohort数据，包括普通、添加adtype、大盘、只分国家 4种

# AF 花费、收入数据 24小时版本
def createAfAppMediaCountryCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cohort_cost_revenue_app_country_group_media_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_cohort_d1) AS revenue_d1,
	SUM(revenue_cohort_d3) AS revenue_d3,
	SUM(revenue_cohort_d7) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_public_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	roi.app = '502'
	AND m.month_diff > 0
	AND roi.facebook_segment IN ('country', 'N/A')
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	COALESCE(cg.country_group, 'other'),
	mediasource,
	ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# iOS 版本，额外applovin_int 拆分成 applovin_int_d7 和 applovin_int_d28
# AF 花费、收入数据 24小时版本
def createAfIosAppMediaCountryCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_ios_af_cohort_cost_revenue_app_country_group_media_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		ELSE mediasource
	END as mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_cohort_d1) AS revenue_d1,
	SUM(revenue_cohort_d3) AS revenue_d3,
	SUM(revenue_cohort_d7) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_public_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	roi.app = '502'
	AND m.month_diff > 0
	AND roi.facebook_segment IN ('country', 'N/A')
	AND app_package IN ('id6448786147','id6736925794')
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	COALESCE(cg.country_group, 'other'),
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		ELSE mediasource
	END,
	ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


# AF 花费、收入数据 只分app和国家，不分媒体 24小时版本
def createAfAppCountryCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cohort_cost_revenue_app_country_group_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'ALL' AS mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_cohort_d1) AS revenue_d1,
	SUM(revenue_cohort_d3) AS revenue_d3,
	SUM(revenue_cohort_d7) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_public_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	roi.app = '502'
	AND m.month_diff > 0
	AND roi.facebook_segment IN ('country', 'N/A')
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	country_group
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# AF 花费、收入数据 大盘 24小时版本
def createAfAppCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cohort_cost_revenue_app_month_view_by_j AS
SELECT
	app_package,
	m.install_month,
	country_group,
	mediasource,
	ad_type,
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
FROM
(
		SELECT
			app_package,
			SUBSTR(install_day, 1, 6) AS install_month,
			'ALL' AS country_group,
			'ALL' AS mediasource,
			'ALL' AS ad_type,
			SUM(cost_value_usd) AS cost,
			SUM(revenue_cohort_d1) AS revenue_d1,
			SUM(revenue_cohort_d3) AS revenue_d3,
			SUM(revenue_cohort_d7) AS revenue_d7,
			SUM(revenue_cohort_d14) AS revenue_d14,
			SUM(revenue_cohort_d30) AS revenue_d30,
			SUM(revenue_cohort_d60) AS revenue_d60,
			SUM(revenue_cohort_d90) AS revenue_d90,
			SUM(revenue_cohort_d120) AS revenue_d120,
			SUM(revenue_cohort_d150) AS revenue_d150
		FROM
			marketing.attribution.dws_overseas_public_roi
		WHERE
			app = '502'
			AND facebook_segment IN ('country', 'N/A')
		GROUP BY
			app_package,
			install_month
	) a
	LEFT JOIN month_view_by_j m ON a.install_month = m.install_month
WHERE
	m.month_diff > 0;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# AF 花费、收入数据 汇总表，24小时版本
def createAfCohortCostRevenueMonthyTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_af_cohort_cost_revenue_app_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_af_cohort_cost_revenue_app_month_table_by_j AS
SELECT
	*,
	'af_cohort' AS tag
FROM lw_20250703_af_cohort_cost_revenue_app_country_group_media_month_view_by_j
UNION ALL
SELECT
	*,
	'only_country_cohort' AS tag
FROM lw_20250703_af_cohort_cost_revenue_app_country_group_month_view_by_j
UNION ALL
SELECT
	*,
	'ALL_cohort' AS tag
FROM lw_20250703_af_cohort_cost_revenue_app_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# GPIR 花费、收入24小时cohort数据，包括普通、添加adtype 2种 

# GPIR版本的月视图，24小时版本
def createGPIRAppMediaCountryCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_cohort_cost_revenue_app_country_group_media_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	mediasource,
	'ALL' AS ad_type,
	sum(cost_value_usd) as cost,
	SUM(revenue_cohort_d1) AS revenue_d1,
	SUM(revenue_cohort_d3) AS revenue_d3,
	SUM(revenue_cohort_d7) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
from
	marketing.attribution.dws_overseas_gpir_roi roi
	left join lw_country_group_table_by_j_20250703 cg on roi.country = cg.country
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
where
	roi.facebook_segment in ('country', 'N/A')
	AND m.month_diff > 0
group by
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	country_group,
	mediasource
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# GPIR 花费、收入数据 汇总表
def createGPIRCohortCostRevenueMonthyTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_gpir_cohort_cost_revenue_app_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_gpir_cohort_cost_revenue_app_month_table_by_j AS
SELECT
	*,
	'gpir_cohort' AS tag
FROM lw_20250703_gpir_cohort_cost_revenue_app_country_group_media_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# AF纯利 花费、收入24小时cohort数据，包括普通、添加adtype 2种

# AF纯利表，并且24小时版本
def createAfOnlyprofitAppMediaCountryCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_onlyprofit_cost_revenue_app_country_group_media_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	case 
		when mediasource in ('restricted','Facebook Ads') then 'Facebook Ads'
		when mediasource in ('bytedanceglobal_int','tiktokglobal_int') then 'bytedanceglobal_int'
		else mediasource
	end AS mediasource,
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
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	m.month_diff > 0
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	COALESCE(cg.country_group, 'other'),
	case 
		when mediasource in ('restricted','Facebook Ads') then 'Facebook Ads'
		when mediasource in ('bytedanceglobal_int','tiktokglobal_int') then 'bytedanceglobal_int'
		else mediasource
	end,
	ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# AF纯利表，只分app和国家，不分媒体 24小时版本
def createAfOnlyprofitAppCountryCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_onlyprofit_cost_revenue_app_country_group_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'ALL' mediasource,
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
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	m.month_diff > 0
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	COALESCE(cg.country_group, 'other')
	;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# AF纯利表 大盘 24小时版本
def createAfOnlyprofitAppCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_onlyprofit_cost_revenue_app_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	'ALL' AS country_group,
	'ALL' mediasource,
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
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	m.month_diff > 0
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6)
	;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# AF纯利表 汇总
def createAfOnlyProfitCohortCostRevenueMonthyTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_af_onlyprofit_cost_revenue_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_af_onlyprofit_cost_revenue_month_table_by_j AS
SELECT
	*,
	'af_onlyprofit_cohort' AS tag
FROM lw_20250703_af_onlyprofit_cost_revenue_app_country_group_media_month_view_by_j
UNION ALL
SELECT
	*,
	'af_onlyprofit_only_country_cohort' AS tag
FROM lw_20250703_af_onlyprofit_cost_revenue_app_country_group_month_view_by_j
UNION ALL
SELECT
	*,
	'af_onlyprofit_ALL_cohort' AS tag
FROM lw_20250703_af_onlyprofit_cost_revenue_app_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# GPIR纯利 花费、收入24小时cohort数据，包括普通、添加adtype 2种

# GPIR纯利表，并且24小时版本
def createGPIROnlyprofitAppMediaCountryCohortCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_onlyprofit_cost_revenue_app_country_group_media_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	mediasource,
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
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	m.month_diff > 0
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	COALESCE(cg.country_group, 'other'),
	mediasource,
	ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# GPIR纯利表 汇总
def createGPIROnlyProfitCohortCostRevenueMonthyTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_gpir_onlyprofit_cost_revenue_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)

	sql2 = """
CREATE TABLE lw_20250703_gpir_onlyprofit_cost_revenue_month_table_by_j AS
SELECT
	*,
	'gpir_onlyprofit_cohort' AS tag
FROM lw_20250703_gpir_onlyprofit_cost_revenue_app_country_group_media_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# 纯净版
# 纯净版只要gpir corhort数据
# 纯净版要把applovin 拆成D7和D28两个媒体
# 纯净版只分app、国家、媒体，不再细分adtype
# 纯净版tag：for_ua
def createForUaCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_for_ua_cost_revenue_app_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	case
	when roi.mediasource = 'applovin_int' and roi.campaign_name like '%D7%' then 'applovin_int_d7'
	when roi.mediasource = 'applovin_int' and roi.campaign_name like '%D28%' then 'applovin_int_d28'
	when roi.mediasource = 'applovin_int' then 'applovin_int'
	else roi.mediasource end as mediasource,
	'ALL' AS ad_type,
	sum(cost_value_usd) as cost,
	SUM(revenue_cohort_d1) AS revenue_d1,
	SUM(revenue_cohort_d3) AS revenue_d3,
	SUM(revenue_cohort_d7) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_gpir_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
	roi.facebook_segment IN ('country', 'N/A')
	AND m.month_diff > 0
GROUP BY
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	COALESCE(cg.country_group, 'other'),
	case
	when roi.mediasource = 'applovin_int' and roi.campaign_name like '%D7%' then 'applovin_int_d7'
	when roi.mediasource = 'applovin_int' and roi.campaign_name like '%D28%' then 'applovin_int_d28'
	when roi.mediasource = 'applovin_int' then 'applovin_int'
	else roi.mediasource end

;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createAppLovinRatioView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_applovin_ratio_view_by_j AS
WITH base_data AS (
	SELECT 
		app_package,
		install_month,
		country_group,
		SUM(CASE WHEN mediasource = 'applovin_int' THEN revenue_d1 ELSE 0 END) AS applovin_int_revenue_d1,
		SUM(CASE WHEN mediasource = 'applovin_int' THEN revenue_d3 ELSE 0 END) AS applovin_int_revenue_d3,
		SUM(CASE WHEN mediasource = 'applovin_int' THEN revenue_d7 ELSE 0 END) AS applovin_int_revenue_d7,
		SUM(CASE WHEN mediasource = 'applovin_int_d7' THEN revenue_d1 ELSE 0 END) AS applovin_int_d7_revenue_d1,
		SUM(CASE WHEN mediasource = 'applovin_int_d7' THEN revenue_d3 ELSE 0 END) AS applovin_int_d7_revenue_d3,
		SUM(CASE WHEN mediasource = 'applovin_int_d7' THEN revenue_d7 ELSE 0 END) AS applovin_int_d7_revenue_d7,
		SUM(CASE WHEN mediasource = 'applovin_int_d28' THEN revenue_d1 ELSE 0 END) AS applovin_int_d28_revenue_d1,
		SUM(CASE WHEN mediasource = 'applovin_int_d28' THEN revenue_d3 ELSE 0 END) AS applovin_int_d28_revenue_d3,
		SUM(CASE WHEN mediasource = 'applovin_int_d28' THEN revenue_d7 ELSE 0 END) AS applovin_int_d28_revenue_d7,
		SUM(CASE WHEN mediasource IN ('applovin_int', 'applovin_int_d7', 'applovin_int_d28') THEN revenue_d1 ELSE 0 END) AS total_applovin_revenue_d1,
		SUM(CASE WHEN mediasource IN ('applovin_int', 'applovin_int_d7', 'applovin_int_d28') THEN revenue_d3 ELSE 0 END) AS total_applovin_revenue_d3,
		SUM(CASE WHEN mediasource IN ('applovin_int', 'applovin_int_d7', 'applovin_int_d28') THEN revenue_d7 ELSE 0 END) AS total_applovin_revenue_d7
	FROM lw_20250703_for_ua_cost_revenue_app_month_view_by_j
	GROUP BY app_package, install_month, country_group
),
monthly_ratios AS (
	SELECT 
		app_package,
		install_month,
		country_group,
		applovin_int_revenue_d1,
		applovin_int_revenue_d3,
		applovin_int_revenue_d7,
		applovin_int_d7_revenue_d1,
		applovin_int_d7_revenue_d3,
		applovin_int_d7_revenue_d7,
		applovin_int_d28_revenue_d1,
		applovin_int_d28_revenue_d3,
		applovin_int_d28_revenue_d7,
		total_applovin_revenue_d1,
		total_applovin_revenue_d3,
		total_applovin_revenue_d7,
		-- 计算当月比例
		CASE 
			WHEN total_applovin_revenue_d1 > 0 
			THEN ROUND(applovin_int_revenue_d1 / total_applovin_revenue_d1, 4)
			ELSE 0 
		END AS current_month_r1_ratio,
		CASE 
			WHEN total_applovin_revenue_d3 > 0 
			THEN ROUND(applovin_int_revenue_d3 / total_applovin_revenue_d3, 4)
			ELSE 0 
		END AS current_month_r3_ratio,
		CASE 
			WHEN total_applovin_revenue_d7 > 0 
			THEN ROUND(applovin_int_revenue_d7 / total_applovin_revenue_d7, 4)
			ELSE 0 
		END AS current_month_r7_ratio,
		ROW_NUMBER() OVER (
			PARTITION BY app_package, country_group 
			ORDER BY install_month
		) AS rn
	FROM base_data
),
with_lag_ratios AS (
	SELECT 
		*,
		COALESCE(LAG(current_month_r1_ratio, 1) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag1_r1_ratio,
		COALESCE(LAG(current_month_r1_ratio, 2) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag2_r1_ratio,
		COALESCE(LAG(current_month_r1_ratio, 3) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag3_r1_ratio,
		COALESCE(LAG(current_month_r3_ratio, 1) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag1_r3_ratio,
		COALESCE(LAG(current_month_r3_ratio, 2) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag2_r3_ratio,
		COALESCE(LAG(current_month_r3_ratio, 3) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag3_r3_ratio,
		COALESCE(LAG(current_month_r7_ratio, 1) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag1_r7_ratio,
		COALESCE(LAG(current_month_r7_ratio, 2) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag2_r7_ratio,
		COALESCE(LAG(current_month_r7_ratio, 3) OVER (PARTITION BY app_package, country_group ORDER BY install_month), 0) AS lag3_r7_ratio
	FROM monthly_ratios
)
SELECT 
	app_package,
	install_month,
	country_group,
	-- 当月各项收入
	applovin_int_revenue_d1,
	applovin_int_revenue_d3,
	applovin_int_revenue_d7,
	applovin_int_d7_revenue_d1,
	applovin_int_d7_revenue_d3,
	applovin_int_d7_revenue_d7,
	applovin_int_d28_revenue_d1,
	applovin_int_d28_revenue_d3,
	applovin_int_d28_revenue_d7,
	-- 当月比例
	current_month_r1_ratio,
	current_month_r3_ratio,
	current_month_r7_ratio,
	-- 最近3个月去掉最大值后的平均值
	ROUND(
		(lag1_r1_ratio + lag2_r1_ratio + lag3_r1_ratio - 
		 GREATEST(lag1_r1_ratio, lag2_r1_ratio, lag3_r1_ratio)) / 2, 4
	) AS applovin_int_other_r1_ratio,
		
	ROUND(
		(lag1_r3_ratio + lag2_r3_ratio + lag3_r3_ratio - 
		 GREATEST(lag1_r3_ratio, lag2_r3_ratio, lag3_r3_ratio)) / 2, 4
	) AS applovin_int_other_r3_ratio,
		
	ROUND(
		(lag1_r7_ratio + lag2_r7_ratio + lag3_r7_ratio - 
		 GREATEST(lag1_r7_ratio, lag2_r7_ratio, lag3_r7_ratio)) / 2, 4
	) AS applovin_int_other_r7_ratio
FROM with_lag_ratios
ORDER BY app_package, country_group, install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return
	
def createForUaCostRevenueMonthyTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_for_ua_cost_revenue_app_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_for_ua_cost_revenue_app_month_table_by_j AS
SELECT
	* ,
	'for_ua' AS tag
FROM lw_20250703_for_ua_cost_revenue_app_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# 拟合iOS结果相关

# 基础表，按天 花费、收入
def createIosAfCostRevenueDayView():
	sql = """
CREATE OR REPLACE VIEW lw_20250806_af_cohort_cost_revenue_app_country_group_media_day_view_by_j AS
SELECT
	'id6448786147' as app_package,
	roi.install_day,
	COALESCE(cg.country_group, 'other') AS country_group,
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		ELSE mediasource
	END as mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_cohort_d1) AS revenue_d1,
	SUM(revenue_cohort_d3) AS revenue_d3,
	SUM(revenue_cohort_d7) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_public_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.app = '502'
	AND roi.facebook_segment IN ('country', 'N/A')
	AND app_package IN ('id6448786147', 'id6736925794')
GROUP BY
	roi.install_day,
	COALESCE(cg.country_group, 'other'),
	CASE 
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
		ELSE mediasource
	END,
	ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 修正表，主要针对google的收入进行修正，按照google在所有媒体中的花费比例，估计他的收入
# 然后对应修正自然量，使得每天的总收入保持不变
# 改为直接创建table，防止后续试图循环引用的复杂问题
def createIosAfCostRevenueDayFixTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250806_af_cohort_cost_revenue_day_fix_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250806_af_cohort_cost_revenue_day_fix_table_by_j AS
WITH base_data AS (
	-- 获取基础数据
	SELECT
		app_package,
		install_day,
		country_group,
		mediasource,
		ad_type,
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
	FROM lw_20250806_af_cohort_cost_revenue_app_country_group_media_day_view_by_j
),
paid_media_totals AS (
	-- 计算付费媒体的总花费（用于计算Google的收入比例）
	SELECT
		app_package,
		install_day,
		country_group,
		SUM(cost) AS total_paid_cost,
		SUM(revenue_d1) AS total_paid_revenue_d1,
		SUM(revenue_d3) AS total_paid_revenue_d3,
		SUM(revenue_d7) AS total_paid_revenue_d7,
		SUM(revenue_d14) AS total_paid_revenue_d14,
		SUM(revenue_d30) AS total_paid_revenue_d30,
		SUM(revenue_d60) AS total_paid_revenue_d60,
		SUM(revenue_d90) AS total_paid_revenue_d90,
		SUM(revenue_d120) AS total_paid_revenue_d120,
		SUM(revenue_d150) AS total_paid_revenue_d150
	FROM base_data
	WHERE mediasource != 'Organic'
		AND mediasource != 'googleadwords_int'
	GROUP BY
		app_package,
		install_day,
		country_group
),
daily_totals AS (
	-- 计算每天的总收入（用于后续自然量修正）
	SELECT
		app_package,
		install_day,
		country_group,
		SUM(revenue_d1) AS total_revenue_d1,
		SUM(revenue_d3) AS total_revenue_d3,
		SUM(revenue_d7) AS total_revenue_d7,
		SUM(revenue_d14) AS total_revenue_d14,
		SUM(revenue_d30) AS total_revenue_d30,
		SUM(revenue_d60) AS total_revenue_d60,
		SUM(revenue_d90) AS total_revenue_d90,
		SUM(revenue_d120) AS total_revenue_d120,
		SUM(revenue_d150) AS total_revenue_d150
	FROM base_data
	GROUP BY
		app_package,
		install_day,
		country_group
),
adjusted_media_data AS (
	-- 调整Google的收入，其他媒体保持不变
	SELECT
		bd.app_package,
		bd.install_day,
		bd.country_group,
		bd.mediasource,
		bd.ad_type,
		bd.cost,
		-- 原始收入
		bd.revenue_d1 AS original_revenue_d1,
		bd.revenue_d3 AS original_revenue_d3,
		bd.revenue_d7 AS original_revenue_d7,
		bd.revenue_d14 AS original_revenue_d14,
		bd.revenue_d30 AS original_revenue_d30,
		bd.revenue_d60 AS original_revenue_d60,
		bd.revenue_d90 AS original_revenue_d90,
		bd.revenue_d120 AS original_revenue_d120,
		bd.revenue_d150 AS original_revenue_d150,
		-- 调整后收入
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d1 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d1
		END AS adjusted_revenue_d1,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d3 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d3
		END AS adjusted_revenue_d3,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d7 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d7
		END AS adjusted_revenue_d7,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d14 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d14
		END AS adjusted_revenue_d14,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d30 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d30
		END AS adjusted_revenue_d30,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d60 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d60
		END AS adjusted_revenue_d60,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d90 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d90
		END AS adjusted_revenue_d90,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d120 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d120
		END AS adjusted_revenue_d120,
		CASE
			WHEN bd.mediasource = 'googleadwords_int' THEN
				CASE WHEN COALESCE(pmt.total_paid_cost, 0) > 0 THEN
					pmt.total_paid_revenue_d150 * (bd.cost / pmt.total_paid_cost)
				ELSE 0 END
			ELSE bd.revenue_d150
		END AS adjusted_revenue_d150
	FROM base_data bd
	LEFT JOIN paid_media_totals pmt ON bd.app_package = pmt.app_package
									AND bd.install_day = pmt.install_day
									AND bd.country_group = pmt.country_group
),
adjusted_paid_totals AS (
	-- 计算调整后的付费媒体总收入
	SELECT
		app_package,
		install_day,
		country_group,
		SUM(adjusted_revenue_d1) AS total_adjusted_paid_revenue_d1,
		SUM(adjusted_revenue_d3) AS total_adjusted_paid_revenue_d3,
		SUM(adjusted_revenue_d7) AS total_adjusted_paid_revenue_d7,
		SUM(adjusted_revenue_d14) AS total_adjusted_paid_revenue_d14,
		SUM(adjusted_revenue_d30) AS total_adjusted_paid_revenue_d30,
		SUM(adjusted_revenue_d60) AS total_adjusted_paid_revenue_d60,
		SUM(adjusted_revenue_d90) AS total_adjusted_paid_revenue_d90,
		SUM(adjusted_revenue_d120) AS total_adjusted_paid_revenue_d120,
		SUM(adjusted_revenue_d150) AS total_adjusted_paid_revenue_d150
	FROM adjusted_media_data
	WHERE mediasource != 'Organic'
	GROUP BY
		app_package,
		install_day,
		country_group
)
-- 最终结果：付费媒体使用调整后收入，自然量使用修正后收入
SELECT
	amd.app_package,
	amd.install_day,
	amd.country_group,
	amd.mediasource,
	amd.ad_type,
	amd.cost,
	-- 对于自然量，使用修正后的收入；对于付费媒体，使用调整后的收入
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d1 - COALESCE(apt.total_adjusted_paid_revenue_d1, 0))
		ELSE amd.adjusted_revenue_d1
	END AS revenue_d1,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d3 - COALESCE(apt.total_adjusted_paid_revenue_d3, 0))
		ELSE amd.adjusted_revenue_d3
	END AS revenue_d3,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d7 - COALESCE(apt.total_adjusted_paid_revenue_d7, 0))
		ELSE amd.adjusted_revenue_d7
	END AS revenue_d7,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d14 - COALESCE(apt.total_adjusted_paid_revenue_d14, 0))
		ELSE amd.adjusted_revenue_d14
	END AS revenue_d14,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d30 - COALESCE(apt.total_adjusted_paid_revenue_d30, 0))
		ELSE amd.adjusted_revenue_d30
	END AS revenue_d30,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d60 - COALESCE(apt.total_adjusted_paid_revenue_d60, 0))
		ELSE amd.adjusted_revenue_d60
	END AS revenue_d60,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d90 - COALESCE(apt.total_adjusted_paid_revenue_d90, 0))
		ELSE amd.adjusted_revenue_d90
	END AS revenue_d90,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d120 - COALESCE(apt.total_adjusted_paid_revenue_d120, 0))
		ELSE amd.adjusted_revenue_d120
	END AS revenue_d120,
	CASE
		WHEN amd.mediasource = 'Organic' THEN
			GREATEST(0, dt.total_revenue_d150 - COALESCE(apt.total_adjusted_paid_revenue_d150, 0))
		ELSE amd.adjusted_revenue_d150
	END AS revenue_d150
FROM adjusted_media_data amd
LEFT JOIN daily_totals dt ON amd.app_package = dt.app_package
						   AND amd.install_day = dt.install_day
						   AND amd.country_group = dt.country_group
LEFT JOIN adjusted_paid_totals apt ON amd.app_package = apt.app_package
									AND amd.install_day = apt.install_day
									AND amd.country_group = apt.country_group
ORDER BY
	amd.app_package,
	amd.install_day,
	amd.country_group,
	amd.mediasource
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

def createIosAfCostRevenueMonthyFixView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_ios_af_cohort_cost_revenue_app_country_group_media_month_fix_view_by_j AS
SELECT
	app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	country_group,
	mediasource,
	ad_type,
	SUM(cost) AS cost,
	SUM(revenue_d1) AS revenue_d1,
	SUM(revenue_d3) AS revenue_d3,
	SUM(revenue_d7) AS revenue_d7,
	SUM(revenue_d14) AS revenue_d14,
	SUM(revenue_d30) AS revenue_d30,
	SUM(revenue_d60) AS revenue_d60,
	SUM(revenue_d90) AS revenue_d90,
	SUM(revenue_d120) AS revenue_d120,
	SUM(revenue_d150) AS revenue_d150
FROM lw_20250806_af_cohort_cost_revenue_day_fix_table_by_j
GROUP BY
	app_package,
	install_month,
	country_group,
	mediasource,
	ad_type
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 拟合表，用上面修正表的数据，结合拟合结果，预测各媒体收入
# 拟合结果在表lw_20250703_ios_bayesian_result_by_j中
# lw_20250703_ios_bayesian_result_by_j 创建CREATE TABLE IF NOT EXISTS rg_bi.lw_20250703_ios_bayesian_result_by_j(country_group STRING COMMENT '国家组', organic_revenue DOUBLE COMMENT '自然量收入', applovin_int_d7_coeff DOUBLE COMMENT 'applovin_int_d7系数', applovin_int_d28_coeff DOUBLE COMMENT 'applovin_int_d28系数', facebook_ads_coeff DOUBLE COMMENT 'Facebook Ads系数', moloco_int_coeff DOUBLE COMMENT 'moloco_int系数', bytedanceglobal_int_coeff DOUBLE COMMENT 'bytedanceglobal_int系数') PARTITIONED BY (tag STRING COMMENT '标签分区，格式：20250804_{organic_ratio}') STORED AS ALIORC TBLPROPERTIES ('columnar.nested.type'='true');
# 将媒体收入数据乘以对应的系数，得到拟合后的收入，没有媒体系数的不变
# 将原有自然量放弃，直接使用lw_20250703_ios_bayesian_result_by_j中的organic_revenue作为自然量收入
# organic_revenue中的tag代表不同的拟合结果，其中暂时只关注 20250806_10、20250806_20、20250806_30 3个，对应的，应该成成3套拟合后收入
# 只关注7日收入和120日收入，其中7日收入用于验算拟合效果，120日收入用于估测自然量
def createIosAfCostRevenueDayFitTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250806_af_cohort_cost_revenue_day_fit_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250806_af_cohort_cost_revenue_day_fit_table_by_j AS
WITH base_data AS (
	-- 获取修正后的基础数据（排除自然量，后续用拟合结果替换）
	SELECT
		app_package,
		install_day,
		country_group,
		mediasource,
		ad_type,
		cost,
		revenue_d7,
		revenue_d120
	FROM lw_20250806_af_cohort_cost_revenue_day_fix_table_by_j
	WHERE mediasource != 'Organic'
),
original_totals AS (
	-- 获取原始数据的总收入（用于计算120日自然量）
	SELECT
		app_package,
		install_day,
		country_group,
		SUM(revenue_d7) AS total_original_revenue_d7,
		SUM(revenue_d120) AS total_original_revenue_d120
	FROM lw_20250806_af_cohort_cost_revenue_day_fix_table_by_j
	GROUP BY
		app_package,
		install_day,
		country_group
),
bayesian_results AS (
	-- 获取拟合系数和自然量收入
	SELECT 
		country_group,
		organic_revenue,
		applovin_int_d7_coeff,
		applovin_int_d28_coeff,
		facebook_ads_coeff,
		moloco_int_coeff,
		bytedanceglobal_int_coeff,
		tag
	FROM lw_20250703_ios_bayesian_result_by_j
	WHERE tag IN ('20250806_10', '20250806_20', '20250808_20')
),
fitted_paid_media AS (
	-- 对付费媒体应用拟合系数
	SELECT
		bd.app_package,
		bd.install_day,
		bd.country_group,
		bd.mediasource,
		bd.ad_type,
		bd.cost,
		br.tag,
		-- 根据媒体类型应用对应系数，没有系数的保持不变
		CASE
			WHEN bd.mediasource = 'applovin_int_d7' THEN 
				bd.revenue_d7 * COALESCE(br.applovin_int_d7_coeff, 1.0)
			WHEN bd.mediasource = 'applovin_int_d28' THEN 
				bd.revenue_d7 * COALESCE(br.applovin_int_d28_coeff, 1.0)
			WHEN bd.mediasource = 'Facebook Ads' THEN 
				bd.revenue_d7 * COALESCE(br.facebook_ads_coeff, 1.0)
			WHEN bd.mediasource = 'moloco_int' THEN 
				bd.revenue_d7 * COALESCE(br.moloco_int_coeff, 1.0)
			WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
				bd.revenue_d7 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
			ELSE bd.revenue_d7
		END AS fitted_revenue_d7,
		CASE
			WHEN bd.mediasource = 'applovin_int_d7' THEN 
				bd.revenue_d120 * COALESCE(br.applovin_int_d7_coeff, 1.0)
			WHEN bd.mediasource = 'applovin_int_d28' THEN 
				bd.revenue_d120 * COALESCE(br.applovin_int_d28_coeff, 1.0)
			WHEN bd.mediasource = 'Facebook Ads' THEN 
				bd.revenue_d120 * COALESCE(br.facebook_ads_coeff, 1.0)
			WHEN bd.mediasource = 'moloco_int' THEN 
				bd.revenue_d120 * COALESCE(br.moloco_int_coeff, 1.0)
			WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
				bd.revenue_d120 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
			ELSE bd.revenue_d120
		END AS fitted_revenue_d120
	FROM base_data bd
	LEFT JOIN bayesian_results br ON bd.country_group = br.country_group
),
fitted_paid_totals AS (
	-- 计算调整后的付费媒体总收入（用于计算120日自然量）
	SELECT
		app_package,
		install_day,
		country_group,
		tag,
		SUM(fitted_revenue_d7) AS total_fitted_paid_revenue_d7,
		SUM(fitted_revenue_d120) AS total_fitted_paid_revenue_d120
	FROM fitted_paid_media
	GROUP BY
		app_package,
		install_day,
		country_group,
		tag
),
organic_data AS (
	-- 生成自然量数据
	SELECT DISTINCT
		bd.app_package,
		bd.install_day,
		bd.country_group,
		'Organic' as mediasource,
		'ALL' as ad_type,
		0.0 as cost,
		br.tag,
		-- 7日自然量：直接使用预测表里的自然量收入
		br.organic_revenue as fitted_revenue_d7,
		-- 120日自然量：原始总收入 - 调整后的付费媒体总收入
		GREATEST(0, ot.total_original_revenue_d120 - COALESCE(fpt.total_fitted_paid_revenue_d120, 0)) as fitted_revenue_d120
	FROM (
		SELECT DISTINCT app_package, install_day, country_group 
		FROM base_data
	) bd
	LEFT JOIN bayesian_results br ON bd.country_group = br.country_group
	LEFT JOIN original_totals ot ON bd.app_package = ot.app_package
								 AND bd.install_day = ot.install_day
								 AND bd.country_group = ot.country_group
	LEFT JOIN fitted_paid_totals fpt ON bd.app_package = fpt.app_package
									 AND bd.install_day = fpt.install_day
									 AND bd.country_group = fpt.country_group
									 AND br.tag = fpt.tag
	WHERE br.organic_revenue IS NOT NULL
)
-- 合并付费媒体和自然量数据
SELECT
	app_package,
	install_day,
	country_group,
	mediasource,
	ad_type,
	cost,
	tag,
	fitted_revenue_d7 as revenue_d7,
	fitted_revenue_d120 as revenue_d120
FROM fitted_paid_media

UNION ALL

SELECT
	app_package,
	install_day,
	country_group,
	mediasource,
	ad_type,
	cost,
	tag,
	fitted_revenue_d7 as revenue_d7,
	fitted_revenue_d120 as revenue_d120
FROM organic_data

ORDER BY
	tag,
	app_package,
	install_day,
	country_group,
	mediasource
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return


# 将fit数据汇总，计算分国家的7日总收入
# 与原始数据的分国家7日总收入进行join
# 合成一个误差监测view
def createIosAfCostRevenueDayFitCheckTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250806_af_cohort_cost_revenue_day_fit_table_check_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250806_af_cohort_cost_revenue_day_fit_table_check_by_j AS
WITH original_data AS (
	-- 获取原始数据的分国家7日总收入
	SELECT
		app_package,
		install_day,
		country_group,
		SUM(revenue_d7) AS original_revenue_d7
	FROM lw_20250806_af_cohort_cost_revenue_day_fix_table_by_j
	GROUP BY
		app_package,
		install_day,
		country_group
),
fitted_data AS (
	-- 获取拟合数据的分国家7日总收入
	SELECT
		app_package,
		install_day,
		country_group,
		tag,
		SUM(revenue_d7) AS fitted_revenue_d7
	FROM lw_20250806_af_cohort_cost_revenue_day_fit_table_by_j
	GROUP BY
		app_package,
		install_day,
		country_group,
		tag
)
-- 合并原始数据和拟合数据
SELECT
	fd.app_package,
	fd.install_day,
	fd.country_group,
	fd.tag,
	od.original_revenue_d7,
	fd.fitted_revenue_d7
FROM fitted_data fd
LEFT JOIN original_data od ON fd.app_package = od.app_package
						   AND fd.install_day = od.install_day
						   AND fd.country_group = od.country_group
ORDER BY
	fd.tag,
	fd.app_package,
	fd.install_day,
	fd.country_group
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# 将for_ua的数据中iOS部分复制出来，加上不同的tag，用于和后续的媒体系数一起展示在界面上
def createIosTagCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_for_ua_cost_revenue_ios_month_view_by_j AS
SELECT 
	* ,
	'20250806_10' AS tag
FROM lw_20250703_ios_af_cohort_cost_revenue_app_country_group_media_month_view_by_j
WHERE app_package IN ('id6448786147','id6736925794')
UNION ALL
SELECT 
	* ,
	'20250806_20' AS tag
FROM lw_20250703_ios_af_cohort_cost_revenue_app_country_group_media_month_view_by_j
WHERE app_package IN ('id6448786147','id6736925794')
UNION ALL
SELECT 
	* ,
	'20250808_20' AS tag
FROM lw_20250703_ios_af_cohort_cost_revenue_app_country_group_media_month_view_by_j
WHERE app_package IN ('id6448786147','id6736925794')
UNION ALL
SELECT
	* ,
	'af_cohort_fix' AS tag
FROM lw_20250703_ios_af_cohort_cost_revenue_app_country_group_media_month_fix_view_by_j
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


#####################################################
# 所有的花费、收入数据汇总
# 汇总各种不同tag的CostRevenueMonthy数据，并建表
def createCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_cost_revenue_app_month_view_by_j AS
SELECT
*
FROM lw_20250703_af_cohort_cost_revenue_app_month_table_by_j
UNION ALL
SELECT
*
FROM lw_20250703_gpir_cohort_cost_revenue_app_month_table_by_j
UNION ALL
SELECT
	*
FROM lw_20250703_af_onlyprofit_cost_revenue_month_table_by_j
UNION ALL
SELECT
	*
FROM lw_20250703_gpir_onlyprofit_cost_revenue_month_table_by_j
UNION ALL
SELECT
	*
FROM lw_20250703_for_ua_cost_revenue_app_month_table_by_j
UNION ALL
SELECT
	*
FROM lw_20250703_for_ua_cost_revenue_ios_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createCostRevenueMonthyTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_cost_revenue_app_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_cost_revenue_app_month_table_by_j AS
SELECT
	case
		when app_package in ('com.fun.lastwar.gp','com.fun.lastwar.vn.gp') then 'com.fun.lastwar.gp'
		when app_package in ('id6448786147','id6736925794') then 'id6448786147'
		else app_package
	end AS app_package,
	install_month,
	country_group,
	mediasource,
	ad_type,
	sum(cost) AS cost,
	sum(revenue_d1) AS revenue_d1,
	sum(revenue_d3) AS revenue_d3,
	sum(revenue_d7) AS revenue_d7,
	sum(revenue_d14) AS revenue_d14,
	sum(revenue_d30) AS revenue_d30,
	sum(revenue_d60) AS revenue_d60,
	sum(revenue_d90) AS revenue_d90,
	sum(revenue_d120) AS revenue_d120,
	sum(revenue_d150) AS revenue_d150,
	tag
FROM lw_20250703_cost_revenue_app_month_view_by_j
GROUP BY
	case
		when app_package in ('com.fun.lastwar.gp','com.fun.lastwar.vn.gp') then 'com.fun.lastwar.gp'
		when app_package in ('id6448786147','id6736925794') then 'id6448786147'
		else app_package
	end,
	install_month,
	country_group,
	mediasource,
	ad_type,
	tag
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# 计算kpi_target

# 从lw_20250703_cost_revenue_app_month_table_by_j中
# 获取 tag = gpir_cohort 的 app_package、country_group、mediasource、ad_type、install_month、cost、revenue_d120
# 获取 tag = gpir_onlyprofit_cohort 的 app_package、country_group、mediasource、ad_type、install_month、cost as cost_p、revenue_d120 as revenue_d120_p
# 按照app_package、country_group、mediasource、ad_type、install_month 进行join
# 计算 roi_120d = revenue_d120 / cost
# 计算 roi_120d_p = revenue_d120_p / cost_p
# 按照app_package、country_group、mediasource、ad_type分组
# 并按照install_month排序，取last4、5、6 3个月的 roi_120d 平均，和 roi_120d_p 平均，记作 last456_roi_120d_avg 和 last456_roi_120d_p_avg
# 比如install_month = 202506的 应该获取的是 install_month between 202412 and 202502 的数据进行平均，即202506 - 6 ~ 202506 - 4
# 计算 kpi_target = 100% * (last456_roi_120d_avg / last456_roi_120d_p_avg)
# 最终输出列： app_package、country_group、mediasource、ad_type、install_month、cost、revenue_d120、cost_p、revenue_d120_p、roi_120d、roi_120d_p、last456_roi_120d_avg、last456_roi_120d_p_avg、kpi_target
def createGpirCohortKpiTargetView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_cohort_kpi_target_month_view_by_j AS
WITH gpir_data AS (
	SELECT
		app_package,
		country_group,
		mediasource,
		ad_type,
		install_month,
		cost,
		revenue_d120
	FROM
		lw_20250703_cost_revenue_app_month_table_by_j
	WHERE
		tag = 'gpir_cohort'
),
gpir_onlyprofit_data AS (
	SELECT
		app_package,
		country_group,
		mediasource,
		ad_type,
		install_month,
		cost AS cost_p,
		revenue_d120 AS revenue_d120_p
	FROM
		lw_20250703_cost_revenue_app_month_table_by_j
	WHERE
		tag = 'gpir_onlyprofit_cohort'
),
joined_data AS (
	SELECT
		g.app_package,
		g.country_group,
		g.mediasource,
		g.ad_type,
		g.install_month,
		g.cost,
		g.revenue_d120,
		gp.cost_p,
		gp.revenue_d120_p
	FROM
		gpir_data g
		JOIN gpir_onlyprofit_data gp ON g.app_package = gp.app_package
		AND g.country_group = gp.country_group
		AND g.mediasource = gp.mediasource
		AND g.ad_type = gp.ad_type
		AND g.install_month = gp.install_month
),
-- 1. 按原分组获取最近1、2、3个月的花费平均值
cost_lag_data AS (
	SELECT
		app_package,
		country_group,
		mediasource,
		ad_type,
		install_month,
		cost,
		revenue_d120,
		cost_p,
		revenue_d120_p,
		(
			COALESCE(
				LAG(cost, 1) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost, 2) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost, 3) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			)
		) / 3 AS last123_cost_avg,
		(
			COALESCE(
				LAG(cost_p, 1) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost_p, 2) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost_p, 3) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			)
		) / 3 AS last123_cost_p_avg
	FROM
		joined_data
),
-- 2. 只按app_package、country_group分组获取最近4、5、6个月的收入平均值
revenue_agg_data AS (
	SELECT
		app_package,
		country_group,
		install_month,
		SUM(revenue_d120) AS total_revenue_d120,
		SUM(revenue_d120_p) AS total_revenue_d120_p
	FROM
		joined_data
	GROUP BY
		app_package,
		country_group,
		install_month
),
revenue_lag_data AS (
	SELECT
		app_package,
		country_group,
		install_month,
		total_revenue_d120,
		total_revenue_d120_p,
		(
			COALESCE(
				LAG(total_revenue_d120, 4) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120, 5) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120, 6) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			)
		) / 3 AS last456_revenue_d120_avg,
		(
			COALESCE(
				LAG(total_revenue_d120_p, 4) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120_p, 5) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120_p, 6) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			)
		) / 3 AS last456_revenue_d120_p_avg
	FROM
		revenue_agg_data
),
-- 3. 将花费和收入数据join在一起
final_joined AS (
	SELECT
		c.app_package,
		c.country_group,
		c.mediasource,
		c.ad_type,
		c.install_month,
		c.cost,
		c.revenue_d120,
		r.total_revenue_d120,
		c.cost_p,
		c.revenue_d120_p,
		r.total_revenue_d120_p,
		c.last123_cost_avg,
		c.last123_cost_p_avg,
		r.last456_revenue_d120_avg,
		r.last456_revenue_d120_p_avg
	FROM
		cost_lag_data c
		LEFT JOIN revenue_lag_data r ON c.app_package = r.app_package
		AND c.country_group = r.country_group
		AND c.install_month = r.install_month
)
-- 4. 计算ROI和KPI目标值
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	cost,
	revenue_d120,
	total_revenue_d120,
	cost_p,
	revenue_d120_p,
	total_revenue_d120_p,
	last123_cost_avg,
	last123_cost_p_avg,
	last456_revenue_d120_avg,
	last456_revenue_d120_p_avg,
	-- 计算基于历史数据的ROI
	CASE
		WHEN last123_cost_avg > 0 THEN last456_revenue_d120_avg / last123_cost_avg
		ELSE NULL
	END AS roi_120d,
	CASE
		WHEN last123_cost_p_avg > 0 THEN last456_revenue_d120_p_avg / last123_cost_p_avg
		ELSE NULL
	END AS roi_120d_p,
	-- 计算KPI目标值
	CASE
		WHEN last123_cost_avg > 0 AND last456_revenue_d120_p_avg > 0 AND last123_cost_p_avg > 0
		THEN (last456_revenue_d120_avg / last123_cost_avg) / (last456_revenue_d120_p_avg / last123_cost_p_avg)
		ELSE NULL
	END AS kpi_target
FROM
	final_joined
ORDER BY
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


# 计算iOS的kpi_target
# app_package = 'id6448786147'
# iOS 没有GPIR，使用tag af_cohort 和 af_onlyprofit_cohort
# 其他逻辑与输出结果与lw_20250703_gpir_cohort_kpi_target_month_view_by_j相同
def createIosCohortKpiTargetView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_ios_cohort_kpi_target_month_view_by_j AS
WITH af_cohort_data AS (
	SELECT
		app_package,
		country_group,
		mediasource,
		ad_type,
		install_month,
		cost,
		revenue_d120
	FROM
		lw_20250703_cost_revenue_app_month_table_by_j
	WHERE
		tag = 'af_cohort'
		AND app_package = 'id6448786147'
),
af_onlyprofit_cohort_data AS (
	SELECT
		app_package,
		country_group,
		mediasource,
		ad_type,
		install_month,
		cost AS cost_p,
		revenue_d120 AS revenue_d120_p
	FROM
		lw_20250703_cost_revenue_app_month_table_by_j
	WHERE
		tag = 'af_onlyprofit_cohort'
		AND app_package = 'id6448786147'
),
joined_data AS (
	SELECT
		a.app_package,
		a.country_group,
		a.mediasource,
		a.ad_type,
		a.install_month,
		a.cost,
		a.revenue_d120,
		ap.cost_p,
		ap.revenue_d120_p
	FROM
		af_cohort_data a
		JOIN af_onlyprofit_cohort_data ap ON a.app_package = ap.app_package
		AND a.country_group = ap.country_group
		AND a.mediasource = ap.mediasource
		AND a.ad_type = ap.ad_type
		AND a.install_month = ap.install_month
),
-- 1. 按原分组获取最近1、2、3个月的花费平均值
cost_lag_data AS (
	SELECT
		app_package,
		country_group,
		mediasource,
		ad_type,
		install_month,
		cost,
		revenue_d120,
		cost_p,
		revenue_d120_p,
		(
			COALESCE(
				LAG(cost, 1) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost, 2) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost, 3) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			)
		) / 3 AS last123_cost_avg,
		(
			COALESCE(
				LAG(cost_p, 1) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost_p, 2) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(cost_p, 3) OVER (
					PARTITION BY app_package, country_group, mediasource, ad_type
					ORDER BY install_month
				), 0
			)
		) / 3 AS last123_cost_p_avg
	FROM
		joined_data
),
-- 2. 只按app_package、country_group分组获取最近4、5、6个月的收入平均值
revenue_agg_data AS (
	SELECT
		app_package,
		country_group,
		install_month,
		SUM(revenue_d120) AS total_revenue_d120,
		SUM(revenue_d120_p) AS total_revenue_d120_p
	FROM
		joined_data
	GROUP BY
		app_package,
		country_group,
		install_month
),
revenue_lag_data AS (
	SELECT
		app_package,
		country_group,
		install_month,
		total_revenue_d120,
		total_revenue_d120_p,
		(
			COALESCE(
				LAG(total_revenue_d120, 4) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120, 5) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120, 6) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			)
		) / 3 AS last456_revenue_d120_avg,
		(
			COALESCE(
				LAG(total_revenue_d120_p, 4) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120_p, 5) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			) + COALESCE(
				LAG(total_revenue_d120_p, 6) OVER (
					PARTITION BY app_package, country_group
					ORDER BY install_month
				), 0
			)
		) / 3 AS last456_revenue_d120_p_avg
	FROM
		revenue_agg_data
),
-- 3. 将花费和收入数据join在一起
final_joined AS (
	SELECT
		c.app_package,
		c.country_group,
		c.mediasource,
		c.ad_type,
		c.install_month,
		c.cost,
		c.revenue_d120,
		r.total_revenue_d120,
		c.cost_p,
		c.revenue_d120_p,
		r.total_revenue_d120_p,
		c.last123_cost_avg,
		c.last123_cost_p_avg,
		r.last456_revenue_d120_avg,
		r.last456_revenue_d120_p_avg
	FROM
		cost_lag_data c
		LEFT JOIN revenue_lag_data r ON c.app_package = r.app_package
		AND c.country_group = r.country_group
		AND c.install_month = r.install_month
)
-- 4. 计算ROI和KPI目标值
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	cost,
	revenue_d120,
	total_revenue_d120,
	cost_p,
	revenue_d120_p,
	total_revenue_d120_p,
	last123_cost_avg,
	last123_cost_p_avg,
	last456_revenue_d120_avg,
	last456_revenue_d120_p_avg,
	-- 计算基于历史数据的ROI
	CASE
		WHEN last123_cost_avg > 0 THEN last456_revenue_d120_avg / last123_cost_avg
		ELSE NULL
	END AS roi_120d,
	CASE
		WHEN last123_cost_p_avg > 0 THEN last456_revenue_d120_p_avg / last123_cost_p_avg
		ELSE NULL
	END AS roi_120d_p,
	-- 计算KPI目标值
	CASE
		WHEN last123_cost_avg > 0 AND last123_cost_p_avg > 0 AND last456_revenue_d120_p_avg > 0 
		THEN (last456_revenue_d120_avg / last123_cost_avg) / (last456_revenue_d120_p_avg / last123_cost_p_avg)
		ELSE NULL
	END AS kpi_target
FROM
	final_joined
ORDER BY
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return



# 计算iOS的kpi_target 针对applovin的7d 和 28d 兼容
def createIosCohortKpiTargetView2():
	sql = f"""
CREATE OR REPLACE VIEW lw_20250703_ios_cohort_kpi_target_month_view2_by_j AS
SELECT 
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target
FROM lw_20250703_ios_cohort_kpi_target_month_view_by_j
UNION ALL
-- 复制applovin_int行并改为applovin_7d
SELECT 
	app_package,
	country_group,
	'applovin_int_d7' AS mediasource,
	ad_type,
	install_month,
	kpi_target
FROM lw_20250703_ios_cohort_kpi_target_month_view_by_j
WHERE mediasource = 'applovin_int'
UNION ALL
-- 复制applovin_int行并改为applovin_28d
SELECT 
	app_package,
	country_group,
	'applovin_int_d28' AS mediasource,
	ad_type,
	install_month,
	kpi_target
FROM lw_20250703_ios_cohort_kpi_target_month_view_by_j
WHERE mediasource = 'applovin_int'
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


def createForUaKpiTargetView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_for_ua_kpi_target_month_view_by_j AS
SELECT *
FROM lw_20250703_gpir_cohort_kpi_target_month_view_by_j

UNION ALL

-- 复制applovin_int行并改为applovin_7d
SELECT 
	app_package,
	country_group,
	'applovin_int_d7' AS mediasource,
	ad_type,
	install_month,
	cost,
	revenue_d120,
	total_revenue_d120,
	cost_p,
	revenue_d120_p,
	total_revenue_d120_p,
	last123_cost_avg,
	last123_cost_p_avg,
	last456_revenue_d120_avg,
	last456_revenue_d120_p_avg,
	roi_120d,
	roi_120d_p,
	kpi_target
FROM lw_20250703_gpir_cohort_kpi_target_month_view_by_j
WHERE mediasource = 'applovin_int'

UNION ALL

-- 复制applovin_int行并改为applovin_28d
SELECT 
	app_package,
	country_group,
	'applovin_int_d28' AS mediasource,
	ad_type,
	install_month,
	cost,
	revenue_d120,
	total_revenue_d120,
	cost_p,
	revenue_d120_p,
	total_revenue_d120_p,
	last123_cost_avg,
	last123_cost_p_avg,
	last456_revenue_d120_avg,
	last456_revenue_d120_p_avg,
	roi_120d,
	roi_120d_p,
	kpi_target
FROM lw_20250703_gpir_cohort_kpi_target_month_view_by_j
WHERE mediasource = 'applovin_int'
ORDER BY 
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month;
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createKpiTargetTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_kpi_target_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_kpi_target_month_table_by_j AS
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target,
	'gpir_cohort' AS tag
FROM lw_20250703_gpir_cohort_kpi_target_month_view_by_j
UNION ALL
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target,
	'af_cohort' AS tag
FROM lw_20250703_ios_cohort_kpi_target_month_view_by_j
UNION ALL
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target,
	'af_cohort_fix' AS tag
FROM lw_20250703_ios_cohort_kpi_target_month_view2_by_j
UNION ALL
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target,
	'20250806_10' AS tag
FROM lw_20250703_ios_cohort_kpi_target_month_view2_by_j

UNION ALL
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target,
	'20250806_20' AS tag
FROM lw_20250703_ios_cohort_kpi_target_month_view2_by_j

UNION ALL
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target,
	'20250808_20' AS tag
FROM lw_20250703_ios_cohort_kpi_target_month_view2_by_j

UNION ALL
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month,
	kpi_target,
	'for_ua' AS tag
FROM lw_20250703_for_ua_kpi_target_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

#####################################################
# 计算收入增长率

def createRevenueRiseRatioView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_revenue_rise_ratio_month_view_by_j AS
WITH ratios AS (
	SELECT
		app_package,
		install_month,
		country_group,
		mediasource,
		ad_type,
		tag,
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
			ELSE revenue_d14 / revenue_d7
		END AS r14_r7,
		CASE
			WHEN revenue_d14 = 0 THEN 0
			ELSE revenue_d30 / revenue_d14
		END AS r30_r14,
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
		END AS r120_r90,
		CASE
			WHEN revenue_d120 = 0 THEN 0
			ELSE revenue_d150 / revenue_d120
		END AS r150_r120
	FROM
		lw_20250703_cost_revenue_app_month_table_by_j
),
ratios_with_rownum AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY app_package,
			country_group,
			mediasource,
			ad_type,
			tag
			ORDER BY
				install_month
		) AS row_num
	FROM
		ratios
),
last3month_ratios AS (
	SELECT
		cur.app_package,
		cur.install_month,
		cur.country_group,
		cur.mediasource,
		cur.ad_type,
		cur.tag,
		cur.r3_r1,
		cur.r7_r3,
		cur.r14_r7,
		cur.r30_r14,
		cur.r30_r7,
		cur.r60_r30,
		cur.r90_r60,
		cur.r120_r90,
		cur.r150_r120,
		-- 计算平均值时排除0值
		COALESCE(
			AVG(
				CASE
					WHEN prev.r3_r1 <> 0 THEN prev.r3_r1
				END
			),
			0
		) AS last3month_r3_r1,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r7_r3 <> 0 THEN prev.r7_r3
				END
			),
			0
		) AS last3month_r7_r3,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r14_r7 <> 0 THEN prev.r14_r7
				END
			),
			0
		) AS last3month_r14_r7,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r30_r14 <> 0 THEN prev.r30_r14
				END
			),
			0
		) AS last3month_r30_r14,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r30_r7 <> 0 THEN prev.r30_r7
				END
			),
			0
		) AS last3month_r30_r7,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r60_r30 <> 0 THEN prev.r60_r30
				END
			),
			0
		) AS last3month_r60_r30,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r90_r60 <> 0 THEN prev.r90_r60
				END
			),
			0
		) AS last3month_r90_r60,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r120_r90 <> 0 THEN prev.r120_r90
				END
			),
			0
		) AS last3month_r120_r90,
		COALESCE(
			AVG(
				CASE
					WHEN prev.r150_r120 <> 0 THEN prev.r150_r120
				END
			),
			0
		) AS last3month_r150_r120
	FROM
		ratios_with_rownum cur
		LEFT JOIN ratios_with_rownum prev ON 
		cur.app_package = prev.app_package
		AND cur.country_group = prev.country_group
		AND cur.mediasource = prev.mediasource
		AND cur.ad_type = prev.ad_type
		AND cur.tag = prev.tag
		AND prev.row_num BETWEEN cur.row_num - 3
		AND cur.row_num - 1
	GROUP BY
		cur.app_package,
		cur.install_month,
		cur.country_group,
		cur.mediasource,
		cur.ad_type,
		cur.tag,
		cur.r3_r1,
		cur.r7_r3,
		cur.r14_r7,
		cur.r30_r14,
		cur.r30_r7,
		cur.r60_r30,
		cur.r90_r60,
		cur.r120_r90,
		cur.r150_r120
)
SELECT
	app_package,
	install_month,
	country_group,
	mediasource,
	ad_type,
	tag,
	r3_r1,
	r7_r3,
	r14_r7,
	r30_r14,
	r30_r7,
	r60_r30,
	r90_r60,
	r120_r90,
	r150_r120,
	last3month_r3_r1,
	last3month_r7_r3,
	last3month_r14_r7,
	last3month_r30_r14,
	last3month_r30_r7,
	last3month_r60_r30,
	last3month_r90_r60,
	last3month_r120_r90,
	last3month_r150_r120
FROM
	last3month_ratios
ORDER BY
	tag,
	app_package,
	country_group,
	mediasource,
	ad_type,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 由于满日数据问题，当月数据不完整，需要使用之前数据完成预测。
def createPredictRevenueRiseRatioView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j AS
WITH base AS (
	SELECT
		app_package,
		country_group,
		mediasource,
		ad_type,
		tag,
		install_month,
		r3_r1,
		r7_r3,
		r14_r7,
		r30_r14,
		r30_r7,
		r60_r30,
		r90_r60,
		r120_r90,
		r150_r120,
		last3month_r3_r1,
		last3month_r7_r3,
		last3month_r14_r7,
		last3month_r30_r14,
		last3month_r30_r7,
		last3month_r60_r30,
		last3month_r90_r60,
		last3month_r120_r90,
		last3month_r150_r120,
		ROW_NUMBER() OVER (
			PARTITION BY 
			app_package,
			country_group,
			mediasource,
			ad_type,
			tag
			ORDER BY
				install_month
		) AS row_num
	FROM
		lw_20250703_af_revenue_rise_ratio_month_view_by_j
)
SELECT
	cur.app_package,
	cur.country_group,
	cur.mediasource,
	cur.ad_type,
	cur.tag,
	cur.install_month,
	cur.r3_r1,
	cur.r7_r3,
	cur.r14_r7,
	cur.r30_r14,
	cur.r30_r7,
	cur.r60_r30,
	cur.r90_r60,
	cur.r120_r90,
	cur.r150_r120,
	cur.last3month_r3_r1,
	cur.last3month_r7_r3,
	cur.last3month_r14_r7,
	cur.last3month_r30_r14,
	cur.last3month_r30_r7,
	cur.last3month_r60_r30,
	cur.last3month_r90_r60,
	cur.last3month_r120_r90,
	cur.last3month_r150_r120,
	-- 本行的预测值
	cur.last3month_r3_r1 AS predict_r3_r1,
	cur.last3month_r7_r3 AS predict_r7_r3,
	cur.last3month_r30_r7 AS predict_r30_r7,
	cur.last3month_r14_r7 AS predict_r14_r7,
	cur.last3month_r30_r14 AS predict_r30_r14,
	-- 上一行的预测值
	COALESCE(prev1.last3month_r60_r30, 0) AS predict_r60_r30,
	-- 上两行的预测值
	COALESCE(prev2.last3month_r90_r60, 0) AS predict_r90_r60,
	-- 上三行的预测值
	COALESCE(prev3.last3month_r120_r90, 0) AS predict_r120_r90,
	-- 上四行的预测值
	COALESCE(prev4.last3month_r150_r120, 0) AS predict_r150_r120
FROM
	base cur
	LEFT JOIN base prev1 ON 
	cur.app_package = prev1.app_package
	AND cur.country_group = prev1.country_group
	AND cur.mediasource = prev1.mediasource
	AND cur.ad_type = prev1.ad_type
	AND cur.tag = prev1.tag
	AND cur.row_num = prev1.row_num + 1
	LEFT JOIN base prev2 ON 
	cur.app_package = prev2.app_package
	AND cur.country_group = prev2.country_group
	AND cur.mediasource = prev2.mediasource
	AND cur.ad_type = prev2.ad_type
	AND cur.tag = prev2.tag
	AND cur.row_num = prev2.row_num + 2
	LEFT JOIN base prev3 ON 
	cur.app_package = prev3.app_package
	AND cur.country_group = prev3.country_group
	AND cur.mediasource = prev3.mediasource
	AND cur.ad_type = prev3.ad_type
	AND cur.tag = prev3.tag
	AND cur.row_num = prev3.row_num + 3
	LEFT JOIN base prev4 ON
	cur.app_package = prev4.app_package
	AND cur.country_group = prev4.country_group
	AND cur.mediasource = prev4.mediasource
	AND cur.ad_type = prev4.ad_type
	AND cur.tag = prev4.tag
	AND cur.row_num = prev4.row_num + 4
ORDER BY
	cur.tag,
	cur.app_package,
	cur.country_group,
	cur.mediasource,
	cur.ad_type,
	cur.install_month;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createPredictRevenueRiseRatioTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_af_revenue_rise_ratio_predict_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_af_revenue_rise_ratio_predict_month_table_by_j AS
SELECT 
	rr.app_package,
	rr.country_group,
	rr.mediasource,
	rr.ad_type,
	rr.tag,
	rr.install_month,
	rr.r3_r1,
	rr.r7_r3,
	rr.r14_r7,
	case when m.month_diff >= 1 then rr.r30_r14 
	else NULL end
	as r30_r14,
	case when m.month_diff >= 1 then rr.r30_r7
	else NULL end
	as r30_r7,
	case when m.month_diff >= 2 then rr.r60_r30
	else NULL end
	as r60_r30,
	case when m.month_diff >= 3 then rr.r90_r60
	else NULL end
	as r90_r60,
	case when m.month_diff >= 4 then rr.r120_r90
	else NULL end
	as r120_r90,
	case when m.month_diff >= 5 then rr.r150_r120
	else NULL end
	as r150_r120,
	rr.last3month_r3_r1,
	rr.last3month_r7_r3,
	rr.last3month_r14_r7,
	rr.last3month_r30_r14,
	rr.last3month_r30_r7,
	rr.last3month_r60_r30,
	rr.last3month_r90_r60,
	rr.last3month_r120_r90,
	rr.last3month_r150_r120,
	rr.predict_r3_r1,
	rr.predict_r7_r3,
	rr.predict_r30_r7,
	rr.predict_r14_r7,
	rr.predict_r30_r14,
	rr.predict_r60_r30,
	rr.predict_r90_r60,
	rr.predict_r120_r90,
	rr.predict_r150_r120
FROM lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j rr
LEFT JOIN month_view_by_j m ON rr.install_month = m.install_month
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return


# 合并lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j和lw_20250703_kpi_target_month_table_by_j
def createPredictRevenueRiseRatioAndkpiTargetView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_revenue_rise_ratio_predict_kpi_target_month_view_by_j AS
SELECT
	rr.*,
	COALESCE(
		kt.kpi_target, 
		-- 如果动态KPI不存在，使用原来的写死逻辑
		CASE
			WHEN rr.tag LIKE '%onlyprofit%' THEN 1.00
			ELSE 
				CASE
					WHEN rr.country_group = 'US' THEN 1.45
					WHEN rr.country_group = 'KR' THEN 1.58
					WHEN rr.country_group = 'JP' THEN 1.66
					WHEN rr.country_group = 'GCC' THEN 1.45
					WHEN rr.country_group = 'T1' THEN 1.65
					WHEN rr.country_group = 'other' THEN 1.65
					ELSE 1.43
				END
		END
	) AS kpi_target
FROM lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j rr
LEFT JOIN lw_20250703_kpi_target_month_table_by_j kt ON
	rr.app_package = kt.app_package
	AND rr.country_group = kt.country_group
	AND rr.mediasource = kt.mediasource
	AND rr.ad_type = kt.ad_type
	AND rr.install_month = kt.install_month
	AND rr.tag = kt.tag
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

#####################################################
# 推算KPI

# 其中回本是按照佳玥给出的表格进行计算的
def createKpiView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_kpi_month_view_by_j AS
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	tag,
	install_month,
	kpi_target,
	CASE
		WHEN (
			predict_r3_r1 * predict_r7_r3 * predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
		) = 0 THEN NULL
		ELSE ROUND(
			kpi_target / (
				predict_r3_r1 * predict_r7_r3 * predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
			),
			4
		)
	END AS kpi1,
	CASE
		WHEN (
			predict_r7_r3 * predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
		) = 0 THEN NULL
		ELSE ROUND(
			kpi_target / (
				predict_r7_r3 * predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
			),
			4
		)
	END AS kpi3,
	CASE
		WHEN (
			predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
		) = 0 THEN NULL
		ELSE ROUND(
			kpi_target / (
				predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
			),
			4
		)
	END AS kpi7
FROM
	(
		SELECT
			*
		FROM lw_20250703_af_revenue_rise_ratio_predict_kpi_target_month_view_by_j
	) t
ORDER BY
	app_package,
	country_group,
	mediasource,
	ad_type,
	tag,
	install_month;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 针对applovin的kpi进行折扣，解决applovin gpir 部分用户不能归因。
# 20250804添加，与lw_20250703_ios_bayesian_result_by_j join
# 然后针对lw_20250703_ios_bayesian_result_by_j中涉及到的媒体进行kpi折扣
# applovin_int_d7、applovin_int_d28、Facebook Ads、moloco_int、bytedanceglobal_int 的kpi除以对应系数如果join到系数就除以系数，否则除以1或者保持不变）
def createKpiWithDiscountView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_kpi_month_with_discount_view_by_j AS
SELECT
	k.app_package,
	k.country_group,
	k.mediasource,
	k.ad_type,
	k.tag,
	k.install_month,
	k.kpi_target,
	CASE
		-- 原有逻辑：applovin折扣
		WHEN k.mediasource IN ('applovin_int_d7', 'applovin_int_d28') 
			 AND k.tag = 'for_ua' 
			 AND r.applovin_int_other_r1_ratio IS NOT NULL
		THEN ROUND(k.kpi1 * (1 - r.applovin_int_other_r1_ratio), 4)
		-- 20250804新增：iOS媒体系数折扣
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'applovin_int_d7' 
			 AND b.applovin_int_d7_coeff IS NOT NULL
		THEN ROUND(k.kpi1 / b.applovin_int_d7_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'applovin_int_d28' 
			 AND b.applovin_int_d28_coeff IS NOT NULL
		THEN ROUND(k.kpi1 / b.applovin_int_d28_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'Facebook Ads' 
			 AND b.facebook_ads_coeff IS NOT NULL
		THEN ROUND(k.kpi1 / b.facebook_ads_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'moloco_int' 
			 AND b.moloco_int_coeff IS NOT NULL
		THEN ROUND(k.kpi1 / b.moloco_int_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'bytedanceglobal_int' 
			 AND b.bytedanceglobal_int_coeff IS NOT NULL
		THEN ROUND(k.kpi1 / b.bytedanceglobal_int_coeff, 4)
		ELSE k.kpi1
	END AS kpi1,
	CASE
		-- 原有逻辑：applovin折扣
		WHEN k.mediasource IN ('applovin_int_d7', 'applovin_int_d28') 
			 AND k.tag = 'for_ua' 
			 AND r.applovin_int_other_r3_ratio IS NOT NULL
		THEN ROUND(k.kpi3 * (1 - r.applovin_int_other_r3_ratio), 4)
		-- 20250804新增：iOS媒体系数折扣
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'applovin_int_d7' 
			 AND b.applovin_int_d7_coeff IS NOT NULL
		THEN ROUND(k.kpi3 / b.applovin_int_d7_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'applovin_int_d28' 
			 AND b.applovin_int_d28_coeff IS NOT NULL
		THEN ROUND(k.kpi3 / b.applovin_int_d28_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'Facebook Ads' 
			 AND b.facebook_ads_coeff IS NOT NULL
		THEN ROUND(k.kpi3 / b.facebook_ads_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'moloco_int' 
			 AND b.moloco_int_coeff IS NOT NULL
		THEN ROUND(k.kpi3 / b.moloco_int_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'bytedanceglobal_int' 
			 AND b.bytedanceglobal_int_coeff IS NOT NULL
		THEN ROUND(k.kpi3 / b.bytedanceglobal_int_coeff, 4)
		ELSE k.kpi3
	END AS kpi3,
	CASE
		-- 原有逻辑：applovin折扣
		WHEN k.mediasource IN ('applovin_int_d7', 'applovin_int_d28') 
			 AND k.tag = 'for_ua' 
			 AND r.applovin_int_other_r7_ratio IS NOT NULL
		THEN ROUND(k.kpi7 * (1 - r.applovin_int_other_r7_ratio), 4)
		-- 20250804新增：iOS媒体系数折扣
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'applovin_int_d7' 
			 AND b.applovin_int_d7_coeff IS NOT NULL
		THEN ROUND(k.kpi7 / b.applovin_int_d7_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'applovin_int_d28' 
			 AND b.applovin_int_d28_coeff IS NOT NULL
		THEN ROUND(k.kpi7 / b.applovin_int_d28_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'Facebook Ads' 
			 AND b.facebook_ads_coeff IS NOT NULL
		THEN ROUND(k.kpi7 / b.facebook_ads_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'moloco_int' 
			 AND b.moloco_int_coeff IS NOT NULL
		THEN ROUND(k.kpi7 / b.moloco_int_coeff, 4)
		WHEN k.app_package = 'id6448786147' 
			 AND k.tag IN ('20250806_10', '20250806_20', '20250808_20')
			 AND k.mediasource = 'bytedanceglobal_int' 
			 AND b.bytedanceglobal_int_coeff IS NOT NULL
		THEN ROUND(k.kpi7 / b.bytedanceglobal_int_coeff, 4)
		ELSE k.kpi7
	END AS kpi7
FROM
	lw_20250703_af_kpi_month_view_by_j k
	LEFT JOIN lw_20250703_applovin_ratio_view_by_j r 
		ON k.app_package = r.app_package 
		AND k.install_month = r.install_month 
		AND k.country_group = r.country_group
	LEFT JOIN lw_20250703_ios_bayesian_result_by_j b
		ON k.country_group = b.country_group
		AND k.tag = b.tag
ORDER BY
	k.app_package,
	k.country_group,
	k.mediasource,
	k.ad_type,
	k.tag,
	k.install_month;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createKpiTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_af_kpi_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_af_kpi_month_table_by_j AS
SELECT * FROM lw_20250703_af_kpi_month_with_discount_view_by_j;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

####################################################
# 推算动态KPI

# 动态KPI，是根据30日、60日、90日的ROI来计算的
def createKpi2View():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_kpi2_month_view_by_j AS 
WITH roi_base AS (
	SELECT
		app_package,
		install_month,
		country_group,
		mediasource,
		ad_type,
		tag,
		cost,
		revenue_d1,
		revenue_d3,
		revenue_d7,
		revenue_d30,
		revenue_d60,
		revenue_d90,
		revenue_d120,
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
		END AS roi120
	FROM
		lw_20250703_cost_revenue_app_month_table_by_j
),
predict_base AS (
	SELECT
		*
	FROM lw_20250703_af_revenue_rise_ratio_predict_kpi_target_month_view_by_j
)
SELECT
	r.app_package,
	r.install_month,
	r.country_group,
	r.mediasource,
	r.ad_type,
	r.tag,
	p.kpi_target,
	-- kpi_7
	CASE
		WHEN r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi1 * (
				p.kpi_target / (
					r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90
				)
			),
			4
		)
	END AS kpi1_7,
	CASE
		WHEN r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi3 * (
				p.kpi_target / (
					r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90
				)
			),
			4
		)
	END AS kpi3_7,
	CASE
		WHEN r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi7 * (
				p.kpi_target / (
					r.roi7 * p.predict_r30_r7 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90
				)
			),
			4
		)
	END AS kpi7_7,
	-- kpi_30
	CASE
		WHEN r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi1 * (
				p.kpi_target / (
					r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90
				)
			),
			4
		)
	END AS kpi1_30,
	CASE
		WHEN r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi3 * (
				p.kpi_target / (
					r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90
				)
			),
			4
		)
	END AS kpi3_30,
	CASE
		WHEN r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi7 * (
				p.kpi_target / (
					r.roi30 * p.predict_r60_r30 * p.predict_r90_r60 * p.predict_r120_r90
				)
			),
			4
		)
	END AS kpi7_30,
	-- kpi_60
	CASE
		WHEN r.roi60 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi1 * (
				p.kpi_target / (r.roi60 * p.predict_r90_r60 * p.predict_r120_r90)
			),
			4
		)
	END AS kpi1_60,
	CASE
		WHEN r.roi60 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi3 * (
				p.kpi_target / (r.roi60 * p.predict_r90_r60 * p.predict_r120_r90)
			),
			4
		)
	END AS kpi3_60,
	CASE
		WHEN r.roi60 * p.predict_r90_r60 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi7 * (
				p.kpi_target / (r.roi60 * p.predict_r90_r60 * p.predict_r120_r90)
			),
			4
		)
	END AS kpi7_60,
	-- kpi_90
	CASE
		WHEN r.roi90 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi1 * (p.kpi_target / (r.roi90 * p.predict_r120_r90)),
			4
		)
	END AS kpi1_90,
	CASE
		WHEN r.roi90 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi3 * (p.kpi_target / (r.roi90 * p.predict_r120_r90)),
			4
		)
	END AS kpi3_90,
	CASE
		WHEN r.roi90 * p.predict_r120_r90 = 0 THEN NULL
		ELSE ROUND(
			r.roi7 * (p.kpi_target / (r.roi90 * p.predict_r120_r90)),
			4
		)
	END AS kpi7_90,
	-- 【新增】kpi_120（直接用120日ROI与kpi_target做比较）
	CASE
		WHEN r.roi120 = 0 THEN NULL
		ELSE ROUND(r.roi1 * (p.kpi_target / r.roi120), 4)
	END AS kpi1_120,
	CASE
		WHEN r.roi120 = 0 THEN NULL
		ELSE ROUND(r.roi3 * (p.kpi_target / r.roi120), 4)
	END AS kpi3_120,
	CASE
		WHEN r.roi120 = 0 THEN NULL
		ELSE ROUND(r.roi7 * (p.kpi_target / r.roi120), 4)
	END AS kpi7_120
FROM
	roi_base r
	LEFT JOIN predict_base p ON 
	r.app_package = p.app_package
	AND r.install_month = p.install_month
	AND r.country_group = p.country_group
	AND r.mediasource = p.mediasource
	AND r.ad_type = p.ad_type
	AND r.tag = p.tag
ORDER BY
	r.tag,
	r.app_package,
	r.country_group,
	r.mediasource,
	r.ad_type,
	r.install_month,
	p.kpi_target
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 动态KPI，修正，将不完整数据的月份过滤掉
# 这里的逻辑是基于 lw_kpi_country_group_month_view_by_j 视图进行修正
def createKpi2ViewFix():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_kpi2_fix_month_view_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	CASE
		WHEN b.month_diff >= 5 THEN a.kpi1_120
		WHEN b.month_diff = 4 THEN a.kpi1_90
		WHEN b.month_diff = 3 THEN a.kpi1_60
		WHEN b.month_diff = 2 THEN a.kpi1_30
		WHEN b.month_diff = 1 THEN a.kpi1_7
		ELSE NULL
	END AS d_kpi1,
	CASE
		WHEN b.month_diff >= 5 THEN a.kpi3_120
		WHEN b.month_diff = 4 THEN a.kpi3_90
		WHEN b.month_diff = 3 THEN a.kpi3_60
		WHEN b.month_diff = 2 THEN a.kpi3_30
		WHEN b.month_diff = 1 THEN a.kpi3_7
		ELSE NULL
	END AS d_kpi3,
	CASE
		WHEN b.month_diff >= 5 THEN a.kpi7_120
		WHEN b.month_diff = 4 THEN a.kpi7_90
		WHEN b.month_diff = 3 THEN a.kpi7_60
		WHEN b.month_diff = 2 THEN a.kpi7_30
		WHEN b.month_diff = 1 THEN a.kpi7_7
		ELSE NULL
	END AS d_kpi7
FROM
	lw_20250703_af_kpi2_month_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month
ORDER BY
	a.tag,
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createKpi2FixTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_af_kpi2_fix_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_af_kpi2_fix_month_table_by_j AS
SELECT * FROM lw_20250703_af_kpi2_fix_month_view_by_j;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

###############################################
# 自然量占比，重构版本

# af版本的自然量收入占比，120日版本 - 重构版
def createAfAndroidOrganicMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_android_organic_revenue_ratio_month_view_by_j AS
SELECT
	'com.fun.lastwar.gp' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'af' as tag,
	SUM(
		CASE
			WHEN mediasource = 'Organic' THEN revenue_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_d120) AS revenue_d120
FROM
	marketing.attribution.dws_overseas_public_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.app = '502'
	AND roi.app_package in ('com.fun.lastwar.gp', 'com.fun.lastwar.vn.gp')
	AND roi.facebook_segment IN ('country', 'N/A')
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# af_cohort版本的自然量收入占比，120日版本 - 重构版
def createAfCohortAndroidOrganicMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cohort_android_organic_revenue_ratio_month_view_by_j AS
SELECT
	'com.fun.lastwar.gp' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'af_cohort' as tag,
	SUM(
		CASE
			WHEN mediasource = 'Organic' THEN revenue_cohort_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_cohort_d120) AS revenue_d120
FROM
	marketing.attribution.dws_overseas_public_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.app = '502'
	AND roi.app_package in ('com.fun.lastwar.gp', 'com.fun.lastwar.vn.gp')
	AND roi.facebook_segment IN ('country', 'N/A')
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createAfOnlyprofitCohortAndroidOrganicMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_onlyprofit_cohort_android_organic_revenue_ratio_month_view_by_j AS
SELECT
	'com.fun.lastwar.gp' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'af_onlyprofit_cohort' as tag,
	SUM(
		CASE
			WHEN mediasource = 'Organic' THEN revenue_cohort_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_cohort_d120) AS revenue_d120
FROM
	marketing.attribution.dws_overseas_roi_profit roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.app_package in ('com.fun.lastwar.gp', 'com.fun.lastwar.vn.gp')
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createGpirAndroidOrganic2MonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_android_organic_revenue_ratio_month_view_by_j AS
SELECT
	'com.fun.lastwar.gp' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'gpir' as tag,
	SUM(
		CASE
			WHEN mediasource in ('Organic','organic') THEN revenue_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_d120) AS revenue_d120
FROM
	marketing.attribution.dws_overseas_gpir_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.facebook_segment IN ('country', 'N/A')
	AND roi.app_package in ('com.fun.lastwar.gp', 'com.fun.lastwar.vn.gp')
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createGpirCohortAndroidOrganic2MonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_cohort_android_organic_revenue_ratio_month_view_by_j AS
SELECT
	'com.fun.lastwar.gp' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'gpir_cohort' as tag,
	SUM(
		CASE
			WHEN mediasource in ('Organic','organic') THEN revenue_cohort_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_cohort_d120) AS revenue_d120
FROM
	marketing.attribution.dws_overseas_gpir_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.facebook_segment IN ('country', 'N/A')
	AND roi.app_package in ('com.fun.lastwar.gp', 'com.fun.lastwar.vn.gp')
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createGpirOnlyprofitCohortAndroidOrganic2MonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_onlyprofit_cohort_android_organic_revenue_ratio_month_view_by_j AS
SELECT
	'com.fun.lastwar.gp' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'gpir_onlyprofit_cohort' as tag,
	SUM(
		CASE
			WHEN mediasource in ('Organic','organic') THEN revenue_cohort_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_cohort_d120) AS revenue_d120
FROM
	marketing.attribution.dws_overseas_gpir_roi_profit roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.app_package in ('com.fun.lastwar.gp', 'com.fun.lastwar.vn.gp')
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createForUaAndroidOrganic2MonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_for_ua_android_organic_revenue_ratio_month_view_by_j AS
SELECT
	app_package,
	install_month,
	country_group,
	'for_ua' as tag,
	organic_revenue_d120,
	revenue_d120
FROM lw_20250703_gpir_cohort_android_organic_revenue_ratio_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


# iOS af_cohort版本的自然量收入占比，120日版本
# 直接取原始数据
def createAfCohorotIosOrganicMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cohort_ios_organic_revenue_ratio_month_view_by_j AS
SELECT
	'id6448786147' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	country_group,
	'af_cohort' as tag,
	SUM(
		CASE
			WHEN mediasource = 'Organic' THEN revenue_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_d120) AS revenue_d120
FROM
	lw_20250806_af_cohort_cost_revenue_app_country_group_media_day_view_by_j
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


# iOS af_cohort版本的自然量收入占比，120日版本
# 修正版本，将google按照花费比例估算收入后的cohort自然量占比
def createAfCohorotIosOrganicFixMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cohort_ios_organic_fix_revenue_ratio_month_view_by_j AS
SELECT
	'id6448786147' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	country_group,
	'af_cohort_fix' as tag,
	SUM(
		CASE
			WHEN mediasource = 'Organic' THEN revenue_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_d120) AS revenue_d120
FROM
	lw_20250806_af_cohort_cost_revenue_day_fix_table_by_j
GROUP BY
	install_month,
	country_group
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# iOS af_cohort版本的自然量收入占比，120日版本
# 拟合版本，将拟合结果加入后的cohort自然量占比
def createAfCohorotIosOrganicFitMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cohort_ios_organic_fit_revenue_ratio_month_view_by_j AS
SELECT
	'id6448786147' AS app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	country_group,
	tag,
	SUM(
		CASE
			WHEN mediasource = 'Organic' THEN revenue_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_d120) AS revenue_d120
FROM
	lw_20250806_af_cohort_cost_revenue_day_fit_table_by_j
GROUP BY
	install_month,
	country_group,
	tag
ORDER BY
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


def createOrganicMonthTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250718_android_organic_revenue_ratio_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250718_android_organic_revenue_ratio_month_table_by_j AS
WITH union_data AS (
	SELECT * FROM lw_20250703_af_android_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_af_cohort_android_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_af_onlyprofit_cohort_android_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_gpir_android_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_gpir_cohort_android_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_gpir_onlyprofit_cohort_android_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_for_ua_android_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_af_cohort_ios_organic_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_af_cohort_ios_organic_fix_revenue_ratio_month_view_by_j
	UNION ALL
	SELECT * FROM lw_20250703_af_cohort_ios_organic_fit_revenue_ratio_month_view_by_j
),
calculated_data AS (
	SELECT
		app_package,
		install_month,
		country_group,
		tag,
		organic_revenue_d120,
		revenue_d120,
		-- 计算最近456月的数据（即第4、5、6个月前的数据）
		COALESCE(
			LAG(organic_revenue_d120, 4) OVER (
				PARTITION BY app_package,country_group, tag
				ORDER BY install_month
			), 0
		) + COALESCE(
			LAG(organic_revenue_d120, 5) OVER (
				PARTITION BY app_package,country_group, tag
				ORDER BY install_month
			), 0
		) + COALESCE(
			LAG(organic_revenue_d120, 6) OVER (
				PARTITION BY app_package,country_group, tag
				ORDER BY install_month
			), 0
		) AS prev456_organic_revenue_d120,
		COALESCE(
			LAG(revenue_d120, 4) OVER (
				PARTITION BY app_package,country_group, tag
				ORDER BY install_month
			), 0
		) + COALESCE(
			LAG(revenue_d120, 5) OVER (
				PARTITION BY app_package,country_group, tag
				ORDER BY install_month
			), 0
		) + COALESCE(
			LAG(revenue_d120, 6) OVER (
				PARTITION BY app_package,country_group, tag
				ORDER BY install_month
			), 0
		) AS prev456_revenue_d120
	FROM union_data
)
SELECT
	app_package,
	install_month,
	country_group,
	tag,
	organic_revenue_d120,
	revenue_d120,
	CASE
		WHEN prev456_revenue_d120 = 0 THEN NULL
		ELSE ROUND(prev456_organic_revenue_d120 / prev456_revenue_d120, 4)
	END AS last456month_organic_revenue_ratio
FROM calculated_data
ORDER BY
	tag,
	country_group,
	install_month
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

# 自然量与lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j
# 合并，并计算含自然量的kpi_target，以供各种KPI使用
def createKpiTargetWithOrganicView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_kpi_target_with_organic_month_view_by_j AS
SELECT
	rr.*,
	o.last456month_organic_revenue_ratio,
	CASE
		WHEN o.last456month_organic_revenue_ratio IS NOT NULL 
		THEN rr.kpi_target * (1 - o.last456month_organic_revenue_ratio)
		ELSE rr.kpi_target
	END AS kpi_target_with_organic
FROM
	lw_20250703_af_revenue_rise_ratio_predict_kpi_target_month_view_by_j rr
LEFT JOIN lw_20250718_android_organic_revenue_ratio_month_table_by_j o
	ON rr.app_package = o.app_package
	AND rr.install_month = o.install_month
	AND rr.country_group = o.country_group
	AND rr.tag = o.tag
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

#####################################################
# 推算回本周期（月）

# 真实回本周期，是根据90日、120日、150日的ROI来计算的
def createPayback1View():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback1_month_view_by_j AS
WITH roi_base AS (
	SELECT
		app_package,
		install_month,
		country_group,
		mediasource,
		ad_type,
		tag,
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
		lw_20250703_cost_revenue_app_month_table_by_j
),
predict_base AS (
	SELECT
		*
	FROM
		lw_20250703_af_revenue_rise_ratio_predict_kpi_target_month_view_by_j
)
SELECT
	r.app_package,
	r.install_month,
	r.country_group,
	r.mediasource,
	r.ad_type,
	r.tag,
	p.kpi_target,
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
	AND r.mediasource = p.mediasource
	AND r.ad_type = p.ad_type
	AND r.tag = p.tag;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 修正真是回本周期，将不完整的月份过滤掉
def createPayback1ViewFix():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback1_fix_month_view_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	CASE
		WHEN b.month_diff >= 6 THEN a.payback_150
		WHEN b.month_diff = 5 THEN a.payback_120
		WHEN b.month_diff = 4 THEN a.payback_90
		WHEN b.month_diff = 3 THEN a.payback_60
		ELSE NULL
	END AS d_payback_1
FROM
	lw_20250703_af_payback1_month_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month

	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 真实+预测回本周期，根据真实的30,60,90,120日ROI与预测付费增长比例，预测出回本周期
def createPayback2View():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback2_month_view_by_j AS
WITH roi_base AS (
	SELECT
		app_package,
		install_month,
		country_group,
		mediasource,
		ad_type,
		tag,
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
		lw_20250703_cost_revenue_app_month_table_by_j
),
predict_base AS (
	SELECT
		*
	FROM
		lw_20250703_af_revenue_rise_ratio_predict_kpi_target_month_view_by_j
),
predict AS (
	SELECT
		r.app_package,
		r.install_month,
		r.country_group,
		r.mediasource,
		r.ad_type,
		r.tag,
		p.kpi_target,
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
		LEFT JOIN predict_base p ON r.app_package = p.app_package
		AND r.install_month = p.install_month
		AND r.country_group = p.country_group
		AND r.mediasource = p.mediasource
		AND r.ad_type = p.ad_type
		AND r.tag = p.tag
)
SELECT
	app_package,
	install_month,
	country_group,
	mediasource,
	ad_type,
	tag,
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

# 修正预测回本周期，将不完整的月份过滤掉
def createPayback2ViewFix():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback2_fix_month_view_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	CASE
		WHEN b.month_diff >= 6 THEN a.payback_150
		WHEN b.month_diff = 5 THEN a.payback_120_p_150
		WHEN b.month_diff = 4 THEN a.payback_90_p_150
		WHEN b.month_diff = 3 THEN a.payback_60_p_150
		WHEN b.month_diff = 2 THEN a.payback_30_p_150
		WHEN b.month_diff = 1 THEN a.payback_7_p_150
		ELSE NULL
	END AS d_payback_2
FROM
	lw_20250703_af_payback2_month_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 创建推算回本周期table
# 将lw_20250703_af_payback1_fix_month_view_by_j，lw_20250703_af_payback2_fix_month_view_by_j
# 合并成一个table
def createPaybackTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_af_payback_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_af_payback_month_table_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	a.d_payback_1 as real_payback_month,
	b.d_payback_2 as predict_payback_month
FROM
	lw_20250703_af_payback1_fix_month_view_by_j a
	LEFT JOIN lw_20250703_af_payback2_fix_month_view_by_j b ON
	a.app_package = b.app_package
	AND a.install_month = b.install_month
	AND a.country_group = b.country_group
	AND a.mediasource = b.mediasource
	AND a.ad_type = b.ad_type
	AND a.tag = b.tag
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return


# 带自然量版本，真实回本周期，是根据90日、120日、150日的ROI来计算的
def createPayback1OrganicView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback1_organic_month_view_by_j AS
WITH roi_base AS (
	SELECT
		app_package,
		install_month,
		country_group,
		mediasource,
		ad_type,
		tag,
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
		lw_20250703_cost_revenue_app_month_table_by_j
),
predict_base AS (
	SELECT
		*
	FROM
		lw_20250703_kpi_target_with_organic_month_view_by_j
)
SELECT
	r.app_package,
	r.install_month,
	r.country_group,
	r.mediasource,
	r.ad_type,
	r.tag,
	p.kpi_target_with_organic,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		ELSE NULL
	END AS payback_60,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		WHEN roi90 >= kpi_target_with_organic THEN 2 + (kpi_target_with_organic - roi60) /(roi90 - roi60)
		ELSE NULL
	END AS payback_90,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		WHEN roi90 >= kpi_target_with_organic THEN 2 + (kpi_target_with_organic - roi60) /(roi90 - roi60)
		WHEN roi120 >= kpi_target_with_organic THEN 3 +(kpi_target_with_organic - roi90) /(roi120 - roi90)
		ELSE NULL
	END AS payback_120,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		WHEN roi90 >= kpi_target_with_organic THEN 2 + (kpi_target_with_organic - roi60) /(roi90 - roi60)
		WHEN roi120 >= kpi_target_with_organic THEN 3 +(kpi_target_with_organic - roi90) /(roi120 - roi90)
		WHEN roi150 >= kpi_target_with_organic THEN 4 +(kpi_target_with_organic - roi120) /(roi150 - roi120)
		ELSE 5.0
	END AS payback_150
FROM
	roi_base r
	LEFT JOIN predict_base p ON r.app_package = p.app_package
	AND r.install_month = p.install_month
	AND r.country_group = p.country_group
	AND r.mediasource = p.mediasource
	AND r.ad_type = p.ad_type
	AND r.tag = p.tag;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 带自然量版本，修正真是回本周期，将不完整的月份过滤掉
def createPayback1OrganicViewFix():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback1_organic_fix_month_view_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	CASE
		WHEN b.month_diff >= 6 THEN a.payback_150
		WHEN b.month_diff = 5 THEN a.payback_120
		WHEN b.month_diff = 4 THEN a.payback_90
		WHEN b.month_diff = 3 THEN a.payback_60
		ELSE NULL
	END AS d_payback_1_organic
FROM
	lw_20250703_af_payback1_organic_month_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month

	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 带自然量版本，真实+预测回本周期，根据真实的30,60,90,120日ROI与预测付费增长比例，预测出回本周期
def createPayback2OrganicView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback2_organic_month_view_by_j AS
WITH roi_base AS (
	SELECT
		app_package,
		install_month,
		country_group,
		mediasource,
		ad_type,
		tag,
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
		`marketing`.`attribution`.lw_20250703_cost_revenue_app_month_table_by_j
),
predict_base AS (
	SELECT
		*
	FROM
		`marketing`.`attribution`.lw_20250703_kpi_target_with_organic_month_view_by_j
),
predict AS (
	SELECT
		r.app_package,
		r.install_month,
		r.country_group,
		r.mediasource,
		r.ad_type,
		r.tag,
		p.kpi_target_with_organic,
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
		LEFT JOIN predict_base p ON r.app_package = p.app_package
		AND r.install_month = p.install_month
		AND r.country_group = p.country_group
		AND r.mediasource = p.mediasource
		AND r.ad_type = p.ad_type
		AND r.tag = p.tag
)
SELECT
	app_package,
	install_month,
	country_group,
	mediasource,
	ad_type,
	tag,
	CASE
		WHEN roi_7_predict_60 >= kpi_target_with_organic THEN 2.0
		WHEN roi_7_predict_90 >= kpi_target_with_organic THEN 2 + try_divide(kpi_target_with_organic - roi_7_predict_60, roi_7_predict_90 - roi_7_predict_60)
		WHEN roi_7_predict_120 >= kpi_target_with_organic THEN 3 + try_divide(kpi_target_with_organic - roi_7_predict_90, roi_7_predict_120 - roi_7_predict_90)
		WHEN roi_7_predict_150 >= kpi_target_with_organic THEN 4 + try_divide(kpi_target_with_organic - roi_7_predict_120, roi_7_predict_150 - roi_7_predict_120)
		ELSE 5.0
	END AS payback_7_p_150,
	CASE
		WHEN roi_30_predict_60 >= kpi_target_with_organic THEN 2.0
		WHEN roi_30_predict_90 >= kpi_target_with_organic THEN 2 + try_divide(kpi_target_with_organic - roi_30_predict_60, roi_30_predict_90 - roi_30_predict_60)
		WHEN roi_30_predict_120 >= kpi_target_with_organic THEN 3 + try_divide(kpi_target_with_organic - roi_30_predict_90, roi_30_predict_120 - roi_30_predict_90)
		WHEN roi_30_predict_150 >= kpi_target_with_organic THEN 4 + try_divide(kpi_target_with_organic - roi_30_predict_120, roi_30_predict_150 - roi_30_predict_120)
		ELSE 5.0
	END AS payback_30_p_150,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		WHEN roi_60_predict_90 >= kpi_target_with_organic THEN 2 + try_divide(kpi_target_with_organic - roi60, roi_60_predict_90 - roi60)
		WHEN roi_60_predict_120 >= kpi_target_with_organic THEN 3 + try_divide(kpi_target_with_organic - roi_60_predict_90, roi_60_predict_120 - roi_60_predict_90)
		WHEN roi_60_predict_150 >= kpi_target_with_organic THEN 4 + try_divide(kpi_target_with_organic - roi_60_predict_120, roi_60_predict_150 - roi_60_predict_120)
		ELSE 5.0
	END AS payback_60_p_150,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		WHEN roi90 >= kpi_target_with_organic THEN 2 + try_divide(kpi_target_with_organic - roi60, roi90 - roi60)
		WHEN roi_90_predict_120 >= kpi_target_with_organic THEN 3 + try_divide(kpi_target_with_organic - roi90, roi_90_predict_120 - roi90)
		WHEN roi_90_predict_150 >= kpi_target_with_organic THEN 4 + try_divide(kpi_target_with_organic - roi_90_predict_120, roi_90_predict_150 - roi_90_predict_120)
		ELSE 5.0
	END AS payback_90_p_150,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		WHEN roi90 >= kpi_target_with_organic THEN 2 + try_divide(kpi_target_with_organic - roi60, roi90 - roi60)
		WHEN roi120 >= kpi_target_with_organic THEN 3 + try_divide(kpi_target_with_organic - roi90, roi120 - roi90)
		WHEN roi150 >= kpi_target_with_organic THEN 4 + try_divide(kpi_target_with_organic - roi120, roi_120_predict_150 - roi120)
		ELSE 5.0
	END AS payback_120_p_150,
	CASE
		WHEN roi60 >= kpi_target_with_organic THEN 2.0
		WHEN roi90 >= kpi_target_with_organic THEN 2 + try_divide(kpi_target_with_organic - roi60, roi90 - roi60)
		WHEN roi120 >= kpi_target_with_organic THEN 3 + try_divide(kpi_target_with_organic - roi90, roi120 - roi90)
		WHEN roi150 >= kpi_target_with_organic THEN 4 + try_divide(kpi_target_with_organic - roi120, roi150 - roi120)
		ELSE 5.0
	END AS payback_150
FROM
	predict;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 带自然量版本，修正预测回本周期，将不完整的月份过滤掉
def createPayback2OrganicViewFix():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_payback2_organic_fix_month_view_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	CASE
		WHEN b.month_diff >= 6 THEN a.payback_150
		WHEN b.month_diff = 5 THEN a.payback_120_p_150
		WHEN b.month_diff = 4 THEN a.payback_90_p_150
		WHEN b.month_diff = 3 THEN a.payback_60_p_150
		WHEN b.month_diff = 2 THEN a.payback_30_p_150
		WHEN b.month_diff = 1 THEN a.payback_7_p_150
		ELSE NULL
	END AS d_payback_2_organic
FROM
	lw_20250703_af_payback2_organic_month_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 带自然量版本，创建推算回本周期table
def createPaybackOrganicTable():
	sql1 = """
DROP TABLE IF EXISTS lw_20250703_af_payback_organic_month_table_by_j;
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)
	sql2 = """
CREATE TABLE lw_20250703_af_payback_organic_month_table_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	a.d_payback_1_organic as real_payback_month_organic,
	b.d_payback_2_organic as predict_payback_month_organic
FROM
	lw_20250703_af_payback1_organic_fix_month_view_by_j a
	LEFT JOIN lw_20250703_af_payback2_organic_fix_month_view_by_j b ON
	a.app_package = b.app_package
	AND a.install_month = b.install_month
	AND a.country_group = b.country_group
	AND a.mediasource = b.mediasource
	AND a.ad_type = b.ad_type
	AND a.tag = b.tag
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return



def createViewsAndTables():
	createCountryGroupTable()
	# createMonthView()

	# 只保留cohort，忽略非cohort数据

	# AF 花费、收入24小时cohort数据，包括普通、添加adtype、大盘、只分国家 4种
	createAfAppMediaCountryCohortCostRevenueMonthyView()
	createAfAppCountryCohortCostRevenueMonthyView()
	createAfAppCohortCostRevenueMonthyView()
	createAfCohortCostRevenueMonthyTable()

	# GPIR 花费、收入24小时cohort数据数据，包括普通、添加adtype 2种 
	createGPIRAppMediaCountryCohortCostRevenueMonthyView()
	createGPIRCohortCostRevenueMonthyTable()

	# AF纯利 花费、收入24小时cohort数据，包括普通、添加adtype 2种
	createAfOnlyprofitAppMediaCountryCohortCostRevenueMonthyView()
	createAfOnlyprofitAppCountryCohortCostRevenueMonthyView()
	createAfOnlyprofitAppCohortCostRevenueMonthyView()
	createAfOnlyProfitCohortCostRevenueMonthyTable()

	# GPIR纯利 花费、收入24小时cohort数据，包括普通、添加adtype 2种
	createGPIROnlyprofitAppMediaCountryCohortCostRevenueMonthyView()
	createGPIROnlyProfitCohortCostRevenueMonthyTable()

	createForUaCostRevenueMonthyView()
	createAppLovinRatioView()
	createForUaCostRevenueMonthyTable()

	createAfIosAppMediaCountryCohortCostRevenueMonthyView()
	

	# 拟合iOS结果相关
	createIosAfCostRevenueDayView()
	createIosAfCostRevenueDayFixTable()
	createIosAfCostRevenueMonthyFixView()
	createIosAfCostRevenueDayFitTable()

	createIosAfCostRevenueDayFitCheckTable()

	createIosTagCostRevenueMonthyView()

	# 所有的花费、收入数据汇总
	createCostRevenueMonthyView()
	createCostRevenueMonthyTable()

	# 计算kpi_target
	createGpirCohortKpiTargetView()
	createIosCohortKpiTargetView()
	createIosCohortKpiTargetView2()
	createForUaKpiTargetView()
	createKpiTargetTable()

	# 计算收入增长率
	createRevenueRiseRatioView()
	createPredictRevenueRiseRatioView()
	createPredictRevenueRiseRatioTable()
	createPredictRevenueRiseRatioAndkpiTargetView()

	# 推算KPI
	createKpiView()
	createKpiWithDiscountView()
	createKpiTable()

	# # 推算动态KPI
	# createKpi2View()
	# createKpi2ViewFix()
	# createKpi2FixTable()

	# # 自然量收入占比
	# createAfAndroidOrganicMonthView()
	# createAfCohortAndroidOrganicMonthView()
	# createAfOnlyprofitCohortAndroidOrganicMonthView()
	# createGpirAndroidOrganic2MonthView()
	# createGpirCohortAndroidOrganic2MonthView()
	# createGpirOnlyprofitCohortAndroidOrganic2MonthView()
	# createForUaAndroidOrganic2MonthView()

	# createAfCohorotIosOrganicMonthView()
	# createAfCohorotIosOrganicFixMonthView()
	# createAfCohorotIosOrganicFitMonthView()

	# createOrganicMonthTable()
	
	# # 只用自然量收入占比计算含自然量回本目标
	# createKpiTargetWithOrganicView()

	# # 回本周期计算
	# createPayback1View()
	# createPayback1ViewFix()
	# createPayback2View()
	# createPayback2ViewFix()
	# createPaybackTable()

	# createPayback1OrganicView()
	# createPayback1OrganicViewFix()
	# createPayback2OrganicView()
	# createPayback2OrganicViewFix()
	# createPaybackOrganicTable()

	pass



def main(dayStr=None):
	createViewsAndTables()


if __name__ == "__main__":
	main()
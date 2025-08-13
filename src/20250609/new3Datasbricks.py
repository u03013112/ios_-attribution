import os
import datetime
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.dataBricks import execSql, execSql2


def createCountryGroupTable():
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
('SG','T1'),('UK','T1'),('MO','T1'),('IL','T1'),('TW','T1'),
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
			dws_overseas_public_roi
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
	dws_overseas_public_roi roi
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
	dws_overseas_public_roi roi
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
	dws_overseas_public_roi roi
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
			dws_overseas_public_roi
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
	dws_overseas_gpir_roi roi
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
	dws_overseas_roi_profit roi
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
	dws_overseas_roi_profit roi
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
	dws_overseas_roi_profit roi
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
	dws_overseas_gpir_roi_profit roi
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




def createViewsAndTables():
	# createCountryGroupTable()
	# createMonthView()

	# 只保留cohort，忽略非cohort数据

	# # AF 花费、收入24小时cohort数据，包括普通、添加adtype、大盘、只分国家 4种
	# createAfAppMediaCountryCohortCostRevenueMonthyView()
	# createAfAppCountryCohortCostRevenueMonthyView()
	# createAfAppCohortCostRevenueMonthyView()
	# createAfCohortCostRevenueMonthyTable()

	# # GPIR 花费、收入24小时cohort数据数据，包括普通、添加adtype 2种 
	# createGPIRAppMediaCountryCohortCostRevenueMonthyView()
	# createGPIRCohortCostRevenueMonthyTable()

	# AF纯利 花费、收入24小时cohort数据，包括普通、添加adtype 2种
	createAfOnlyprofitAppMediaCountryCohortCostRevenueMonthyView()
	createAfOnlyprofitAppCountryCohortCostRevenueMonthyView()
	createAfOnlyprofitAppCohortCostRevenueMonthyView()
	createAfOnlyProfitCohortCostRevenueMonthyTable()

	# GPIR纯利 花费、收入24小时cohort数据，包括普通、添加adtype 2种
	createGPIROnlyprofitAppMediaCountryCohortCostRevenueMonthyView()
	createGPIROnlyProfitCohortCostRevenueMonthyTable()


def main(dayStr=None):
	createViewsAndTables()


if __name__ == "__main__":
	main()
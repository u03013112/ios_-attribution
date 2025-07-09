# new2修改
# 代码逻辑修改，为了更好的工作流，可以方便的进行数据添加与更新到quickbi
# 将更多的原始数据拆分成视图，和表格
# 将最终的join直接交给quickbi来完成
# 进一步的数据处理，比如从revenue->roi的计算,暂时没有想好在哪里做
# 放在quickbi中做的好处是他可以进一步汇总，但是代码都在线上，无法复用与迭代
# 自己算的问题就是在quickbi中不方便重新分组汇总


# 此版本建立的view 和 table
#  统一使用 lw_20250703_ 前缀
#  统一使用 by_j 后缀
# 通用的表除外，比如国家分组表

import os
import datetime
from venv import create
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

def createCountryGroupTable():
	sql = """
-- 创建国家分组表 lw_country_group_table_by_j_20250703
CREATE TABLE IF NOT EXISTS lw_country_group_table_by_j_20250703 (
	country STRING,
	country_group STRING
);

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
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 创建月视图，动态的计算目前的月份和安装月份之间的差值，方便后面过滤数据
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

# 自然量收入占比,120日版本
# 取前3个月（不包括本月）的自然量收入和总收入的比值
def createOrganicMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_organic_revenue_ratio_country_group_month_view_by_j AS
WITH base_data AS (
	SELECT
		SUBSTR(install_day, 1, 6) AS install_month,
		COALESCE(cg.country_group, 'other') AS country_group,
		-- 没匹配到的国家为other
		SUM(cost_value_usd) AS cost,
		SUM(
			CASE
				WHEN mediasource = 'Organic' THEN revenue_d120
				ELSE 0
			END
		) AS organic_revenue_d120,
		SUM(revenue_d120) AS revenue_d120
	FROM
		dws_overseas_public_roi roi
		LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	WHERE
		roi.app = '502'
		AND roi.app_package = 'com.fun.lastwar.gp'
		AND roi.facebook_segment IN ('country', 'N/A')
	GROUP BY
		install_month,
		country_group
),
ordered_data AS (
	SELECT
		install_month,
		country_group,
		organic_revenue_d120,
		revenue_d120,
		ROW_NUMBER() OVER (
			PARTITION BY country_group
			ORDER BY
				install_month
		) AS rn
	FROM
		base_data
)
SELECT
	install_month,
	country_group,
	CASE
		WHEN SUM(prev3_revenue_d120) = 0 THEN NULL
		ELSE ROUND(
			SUM(prev3_organic_revenue_d120) / SUM(prev3_revenue_d120),
			4
		)
	END AS last3month_organic_revenue_ratio
FROM
	(
		SELECT
			install_month,
			country_group,
			-- 计算上3个月（不含本月）的累计值，不足3个月补0
			COALESCE(
				LAG(organic_revenue_d120, 1) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 2) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 3) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_organic_revenue_d120,
			COALESCE(
				LAG(revenue_d120, 1) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 2) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 3) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_revenue_d120
		FROM
			ordered_data
	) t
group by
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

# lw_20250703_organic_revenue_ratio_country_group_month_view_by_j -> 固定成一个table
def createOrganicMonthTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_organic_revenue_ratio_country_group_month_table_by_j;
CREATE TABLE lw_20250703_organic_revenue_ratio_country_group_month_table_by_j AS
SELECT * FROM lw_20250703_organic_revenue_ratio_country_group_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 自然量收入占比，与createOrganicMonthView类似，但是取满日数据，所以要向前取更久
def createOrganic2MonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_android_organic_revenue_ratio_month_view_by_j AS
WITH base_data AS (
	SELECT
		SUBSTR(install_day, 1, 6) AS install_month,
		COALESCE(cg.country_group, 'other') AS country_group,
		-- 没匹配到的国家为other
		SUM(cost_value_usd) AS cost,
		SUM(
			CASE
				WHEN mediasource = 'Organic' THEN revenue_d120
				ELSE 0
			END
		) AS organic_revenue_d120,
		SUM(revenue_d120) AS revenue_d120
	FROM
		dws_overseas_public_roi roi
		LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	WHERE
		roi.app = '502'
		AND roi.app_package = 'com.fun.lastwar.gp'
		AND roi.facebook_segment IN ('country', 'N/A')
	GROUP BY
		install_month,
		country_group
),
ordered_data AS (
	SELECT
		install_month,
		country_group,
		organic_revenue_d120,
		revenue_d120,
		ROW_NUMBER() OVER (
			PARTITION BY country_group
			ORDER BY
				install_month
		) AS rn
	FROM
		base_data
)
SELECT
	'com.fun.lastwar.gp' AS app_package,
	install_month,
	country_group,
	CASE
		WHEN SUM(prev3_revenue_d120) = 0 THEN NULL
		ELSE ROUND(
			SUM(prev3_organic_revenue_d120) / SUM(prev3_revenue_d120),
			4
		)
	END AS last456month_organic_revenue_ratio
FROM
	(
		SELECT
			install_month,
			country_group,
			COALESCE(
				LAG(organic_revenue_d120, 4) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 5) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 6) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_organic_revenue_d120,
			COALESCE(
				LAG(revenue_d120, 4) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 5) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 6) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_revenue_d120
		FROM
			ordered_data
	) t
group by
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

def createOrganic2MonthTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_android_organic_revenue_ratio_month_table_by_j;
CREATE TABLE lw_20250703_android_organic_revenue_ratio_month_table_by_j AS
SELECT * FROM lw_20250703_android_organic_revenue_ratio_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createOrganic2MonthViewForDebug():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_android_organic_revenue_ratio_month_view_for_debu_by_j AS
SELECT
		SUBSTR(install_day, 1, 6) AS install_month,
		COALESCE(cg.country_group, 'other') AS country_group,
		-- 没匹配到的国家为other
		SUM(cost_value_usd) AS cost,
		SUM(
			CASE
				WHEN mediasource = 'Organic' THEN revenue_d120
				ELSE 0
			END
		) AS organic_revenue_d120,
		SUM(revenue_d120) AS revenue_d120
	FROM
		dws_overseas_public_roi roi
		LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	WHERE
		roi.app = '502'
		AND roi.app_package = 'com.fun.lastwar.gp'
		AND roi.facebook_segment IN ('country', 'N/A')
	GROUP BY
		install_month,
		country_group
		;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createOrganic2MonthTableForDebug():
	sql = """
DROP TABLE IF EXISTS lw_20250703_android_organic_revenue_ratio_month_table_for_debu_by_j;
CREATE TABLE lw_20250703_android_organic_revenue_ratio_month_table_for_debu_by_j AS
SELECT * FROM lw_20250703_android_organic_revenue_ratio_month_view_for_debu_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# GPIR版本的自然量收入占比，120日版本
# 取前3个月（不包括本月）的自然量收入和总收入的比值
def createGPIROrganicMonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_organic_revenue_ratio_country_group_month_view_by_j AS
WITH base_data AS (
	SELECT
		SUBSTR(install_day, 1, 6) AS install_month,
		COALESCE(cg.country_group, 'other') AS country_group,
		SUM(cost_value_usd) AS cost,
		SUM(
			CASE
				WHEN mediasource in ('Organic','organic') THEN revenue_d120
				ELSE 0
			END
		) AS organic_revenue_d120,
		SUM(revenue_d120) AS revenue_d120
	FROM
		ads_lastwar_mediasource_reattribution roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	WHERE
		roi.facebook_segment IN ('country', 'N/A')
	GROUP BY
		install_month,
		country_group
),
ordered_data AS (
	SELECT
		install_month,
		country_group,
		organic_revenue_d120,
		revenue_d120,
		ROW_NUMBER() OVER (
			PARTITION BY country_group
			ORDER BY
				install_month
		) AS rn
	FROM
		base_data
)
SELECT
	install_month,
	country_group,
	CASE
		WHEN SUM(prev3_revenue_d120) = 0 THEN NULL
		ELSE ROUND(
			SUM(prev3_organic_revenue_d120) / SUM(prev3_revenue_d120),
			4
		)
	END AS last3month_gpir_organic_revenue_ratio
FROM
	(
		SELECT
			install_month,
			country_group,
			-- 计算上3个月（不含本月）的累计值，不足3个月补0
			COALESCE(
				LAG(organic_revenue_d120, 1) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 2) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 3) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_organic_revenue_d120,
			COALESCE(
				LAG(revenue_d120, 1) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 2) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 3) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_revenue_d120
		FROM
			ordered_data
	) t
group by
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

def createGPIROrganicMonthTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_gpir_organic_revenue_ratio_country_group_month_table_by_j;
CREATE TABLE lw_20250703_gpir_organic_revenue_ratio_country_group_month_table_by_j AS
SELECT * FROM lw_20250703_gpir_organic_revenue_ratio_country_group_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# GPIR版本的自然量收入占比，120日版本
def createGPIROrganic2MonthView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_android_gpir_organic_revenue_ratio_month_view_by_j AS
WITH base_data AS (
	SELECT
		SUBSTR(install_day, 1, 6) AS install_month,
		COALESCE(cg.country_group, 'other') AS country_group,
		SUM(cost_value_usd) AS cost,
		SUM(
			CASE
				WHEN mediasource in ('Organic','organic') THEN revenue_d120
				ELSE 0
			END
		) AS organic_revenue_d120,
		SUM(revenue_d120) AS revenue_d120
	FROM
		ads_lastwar_mediasource_reattribution roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	WHERE
		roi.facebook_segment IN ('country', 'N/A')
		and roi.app_package = 'com.fun.lastwar.gp'
	GROUP BY
		install_month,
		country_group
),
ordered_data AS (
	SELECT
		install_month,
		country_group,
		organic_revenue_d120,
		revenue_d120,
		ROW_NUMBER() OVER (
			PARTITION BY country_group
			ORDER BY
				install_month
		) AS rn
	FROM
		base_data
)
SELECT
	'com.fun.lastwar.gp' AS app_package,
	install_month,
	country_group,
	CASE
		WHEN SUM(prev3_revenue_d120) = 0 THEN NULL
		ELSE ROUND(
			SUM(prev3_organic_revenue_d120) / SUM(prev3_revenue_d120),
			4
		)
	END AS last456month_gpir_organic_revenue_ratio
FROM
	(
		SELECT
			install_month,
			country_group,
			-- 计算上3个月（不含本月）的累计值，不足3个月补0
			COALESCE(
				LAG(organic_revenue_d120, 4) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 5) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(organic_revenue_d120, 6) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_organic_revenue_d120,
			COALESCE(
				LAG(revenue_d120, 4) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 5) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) + COALESCE(
				LAG(revenue_d120, 6) OVER (
					PARTITION BY country_group
					ORDER BY
						install_month
				),
				0
			) AS prev3_revenue_d120
		FROM
			ordered_data
	) t
group by
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

def createGPIROrganic2MonthTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_android_gpir_organic_revenue_ratio_month_table_by_j;
CREATE TABLE lw_20250703_android_gpir_organic_revenue_ratio_month_table_by_j AS
SELECT * FROM lw_20250703_android_gpir_organic_revenue_ratio_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


def createGPIROrganic2MonthViewForDebug():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_android_gpir_organic_revenue_ratio_month_view_for_debu_by_j AS
SELECT
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	SUM(cost_value_usd) AS cost,
	SUM(
		CASE
			WHEN mediasource in ('Organic','organic') THEN revenue_d120
			ELSE 0
		END
	) AS organic_revenue_d120,
	SUM(revenue_d120) AS revenue_d120
FROM
	ads_lastwar_mediasource_reattribution roi
LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.facebook_segment IN ('country', 'N/A')
GROUP BY
	install_month,
	country_group
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createGPIROrganic2MonthTableForDebug():
	sql = """
DROP TABLE IF EXISTS lw_20250703_android_gpir_organic_revenue_ratio_month_table_for_debu_by_j;
CREATE TABLE lw_20250703_android_gpir_organic_revenue_ratio_month_table_for_debu_by_j AS
SELECT * FROM lw_20250703_android_gpir_organic_revenue_ratio_month_view_for_debu_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 直接使用bi数据，注意这是AF归因，不是GPIR
def createAfAppMediaCountryCostRevenueMonthyView():
	sql = """
CREATE VIEW IF NOT EXISTS lw_20250703_af_cost_revenue_app_country_group_media_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_d1) AS revenue_d1,
	SUM(revenue_d3) AS revenue_d3,
	SUM(revenue_d7) AS revenue_d7,
	SUM(revenue_d30) AS revenue_d30,
	SUM(revenue_d60) AS revenue_d60,
	SUM(revenue_d90) AS revenue_d90,
	SUM(revenue_d120) AS revenue_d120
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

# 带Adtype的版本
def createAfAppMediaCountryAdtypeCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cost_revenue_app_country_group_media_adtype_month_view_by_j AS
SELECT
    roi.app_package,
    SUBSTR(roi.install_day, 1, 6) AS install_month,
    COALESCE(cg.country_group, 'other') AS country_group,
    roi.mediasource,
    CASE
        WHEN roi.mediasource IN ('Facebook Ads', 'googleadwords_int') THEN roi.optimization_goal
        WHEN roi.mediasource = 'applovin_int' THEN CASE
            WHEN roi.campaign_name LIKE '%D7%' THEN 'D7'
            WHEN roi.campaign_name LIKE '%D28%' THEN 'D28'
            ELSE 'other'
        END
        ELSE 'other'
    END AS ad_type,
    SUM(roi.cost_value_usd) AS cost,
    SUM(roi.revenue_d1) AS revenue_d1,
    SUM(roi.revenue_d3) AS revenue_d3,
    SUM(roi.revenue_d7) AS revenue_d7,
    SUM(roi.revenue_d30) AS revenue_d30,
    SUM(roi.revenue_d60) AS revenue_d60,
    SUM(roi.revenue_d90) AS revenue_d90,
    SUM(roi.revenue_d120) AS revenue_d120
FROM
    dws_overseas_public_roi roi
    LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
    LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
WHERE
    roi.app = '502'
    AND m.month_diff > 0
    AND roi.facebook_segment IN ('country', 'N/A')
    AND roi.mediasource IN (
        'Facebook Ads',
        'googleadwords_int',
        'applovin_int'
    )
GROUP BY
    roi.app_package,
    SUBSTR(roi.install_day, 1, 6),
    COALESCE(cg.country_group, 'other'),
    roi.mediasource,
    ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return



def createAdtypeView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_adtype_view_by_j AS
SELECT
    campaign_name,
    MAX(
        CASE
            WHEN mediasource IN ('Facebook Ads', 'googleadwords_int') THEN optimization_goal
            WHEN mediasource = 'applovin_int' THEN CASE
                WHEN campaign_name LIKE '%D7%' THEN 'D7'
                WHEN campaign_name LIKE '%D28%' THEN 'D28'
                ELSE 'other'
            END
            ELSE 'other'
        END
    ) AS ad_type
FROM dws_overseas_public_roi
WHERE
    app = '502'
    AND facebook_segment IN ('country', 'N/A')
    AND mediasource IN ('Facebook Ads', 'googleadwords_int', 'applovin_int')
GROUP BY campaign_name
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)

	return

# 针对AF分app、媒体、国家、广告类型、安装月，大R削弱，然后最总获得花费与收入金额
def createAfAppNerfBigRCostRevenueMonthyView(percentile=0.99):
	# 先获得不同分组中的制定分位数收入金额
	percentileStr = str(percentile).replace('.', '')
	view1Name = f"lw_20250703_af_big_r_{percentileStr}_month_view_by_j"
	sql1 = f"""
CREATE OR REPLACE VIEW {view1Name} AS
SELECT
    app_package,
    mediasource,
    country_group,
    ad_type,
    percentile_approx(revenue_1d, { percentile }) AS revenue_1d_big_r,
    percentile_approx(revenue_3d, { percentile }) AS revenue_3d_big_r,
    percentile_approx(revenue_7d, { percentile }) AS revenue_7d_big_r,
    percentile_approx(revenue_30d, { percentile }) AS revenue_30d_big_r,
    percentile_approx(revenue_60d, { percentile }) AS revenue_60d_big_r,
    percentile_approx(revenue_90d, { percentile }) AS revenue_90d_big_r,
    percentile_approx(revenue_120d, { percentile }) AS revenue_120d_big_r
FROM
    (
        SELECT
            roi.app_package,
            SUBSTR(roi.install_day, 1, 6) AS install_month,
            roi.mediasource,
            roi.game_uid,
            COALESCE(cg.country_group, 'other') AS country_group,
            'ALL' AS ad_type,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 0 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 2 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 6 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_7d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 29 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_30d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 59 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_60d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 89 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_90d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 119 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_120d
        FROM
            rg_bi.dwd_overseas_revenue_allproject roi
            LEFT JOIN (
                SELECT
                    mediasource,
                    campaign_id,
                    campaign_name
                FROM
                    dwb_overseas_mediasource_campaign_map
                GROUP BY
                    mediasource,
                    campaign_id,
                    campaign_name
            ) b ON roi.mediasource = b.mediasource
            AND roi.campaign_id = b.campaign_id
            LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
            LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
        WHERE
            roi.zone = '0'
            AND roi.app = '502'
            AND roi.install_day BETWEEN '20241001'
            AND '20250301'
            AND m.month_diff > 0
        GROUP BY
            app_package,
            SUBSTR(roi.install_day, 1, 6),
            COALESCE(cg.country_group, 'other'),
            roi.mediasource,
            ad_type,
            roi.game_uid
    ) grouped_user_revenue
GROUP BY
    app_package,
    mediasource,
    country_group,
    ad_type;
	"""
	print(f"Executing SQL1: {sql1}")
	execSql2(sql1)

	# 再将每个分组按照得到金额进行削弱，当用户付费金额大于削弱值，那么改为削弱值
	view2Name = f"lw_20250703_af_revenue_{percentileStr}_month_view_by_j"
	sql2 = f"""
CREATE OR REPLACE VIEW {view2Name} AS
SELECT
    u.app_package,
    u.install_month,
    u.country_group,
    CASE WHEN u.mediasource = 'tiktokglobal_int' THEN 'bytedanceglobal_int'
	ELSE u.mediasource
	END AS mediasource,
    u.ad_type,
    SUM(LEAST(u.revenue_1d, r.revenue_1d_big_r)) AS revenue_d1,
    SUM(LEAST(u.revenue_3d, r.revenue_3d_big_r)) AS revenue_d3,
    SUM(LEAST(u.revenue_7d, r.revenue_7d_big_r)) AS revenue_d7,
    SUM(LEAST(u.revenue_30d, r.revenue_30d_big_r)) AS revenue_d30,
    SUM(LEAST(u.revenue_60d, r.revenue_60d_big_r)) AS revenue_d60,
    SUM(LEAST(u.revenue_90d, r.revenue_90d_big_r)) AS revenue_d90,
    SUM(LEAST(u.revenue_120d, r.revenue_120d_big_r)) AS revenue_d120
FROM
    (
        SELECT
            roi.app_package,
            SUBSTR(roi.install_day, 1, 6) AS install_month,
            roi.mediasource,
            roi.game_uid,
            COALESCE(cg.country_group, 'other') AS country_group,
            'ALL' AS ad_type,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 0 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 2 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 6 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_7d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 29 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_30d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 59 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_60d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 89 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_90d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 119 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_120d
        FROM
            rg_bi.dwd_overseas_revenue_allproject roi
            LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
            LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
        WHERE
            roi.zone = '0'
            AND roi.app = '502'
            AND m.month_diff > 0
        GROUP BY
            roi.app_package,
            SUBSTR(roi.install_day, 1, 6),
            roi.mediasource,
            roi.game_uid,
            COALESCE(cg.country_group, 'other')
    ) u
    LEFT JOIN {view1Name} r ON u.app_package = r.app_package
    AND u.mediasource = r.mediasource
    AND u.country_group = r.country_group
    AND u.ad_type = r.ad_type
GROUP BY
    u.app_package,
    u.install_month,
    u.country_group,
    u.mediasource,
    u.ad_type;
	"""
	print(f"Executing SQL2: {sql2}")
	execSql2(sql2)

	# 加上cost列
	view3Name = f"lw_20250703_af_cost_revenue_{percentileStr}_month_view_by_j"
	sql3 = f"""
CREATE OR REPLACE VIEW {view3Name} AS
select
	before_t.app_package,
	before_t.install_month,
	before_t.country_group,
	CASE WHEN before_t.mediasource = 'tiktokglobal_int' THEN 'bytedanceglobal_int'
	ELSE before_t.mediasource
	END AS mediasource,
	before_t.ad_type,
	before_t.cost,
	before_t.revenue_d1 as before_revenue_d1,
	before_t.revenue_d3 as before_revenue_d3,
	before_t.revenue_d7 as before_revenue_d7,
	before_t.revenue_d30 as before_revenue_d30,
	before_t.revenue_d60 as before_revenue_d60,
	before_t.revenue_d90 as before_revenue_d90,
	before_t.revenue_d120 as before_revenue_d120,
	after_t.revenue_d1 as revenue_d1,
	after_t.revenue_d3 as revenue_d3,
	after_t.revenue_d7 as revenue_d7,
	after_t.revenue_d30 as revenue_d30,
	after_t.revenue_d60 as revenue_d60,
	after_t.revenue_d90 as revenue_d90,
	after_t.revenue_d120 as revenue_d120,
	ROUND(
		(before_t.revenue_d1 - after_t.revenue_d1) / before_t.revenue_d1,
		4
	) AS nerf_ratio_1d,
	ROUND(
		(before_t.revenue_d3 - after_t.revenue_d3) / before_t.revenue_d3,
		4
	) AS nerf_ratio_3d,
	ROUND(
		(before_t.revenue_d7 - after_t.revenue_d7) / before_t.revenue_d7,
		4
	) AS nerf_ratio_7d,
	ROUND(
		(before_t.revenue_d30 - after_t.revenue_d30) / before_t.revenue_d30,
		4
	) AS nerf_ratio_30d,
	ROUND(
		(before_t.revenue_d60 - after_t.revenue_d60) / before_t.revenue_d60,
		4
	) AS nerf_ratio_60d,
	ROUND(
		(before_t.revenue_d90 - after_t.revenue_d90) / before_t.revenue_d90,
		4
	) AS nerf_ratio_90d,
	ROUND(
		(before_t.revenue_d120 - after_t.revenue_d120) / before_t.revenue_d120,
		4
	) AS nerf_ratio_120d
from lw_20250703_af_cost_revenue_app_country_group_media_month_view_by_j before_t
left join {view2Name} after_t
on before_t.app_package = after_t.app_package
and before_t.install_month = after_t.install_month
and before_t.country_group = after_t.country_group
and before_t.mediasource = after_t.mediasource
and before_t.ad_type = after_t.ad_type
;
	"""
	print(f"Executing SQL3: {sql3}")
	execSql2(sql3)

	return

# 大R削弱带AdType版本
def createAfAppAdtypeNerfBigRCostRevenueMonthyView(percentile=0.99):
	percentileStr = str(percentile).replace('.', '')
	view1Name = f"lw_20250703_af_adtype_big_r_{percentileStr}_month_view_by_j"
	sql1 = f"""
CREATE OR REPLACE VIEW {view1Name} AS
SELECT
    app_package,
    mediasource,
    country_group,
    ad_type,
    percentile_approx(revenue_1d, { percentile }) AS revenue_1d_big_r,
    percentile_approx(revenue_3d, { percentile }) AS revenue_3d_big_r,
    percentile_approx(revenue_7d, { percentile }) AS revenue_7d_big_r,
    percentile_approx(revenue_30d, { percentile }) AS revenue_30d_big_r,
    percentile_approx(revenue_60d, { percentile }) AS revenue_60d_big_r,
    percentile_approx(revenue_90d, { percentile }) AS revenue_90d_big_r,
    percentile_approx(revenue_120d, { percentile }) AS revenue_120d_big_r
FROM
    (
        SELECT
            roi.app_package,
            SUBSTR(roi.install_day, 1, 6) AS install_month,
            roi.mediasource,
            roi.game_uid,
            COALESCE(cg.country_group, 'other') AS country_group,
            COALESCE(ad.ad_type, 'other') AS ad_type,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 0 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 2 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 6 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_7d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 29 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_30d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 59 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_60d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 89 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_90d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 119 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_120d
        FROM
            rg_bi.dwd_overseas_revenue_allproject roi
            LEFT JOIN (
                SELECT
                    mediasource,
                    campaign_id,
                    campaign_name
                FROM
                    dwb_overseas_mediasource_campaign_map
                GROUP BY
                    mediasource,
                    campaign_id,
                    campaign_name
            ) b ON roi.mediasource = b.mediasource
            AND roi.campaign_id = b.campaign_id
            LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
            LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
            LEFT JOIN lw_20250703_adtype_view_by_j ad ON b.campaign_name = ad.campaign_name
        WHERE
            roi.zone = '0'
            AND roi.app = '502'
            AND roi.install_day BETWEEN '20241001'
            AND '20250301'
            AND m.month_diff > 0
        GROUP BY
            app_package,
            SUBSTR(roi.install_day, 1, 6),
            COALESCE(cg.country_group, 'other'),
            roi.mediasource,
            COALESCE(ad.ad_type, 'other'),
            roi.game_uid
    ) grouped_user_revenue
GROUP BY
    app_package,
    mediasource,
    country_group,
    ad_type;
	"""
	print(f"Executing SQL1: {sql1}")
	execSql2(sql1)

	view2Name = f"lw_20250703_af_adtype_revenue_{percentileStr}_month_view_by_j"
	sql2 = f"""
CREATE
OR REPLACE VIEW { view2Name } AS
SELECT
    u.app_package,
    u.install_month,
    u.country_group,
    CASE
        WHEN u.mediasource = 'tiktokglobal_int' THEN 'bytedanceglobal_int'
        ELSE u.mediasource
    END AS mediasource,
    u.ad_type,
    SUM(LEAST(u.revenue_1d, r.revenue_1d_big_r)) AS revenue_d1,
    SUM(LEAST(u.revenue_3d, r.revenue_3d_big_r)) AS revenue_d3,
    SUM(LEAST(u.revenue_7d, r.revenue_7d_big_r)) AS revenue_d7,
    SUM(LEAST(u.revenue_30d, r.revenue_30d_big_r)) AS revenue_d30,
    SUM(LEAST(u.revenue_60d, r.revenue_60d_big_r)) AS revenue_d60,
    SUM(LEAST(u.revenue_90d, r.revenue_90d_big_r)) AS revenue_d90,
    SUM(LEAST(u.revenue_120d, r.revenue_120d_big_r)) AS revenue_d120
FROM
    (
        SELECT
            roi.app_package,
            SUBSTR(roi.install_day, 1, 6) AS install_month,
            roi.mediasource,
            roi.game_uid,
            COALESCE(cg.country_group, 'other') AS country_group,
            COALESCE(ad.ad_type, 'other') AS ad_type,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 0 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_1d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 2 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_3d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 6 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_7d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 29 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_30d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 59 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_60d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 89 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_90d,
            SUM(
                CASE
                    WHEN datediff(
                        to_date(event_day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) <= 119 THEN revenue_value_usd
                    ELSE 0
                END
            ) AS revenue_120d
        FROM
            rg_bi.dwd_overseas_revenue_allproject roi
            LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
            LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
            LEFT JOIN (
                SELECT
                    mediasource,
                    campaign_id,
                    campaign_name
                FROM
                    dwb_overseas_mediasource_campaign_map
                GROUP BY
                    mediasource,
                    campaign_id,
                    campaign_name
            ) b ON roi.mediasource = b.mediasource
            AND roi.campaign_id = b.campaign_id
            LEFT JOIN lw_20250703_adtype_view_by_j ad ON b.campaign_name = ad.campaign_name
        WHERE
            roi.zone = '0'
            AND roi.app = '502'
            AND m.month_diff > 0
        GROUP BY
            roi.app_package,
            SUBSTR(roi.install_day, 1, 6),
            roi.mediasource,
            COALESCE(ad.ad_type, 'other'),
            roi.game_uid,
            COALESCE(cg.country_group, 'other')
    ) u
    LEFT JOIN { view1Name } r ON u.app_package = r.app_package
    AND u.mediasource = r.mediasource
    AND u.country_group = r.country_group
    AND u.ad_type = r.ad_type
GROUP BY
    u.app_package,
    u.install_month,
    u.country_group,
    u.mediasource,
    u.ad_type;
	"""
	print(f"Executing SQL2: {sql2}")
	execSql2(sql2)

	view3Name = f"lw_20250703_af_adtype_cost_revenue_{percentileStr}_month_view_by_j"
	sql3 = f"""
	CREATE
OR REPLACE VIEW { view3Name } AS
select
    before_t.app_package,
    before_t.install_month,
    before_t.country_group,
    CASE
        WHEN before_t.mediasource = 'tiktokglobal_int' THEN 'bytedanceglobal_int'
        ELSE before_t.mediasource
    END AS mediasource,
    before_t.ad_type,
    before_t.cost,
    before_t.revenue_d1 as before_revenue_d1,
    before_t.revenue_d3 as before_revenue_d3,
    before_t.revenue_d7 as before_revenue_d7,
    before_t.revenue_d30 as before_revenue_d30,
    before_t.revenue_d60 as before_revenue_d60,
    before_t.revenue_d90 as before_revenue_d90,
    before_t.revenue_d120 as before_revenue_d120,
    after_t.revenue_d1 as revenue_d1,
    after_t.revenue_d3 as revenue_d3,
    after_t.revenue_d7 as revenue_d7,
    after_t.revenue_d30 as revenue_d30,
    after_t.revenue_d60 as revenue_d60,
    after_t.revenue_d90 as revenue_d90,
    after_t.revenue_d120 as revenue_d120,
    ROUND(
        (before_t.revenue_d1 - after_t.revenue_d1) / before_t.revenue_d1,
        4
    ) AS nerf_ratio_1d,
    ROUND(
        (before_t.revenue_d3 - after_t.revenue_d3) / before_t.revenue_d3,
        4
    ) AS nerf_ratio_3d,
    ROUND(
        (before_t.revenue_d7 - after_t.revenue_d7) / before_t.revenue_d7,
        4
    ) AS nerf_ratio_7d,
    ROUND(
        (before_t.revenue_d30 - after_t.revenue_d30) / before_t.revenue_d30,
        4
    ) AS nerf_ratio_30d,
    ROUND(
        (before_t.revenue_d60 - after_t.revenue_d60) / before_t.revenue_d60,
        4
    ) AS nerf_ratio_60d,
    ROUND(
        (before_t.revenue_d90 - after_t.revenue_d90) / before_t.revenue_d90,
        4
    ) AS nerf_ratio_90d,
    ROUND(
        (before_t.revenue_d120 - after_t.revenue_d120) / before_t.revenue_d120,
        4
    ) AS nerf_ratio_120d
from
    lw_20250703_af_cost_revenue_app_country_group_media_adtype_month_view_by_j before_t
    left join { view2Name } after_t on before_t.app_package = after_t.app_package
    and before_t.install_month = after_t.install_month
    and before_t.country_group = after_t.country_group
    and before_t.mediasource = after_t.mediasource
    and before_t.ad_type = after_t.ad_type;
	"""
	print(f"Executing SQL3: {sql3}")
	execSql2(sql3)

	return

def createAfAppNerfBigRDebugTable(percentile=0.99):
	percentileStr = str(percentile).replace('.', '')
	view1Name = f"lw_20250703_af_big_r_{percentileStr}_month_view_by_j"
	table1Name = f"lw_20250703_af_big_r_{percentileStr}_month_table_by_j"
	sql = f"""
DROP TABLE IF EXISTS {table1Name};
CREATE TABLE {table1Name} AS
SELECT * FROM {view1Name};
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)

	view2Name = f"lw_20250703_af_revenue_{percentileStr}_month_view_by_j"
	table2Name = f"lw_20250703_af_revenue_{percentileStr}_month_table_by_j"
	sql = f"""
DROP TABLE IF EXISTS {table2Name};
CREATE TABLE {table2Name} AS
SELECT * FROM {view2Name};
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)

	view3Name = f"lw_20250703_af_cost_revenue_{percentileStr}_month_view_by_j"
	table3Name = f"lw_20250703_af_cost_revenue_{percentileStr}_month_table_by_j"
	sql = f"""
DROP TABLE IF EXISTS {table3Name};
CREATE TABLE {table3Name} AS
SELECT * FROM {view3Name};
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)


	return

# 只分app和国家，不分媒体
def createAfAppCountryCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cost_revenue_app_country_group_month_view_by_j AS
SELECT
	app_package,
	SUBSTR(roi.install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	'ALL' AS mediasource,
	'ALL' AS ad_type,
	SUM(cost_value_usd) AS cost,
	SUM(revenue_d1) AS revenue_d1,
	SUM(revenue_d3) AS revenue_d3,
	SUM(revenue_d7) AS revenue_d7,
	SUM(revenue_d30) AS revenue_d30,
	SUM(revenue_d60) AS revenue_d60,
	SUM(revenue_d90) AS revenue_d90,
	SUM(revenue_d120) AS revenue_d120
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

# 只分app，不分媒体和国家分组
def createAfAppCostRevenueMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_af_cost_revenue_app_month_view_by_j AS
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
	revenue_d30,
	revenue_d60,
	revenue_d90,
	revenue_d120
FROM
(
		SELECT
			app_package,
			SUBSTR(install_day, 1, 6) AS install_month,
			'ALL' AS country_group,
			'ALL' AS mediasource,
			'ALL' AS ad_type,
			SUM(cost_value_usd) AS cost,
			SUM(revenue_d1) AS revenue_d1,
			SUM(revenue_d3) AS revenue_d3,
			SUM(revenue_d7) AS revenue_d7,
			SUM(revenue_d30) AS revenue_d30,
			SUM(revenue_d60) AS revenue_d60,
			SUM(revenue_d90) AS revenue_d90,
			SUM(revenue_d120) AS revenue_d120
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

def createAfCostRevenueMonthyTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_af_cost_revenue_app_month_table_by_j;
CREATE TABLE lw_20250703_af_cost_revenue_app_month_table_by_j AS
SELECT
	*,
	'af' AS tag 
FROM lw_20250703_af_cost_revenue_app_country_group_media_month_view_by_j
UNION ALL
SELECT
	*,
	'af' AS tag 
FROM lw_20250703_af_cost_revenue_app_country_group_media_adtype_month_view_by_j
UNION ALL
SELECT
	*,
	'only country' AS tag 
FROM lw_20250703_af_cost_revenue_app_country_group_month_view_by_j
UNION ALL
SELECT
	*,
	'ALL' AS tag 
FROM lw_20250703_af_cost_revenue_app_month_view_by_j;
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 汇总各种不同tag的CostRevenueMonthy数据，并建表
def createCostRevenueMonthyTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_cost_revenue_app_month_table_by_j;
CREATE TABLE lw_20250703_cost_revenue_app_month_table_by_j AS
SELECT
*
FROM lw_20250703_af_cost_revenue_app_month_table_by_j
UNION ALL
SELECT 
*
FROM lw_20250703_gpir_cost_revenue_app_month_table_by_j
UNION ALL
SELECT
	app_package,
	install_month,
	country_group,
	mediasource,
	ad_type,
	cost,
	revenue_d1,
	revenue_d3,
	revenue_d7,
	revenue_d30,
	revenue_d60,
	revenue_d90,
	revenue_d120,
	'af_nerf_big_r_0999' AS tag
FROM lw_20250703_af_cost_revenue_0999_month_view_by_j
UNION ALL
SELECT
	app_package,
	install_month,
	country_group,
	mediasource,
	ad_type,
	cost,
	revenue_d1,
	revenue_d3,
	revenue_d7,
	revenue_d30,
	revenue_d60,
	revenue_d90,
	revenue_d120,
	'af_nerf_big_r_0999' AS tag
FROM lw_20250703_af_adtype_cost_revenue_0999_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# GPIR版本的月视图
def createGPIRMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_cost_revenue_app_country_group_media_month_view_by_j AS
select
	app_package,
	SUBSTR(install_day, 1, 6) AS install_month,
	COALESCE(cg.country_group, 'other') AS country_group,
	case when mediasource in ('restricted','Facebook Ads') then 'Facebook Ads'
	else mediasource end as mediasource,
	'ALL' AS ad_type,
	sum(cost_value_usd) as cost,
	sum(revenue_d1) as revenue_d1,
	sum(revenue_d3) as revenue_d3,
	sum(revenue_d7) as revenue_d7,
	sum(revenue_d30) as revenue_d30,
	sum(revenue_d60) as revenue_d60,
	sum(revenue_d90) as revenue_d90,
	sum(revenue_d120) as revenue_d120
from
	ads_lastwar_mediasource_reattribution roi
	left join lw_country_group_table_by_j_20250703 cg on roi.country = cg.country
	LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
where
	roi.facebook_segment in ('country', 'N/A')
	AND m.month_diff > 0
group by
	app_package,
	SUBSTR(roi.install_day, 1, 6),
	country_group,
	case when mediasource in ('restricted','Facebook Ads') then 'Facebook Ads'
	else mediasource end;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createGPIRAdtypeMonthyView():
	sql = """
CREATE OR REPLACE VIEW lw_20250703_gpir_cost_revenue_app_country_group_media_adtype_month_view_by_j AS
SELECT
    app_package,
    SUBSTR(roi.install_day, 1, 6) AS install_month,
    COALESCE(cg.country_group, 'other') AS country_group,
    case when roi.mediasource in ('restricted','Facebook Ads') then 'Facebook Ads'
	else roi.mediasource end as mediasource,
    COALESCE(cog.ad_type, 'other') AS ad_type,
    SUM(roi.cost_value_usd) AS cost,
    SUM(roi.revenue_d1) AS revenue_d1,
    SUM(roi.revenue_d3) AS revenue_d3,
    SUM(roi.revenue_d7) AS revenue_d7,
    SUM(roi.revenue_d30) AS revenue_d30,
    SUM(roi.revenue_d60) AS revenue_d60,
    SUM(roi.revenue_d90) AS revenue_d90,
    SUM(roi.revenue_d120) AS revenue_d120
FROM
    ads_lastwar_mediasource_reattribution roi
    LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
    LEFT JOIN month_view_by_j m ON SUBSTR(roi.install_day, 1, 6) = m.install_month
    LEFT JOIN lw_20250703_adtype_view_by_j cog ON roi.campaign_name = cog.campaign_name
WHERE
    roi.facebook_segment IN ('country', 'N/A')
    AND m.month_diff > 0
    AND roi.mediasource IN (
        'Facebook Ads',
		'restricted',
        'googleadwords_int',
        'applovin_int'
    )
GROUP BY
	app_package,
    SUBSTR(roi.install_day, 1, 6),
    COALESCE(cg.country_group, 'other'),
    case when roi.mediasource in ('restricted','Facebook Ads') then 'Facebook Ads'
	else roi.mediasource end,
    ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createGPIRMonthyTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_gpir_cost_revenue_app_month_table_by_j;
CREATE TABLE lw_20250703_gpir_cost_revenue_app_month_table_by_j AS
SELECT
	*,
	'gpir' AS tag
FROM lw_20250703_gpir_cost_revenue_app_country_group_media_month_view_by_j
UNION ALL
SELECT
	*,
	'gpir' AS tag
FROM lw_20250703_gpir_cost_revenue_app_country_group_media_adtype_month_view_by_j
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 基于createAfAppMediaCountryCostRevenueMonthyView，过滤了本月不算，另外计算了ROI
def createRealCostAndRoiMonthyView():
	sql = """
CREATE VIEW IF NOT EXISTS lw_20250703_af_cost_roi_country_group_month_view_by_j AS
SELECT
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.cost,
	CASE
		WHEN a.cost = 0 THEN 0
		ELSE a.revenue_d1 / a.cost
	END AS roi1,
	CASE
		WHEN a.cost = 0 THEN 0
		ELSE a.revenue_d3 / a.cost
	END AS roi3,
	CASE
		WHEN a.cost = 0 THEN 0
		ELSE a.revenue_d7 / a.cost
	END AS roi7
FROM
	lw_real_cost_roi_country_group_month_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month
WHERE
	b.month_diff > 0
ORDER BY
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

def createRealCostAndRoiMonthyTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_af_cost_roi_country_group_month_table_by_j;
CREATE TABLE lw_20250703_af_cost_roi_country_group_month_table_by_j AS
SELECT * FROM lw_20250703_af_cost_roi_country_group_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 收入增长率计算，目前使用比较简单的方法
# 本月的付费增长率和上三个月（不包括本月）的平均增长率
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
		) AS last3month_r120_r90
	FROM
		ratios_with_rownum cur
		LEFT JOIN ratios_with_rownum prev ON 
		cur.app_package = prev.app_package
		AND cur.country_group = prev.country_group
		AND cur.mediasource = prev.mediasource
		AND cur.ad_type = prev.ad_type
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
		cur.r30_r7,
		cur.r60_r30,
		cur.r90_r60,
		cur.r120_r90
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
	r30_r7,
	r60_r30,
	r90_r60,
	r120_r90,
	last3month_r3_r1,
	last3month_r7_r3,
	last3month_r30_r7,
	last3month_r60_r30,
	last3month_r90_r60,
	last3month_r120_r90
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
		r30_r7,
		r60_r30,
		r90_r60,
		r120_r90,
		last3month_r3_r1,
		last3month_r7_r3,
		last3month_r30_r7,
		last3month_r60_r30,
		last3month_r90_r60,
		last3month_r120_r90,
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
	cur.r30_r7,
	cur.r60_r30,
	cur.r90_r60,
	cur.r120_r90,
	cur.last3month_r3_r1,
	cur.last3month_r7_r3,
	cur.last3month_r30_r7,
	cur.last3month_r60_r30,
	cur.last3month_r90_r60,
	cur.last3month_r120_r90,
	-- 本行的预测值
	cur.last3month_r3_r1 AS predict_r3_r1,
	cur.last3month_r7_r3 AS predict_r7_r3,
	cur.last3month_r30_r7 AS predict_r30_r7,
	-- 上一行的预测值
	COALESCE(prev1.last3month_r60_r30, 0) AS predict_r60_r30,
	-- 上两行的预测值
	COALESCE(prev2.last3month_r90_r60, 0) AS predict_r90_r60,
	-- 上三行的预测值
	COALESCE(prev3.last3month_r120_r90, 0) AS predict_r120_r90
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
	sql = """
DROP TABLE IF EXISTS lw_20250703_af_revenue_rise_ratio_predict_month_table_by_j;
CREATE TABLE lw_20250703_af_revenue_rise_ratio_predict_month_table_by_j AS
SELECT * FROM lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 创建 KPI 视图
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
			*,
			CASE
				WHEN country_group = 'US' THEN 1.45
				WHEN country_group = 'KR' THEN 1.58
				WHEN country_group = 'JP' THEN 1.66
				WHEN country_group = 'GCC' THEN 1.45
				WHEN country_group = 'T1' THEN 1.65
				ELSE 1.56
			END AS kpi_target
		FROM
			lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j
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

def createKpiTable():
	sql = """
DROP TABLE IF EXISTS lw_20250703_af_kpi_month_table_by_j;
CREATE TABLE lw_20250703_af_kpi_month_table_by_j AS
SELECT * FROM lw_20250703_af_kpi_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 创建 KPI2 视图
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
		*,
		CASE
			WHEN country_group = 'US' THEN 1.45
			WHEN country_group = 'KR' THEN 1.58
			WHEN country_group = 'JP' THEN 1.66
			WHEN country_group = 'GCC' THEN 1.45
			ELSE 1.65
		END AS kpi_target
	FROM
		lw_20250703_af_revenue_rise_ratio_predict_month_view_by_j
)
SELECT
	r.app_package,
	r.install_month,
	r.country_group,
	r.mediasource,
	r.ad_type,
	r.tag,
	p.kpi_target,
	-- kpi_30
	CASE
		WHEN r.roi30 = 0 THEN NULL
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
		WHEN r.roi30 = 0 THEN NULL
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
		WHEN r.roi30 = 0 THEN NULL
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
		WHEN r.roi60 = 0 THEN NULL
		ELSE ROUND(
			r.roi1 * (
				p.kpi_target / (r.roi60 * p.predict_r90_r60 * p.predict_r120_r90)
			),
			4
		)
	END AS kpi1_60,
	CASE
		WHEN r.roi60 = 0 THEN NULL
		ELSE ROUND(
			r.roi3 * (
				p.kpi_target / (r.roi60 * p.predict_r90_r60 * p.predict_r120_r90)
			),
			4
		)
	END AS kpi3_60,
	CASE
		WHEN r.roi60 = 0 THEN NULL
		ELSE ROUND(
			r.roi7 * (
				p.kpi_target / (r.roi60 * p.predict_r90_r60 * p.predict_r120_r90)
			),
			4
		)
	END AS kpi7_60,
	-- kpi_90
	CASE
		WHEN r.roi90 = 0 THEN NULL
		ELSE ROUND(
			r.roi1 * (p.kpi_target / (r.roi90 * p.predict_r120_r90)),
			4
		)
	END AS kpi1_90,
	CASE
		WHEN r.roi90 = 0 THEN NULL
		ELSE ROUND(
			r.roi3 * (p.kpi_target / (r.roi90 * p.predict_r120_r90)),
			4
		)
	END AS kpi3_90,
	CASE
		WHEN r.roi90 = 0 THEN NULL
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
		ELSE NULL
	END AS d_kpi1,
	CASE
		WHEN b.month_diff >= 5 THEN a.kpi3_120
		WHEN b.month_diff = 4 THEN a.kpi3_90
		WHEN b.month_diff = 3 THEN a.kpi3_60
		WHEN b.month_diff = 2 THEN a.kpi3_30
		ELSE NULL
	END AS d_kpi3,
	CASE
		WHEN b.month_diff >= 5 THEN a.kpi7_120
		WHEN b.month_diff = 4 THEN a.kpi7_90
		WHEN b.month_diff = 3 THEN a.kpi7_60
		WHEN b.month_diff = 2 THEN a.kpi7_30
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
	sql = """
DROP TABLE IF EXISTS lw_20250703_af_kpi2_fix_month_table_by_j;
CREATE TABLE lw_20250703_af_kpi2_fix_month_table_by_j AS
SELECT * FROM lw_20250703_af_kpi2_fix_month_view_by_j;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


# 针对 lw_revenue_rise_ratio_country_group_month_predict_view_by_j 视图创建 MAPE 视图
def createMapeView():
	sql = """
CREATE OR REPLACE VIEW lw_revenue_rise_ratio_month_predict_mape_view_by_j AS
SELECT
	app_package,
	install_month,
	country_group,
	mediasource,
	ad_type,
	tag,
	-- MAPE1计算 (real vs predict)
	CASE
		WHEN (r3_r1 * r7_r3 * r30_r7 * r60_r30 * r90_r60 * r120_r90) = 0 THEN NULL
		ELSE ABS(
			(r3_r1 * r7_r3 * r30_r7 * r60_r30 * r90_r60 * r120_r90) - (
				predict_r3_r1 * predict_r7_r3 * predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
			)
		) / (r3_r1 * r7_r3 * r30_r7 * r60_r30 * r90_r60 * r120_r90)
	END AS MAPE1,
	-- MAPE3计算 (real vs predict)
	CASE
		WHEN (r7_r3 * r30_r7 * r60_r30 * r90_r60 * r120_r90) = 0 THEN NULL
		ELSE ABS(
			(r7_r3 * r30_r7 * r60_r30 * r90_r60 * r120_r90) - (
				predict_r7_r3 * predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
			)
		) / (r7_r3 * r30_r7 * r60_r30 * r90_r60 * r120_r90)
	END AS MAPE3,
	-- MAPE7计算 (real vs predict)
	CASE
		WHEN (r30_r7 * r60_r30 * r90_r60 * r120_r90) = 0 THEN NULL
		ELSE ABS(
			(r30_r7 * r60_r30 * r90_r60 * r120_r90) - (
				predict_r30_r7 * predict_r60_r30 * predict_r90_r60 * predict_r120_r90
			)
		) / (r30_r7 * r60_r30 * r90_r60 * r120_r90)
	END AS MAPE7
FROM
	lw_20250703_af_revenue_rise_ratio_predict_month_table_by_j
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return

# 由于误差需要有了完整120天的数据才能计算
# 所以将不满120天的数据过滤掉
def createMapeViewFix():
	sql = """
CREATE OR REPLACE VIEW lw_revenue_rise_ratio_country_group_month_predict_mape_fix_view_by_j AS
SELECT
	a.app_package,
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
	a.tag,
	b.month_diff,
	a.MAPE1,
	a.MAPE3,
	a.MAPE7
FROM
	lw_revenue_rise_ratio_month_predict_mape_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month
WHERE
	b.month_diff >= 5
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


def createMapeTable():
	sql = """
DROP TABLE IF EXISTS lw_revenue_rise_ratio_country_group_month_predict_mape_table_by_j;
CREATE TABLE lw_revenue_rise_ratio_country_group_month_predict_mape_table_by_j AS
SELECT
	app_package,
	country_group,
	mediasource,
	ad_type,
	tag,
	avg(MAPE1) AS MAPE1,
	avg(MAPE3) AS MAPE3,
	avg(MAPE7) AS MAPE7
FROM lw_revenue_rise_ratio_country_group_month_predict_mape_fix_view_by_j
WHERE
	month_diff <= 10
GROUP BY
	app_package,
	country_group,
	mediasource,
	ad_type,
	tag
;
	"""
	print(f"Executing SQL: {sql}")
	execSql2(sql)
	return


def getMapeData(startMonthStr, endMonthStr):
	sql = f"""
SELECT
	install_month,
	country_group,
	mediasource,
	ad_type,
	avg(MAPE1) as MAPE1,
	avg(MAPE3) as MAPE3,
	avg(MAPE7) as MAPE7
FROM
	lw_revenue_rise_ratio_country_group_month_predict_mape_fix_view_by_j
WHERE
	install_month BETWEEN '{startMonthStr}' AND '{endMonthStr}'
GROUP BY
	install_month,
	country_group,
	mediasource,
	ad_type
;
	"""
	print(f"Executing SQL: {sql}")
	df = execSql(sql)
	# print(df)
	return df


# 有先后顺序，依赖关系
def createViewsAndTables():
	# createCountryGroupTable()

	createAdtypeView()

	# createAfAppMediaCountryCostRevenueMonthyView()
	# createAfAppMediaCountryAdtypeCostRevenueMonthyView()
	# createAfAppCountryCostRevenueMonthyView()
	# createAfAppCostRevenueMonthyView()
	
	# createAfCostRevenueMonthyTable()

	createGPIRMonthyView()
	createGPIRAdtypeMonthyView()
	createGPIRMonthyTable()

	createAfAppNerfBigRCostRevenueMonthyView(percentile=0.999)
	createAfAppAdtypeNerfBigRCostRevenueMonthyView(percentile=0.999)

	createCostRevenueMonthyTable()

	# createRevenueRiseRatioView()
	# createPredictRevenueRiseRatioView()
	createPredictRevenueRiseRatioTable()

	# createKpiView()
	createKpiTable()

	# createOrganic2MonthView()
	# createOrganic2MonthTable()

	createGPIROrganic2MonthView()
	createGPIROrganic2MonthTable()

	# createKpi2View()
	# createKpi2ViewFix()
	createKpi2FixTable()

	# createOrganic2MonthViewForDebug()
	# createOrganic2MonthTableForDebug()

	# createGPIROrganic2MonthViewForDebug()
	# createGPIROrganic2MonthTableForDebug()

	# createMapeView()
	# createMapeViewFix()
	# createMapeTable()

	# createAfAppNerfBigRDebugTable(percentile=0.999)


	

	pass

# 执行过createViewsAndTables后，就不需要反复创建views了，这样会快一点
def createTables():
	createAfCostRevenueMonthyTable()
	createPredictRevenueRiseRatioTable()
	createKpiTable()
	createOrganic2MonthTable()
	createGPIROrganic2MonthTable()
	createGPIRMonthyTable()
	createKpi2FixTable()
	createOrganic2MonthTableForDebug()
	createGPIROrganic2MonthTableForDebug()

# 生成一些计算指标

def main(dayStr=None):
	# adTypeDebug()
	createViewsAndTables()

	
	

	# # 每月的7日执行一次，如果不是7日，则不执行
	# if dayStr:
	# 	today = datetime.datetime.strptime(dayStr, '%Y%m%d').date()
	# else:
	# 	today = datetime.date.today()
	# if today.day == 7:
	# 	print(f"Today is {today}, executing the monthly tasks.")
	# 	createTables()
	# else:
	# 	print(f"Today is {today}, not the 7th day of the month. Skipping execution.")


if __name__ == "__main__":
	main()

	# mapeDf = getMapeData('202406', '202506')
	# mapeDf = mapeDf.groupby(['country_group', 'mediasource', 'ad_type']).mean().reset_index()
	# mediaList = ['Facebook Ads', 'googleadwords_int','moloco_int','bytedanceglobal_int','applovin_int']
	# mapeDf = mapeDf[mapeDf['mediasource'].isin(mediaList)]
	# # print(mapeDf)
	# mapeDf.to_csv('/src/data/lw_revenue_month_mape.csv', index=False)
		

	
# new修改，去掉广告类型，剩下的一切一样
import os
import datetime
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

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


# 为了减少代码修改数量，仍旧沿用ad_type字段，只是不再区分广告类型，全部归为'other'
# 所有的表名字去掉_ad_type

# 直接使用bi数据，注意这是AF归因，不是GPIR
def createRealMonthyView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_real_cost_roi_country_group_month_view_by_j AS
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
	'other' AS ad_type,
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
	and facebook_segment in ('country', 'N/A')
group by
	install_month,
	country_group,
	mediasource,
	ad_type;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 基于createRealMonthyView，过滤了本月不算，另外计算了ROI
def createRealCostAndRoiMonthyView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_real_cost_roi2_country_group_month_view_by_j AS
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

# 收入增长率计算，目前使用比较简单的方法
# 每个分组最近3个月的收入增长率的均值作为预测值
def createRevenueRiseRatioView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_revenue_rise_ratio_country_group_month_view_by_j AS
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
		lw_real_cost_roi_country_group_month_view_by_j
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
last3month_ratios AS (
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
	last3month_r3_r1,
	last3month_r7_r3,
	last3month_r30_r7,
	last3month_r60_r30,
	last3month_r90_r60,
	last3month_r120_r90
FROM
	last3month_ratios
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

# 由于满日数据问题，当月数据不完整，需要使用之前数据完成预测。
def createPredictRevenueRiseRatioView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_revenue_rise_ratio_country_group_month_predict_view_by_j AS
WITH base AS (
	SELECT
		country_group,
		mediasource,
		ad_type,
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
			PARTITION BY country_group,
			mediasource,
			ad_type
			ORDER BY
				install_month
		) AS row_num
	FROM
		lw_revenue_rise_ratio_country_group_month_view_by_j
)
SELECT
	cur.country_group,
	cur.mediasource,
	cur.ad_type,
	cur.install_month,
	cur.r3_r1,
	cur.r7_r3,
	cur.r30_r7,
	cur.r60_r30,
	cur.r90_r60,
	cur.r120_r90,
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
	LEFT JOIN base prev1 ON cur.country_group = prev1.country_group
	AND cur.mediasource = prev1.mediasource
	AND cur.ad_type = prev1.ad_type
	AND cur.row_num = prev1.row_num + 1
	LEFT JOIN base prev2 ON cur.country_group = prev2.country_group
	AND cur.mediasource = prev2.mediasource
	AND cur.ad_type = prev2.ad_type
	AND cur.row_num = prev2.row_num + 2
	LEFT JOIN base prev3 ON cur.country_group = prev3.country_group
	AND cur.mediasource = prev3.mediasource
	AND cur.ad_type = prev3.ad_type
	AND cur.row_num = prev3.row_num + 3
ORDER BY
	cur.country_group,
	cur.mediasource,
	cur.ad_type,
	cur.install_month;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 针对 lw_revenue_rise_ratio_country_group_month_predict_view_by_j 视图创建 MAPE 视图
def createMapeView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_revenue_rise_ratio_country_group_month_predict_mape_view_by_j AS
SELECT
    install_month,
    country_group,
    mediasource,
    ad_type,
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
    lw_revenue_rise_ratio_country_group_month_predict_view_by_j
ORDER BY
    country_group,
    mediasource,
    ad_type,
    install_month;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 由于误差需要有了完整120天的数据才能计算
# 所以将不满120天的数据过滤掉
def createMapeViewFix():
    sql = """
CREATE VIEW IF NOT EXISTS lw_revenue_rise_ratio_country_group_month_predict_mape_fix_view_by_j AS
SELECT
    a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
    a.MAPE1,
    a.MAPE3,
    a.MAPE7
FROM
    lw_revenue_rise_ratio_country_group_month_predict_mape_view_by_j a
    INNER JOIN month_view_by_j b ON a.install_month = b.install_month
WHERE
    b.month_diff >= 5
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
    
# 创建 KPI 视图
# 其中回本是按照佳玥给出的表格进行计算的
def createKpiView():
    sql = """
CREATE VIEW IF NOT EXISTS lw_kpi_country_group_month_view_by_j AS
SELECT
	country_group,
	mediasource,
	ad_type,
	install_month,
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
			lw_revenue_rise_ratio_country_group_month_predict_view_by_j
	) t
ORDER BY
	country_group,
	mediasource,
	ad_type,
	install_month;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 创建 KPI2 视图
# 动态KPI，是根据30日、60日、90日的ROI来计算的
def createKpi2View():
    sql = """
CREATE VIEW IF NOT EXISTS lw_kpi2_country_group_month_view_by_j AS WITH roi_base AS (
	SELECT
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
		lw_real_cost_roi_country_group_month_view_by_j
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
		lw_revenue_rise_ratio_country_group_month_predict_view_by_j
)
SELECT
	r.install_month,
	r.country_group,
	r.mediasource,
	r.ad_type,
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
	LEFT JOIN predict_base p ON r.install_month = p.install_month
	AND r.country_group = p.country_group
	AND r.mediasource = p.mediasource
	AND r.ad_type = p.ad_type
ORDER BY
	r.country_group,
	r.mediasource,
	r.ad_type,
	r.install_month
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 动态KPI，修正，将不完整数据的月份过滤掉
# 这里的逻辑是基于 lw_kpi_country_group_month_view_by_j 视图进行修正
def createKpi2ViewFix():
    sql = """
CREATE VIEW IF NOT EXISTS lw_kpi2_fix_country_group_month_view_by_j AS
SELECT
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type,
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
	lw_kpi2_country_group_month_view_by_j a
	INNER JOIN month_view_by_j b ON a.install_month = b.install_month
ORDER BY
	a.install_month,
	a.country_group,
	a.mediasource,
	a.ad_type;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return


def allInOne():
    o = getO()

    table_name = 'lw_android_kpi_country_group_month_view_by_j_20250702'

    # 判断表是否存在
    if not o.exist_table(table_name):
        # 不存在则创建表
        sql = '''
        CREATE TABLE {0} AS
        SELECT
            a.install_month,
            a.country_group,
            a.mediasource,
            a.ad_type,
            a.cost,
            a.roi1,
            a.roi3,
            a.roi7,
            b.kpi1,
            b.kpi3,
            b.kpi7,
            c.d_kpi1,
            c.d_kpi3,
            c.d_kpi7,
            d.MAPE1,
            d.MAPE3,
            d.MAPE7
        FROM lw_real_cost_roi2_country_group_month_view_by_j a
        LEFT JOIN lw_kpi_country_group_month_view_by_j b
            ON a.install_month = b.install_month
            AND a.country_group = b.country_group
            AND a.mediasource = b.mediasource
            AND a.ad_type = b.ad_type
        LEFT JOIN lw_kpi2_fix_country_group_month_view_by_j c
            ON a.install_month = c.install_month
            AND a.country_group = c.country_group
            AND a.mediasource = c.mediasource
            AND a.ad_type = c.ad_type
        LEFT JOIN lw_revenue_rise_ratio_country_group_month_predict_mape_fix_view_by_j d
            ON a.install_month = d.install_month
            AND a.country_group = d.country_group
            AND a.mediasource = d.mediasource
            AND a.ad_type = d.ad_type;
        '''.format(table_name)
    else:
        # 存在则更新表
        sql = '''
        INSERT OVERWRITE TABLE {0}
        SELECT
            a.install_month,
            a.country_group,
            a.mediasource,
            a.ad_type,
            a.cost,
            a.roi1,
            a.roi3,
            a.roi7,
            b.kpi1,
            b.kpi3,
            b.kpi7,
            c.d_kpi1,
            c.d_kpi3,
            c.d_kpi7,
            d.MAPE1,
            d.MAPE3,
            d.MAPE7
        FROM lw_real_cost_roi2_country_group_month_view_by_j a
        LEFT JOIN lw_kpi_country_group_month_view_by_j b
            ON a.install_month = b.install_month
            AND a.country_group = b.country_group
            AND a.mediasource = b.mediasource
            AND a.ad_type = b.ad_type
        LEFT JOIN lw_kpi2_fix_country_group_month_view_by_j c
            ON a.install_month = c.install_month
            AND a.country_group = c.country_group
            AND a.mediasource = c.mediasource
            AND a.ad_type = c.ad_type
        LEFT JOIN lw_revenue_rise_ratio_country_group_month_predict_mape_fix_view_by_j d
            ON a.install_month = d.install_month
            AND a.country_group = d.country_group
            AND a.mediasource = d.mediasource
            AND a.ad_type = d.ad_type;
        '''.format(table_name)

    # 执行SQL
    instance = o.execute_sql(sql)
    instance.wait_for_success()



def main(dayStr=None):
    # createMonthView()
    # createRealMonthyView()
    # createRealCostAndRoiMonthyView()
    # createRevenueRiseRatioView()
    # createPredictRevenueRiseRatioView()
    # createMapeView()
    # createMapeViewFix()
    # createKpiView()
    # createKpi2View()
    # createKpi2ViewFix()

	allInOne()

    # # 每月的7日执行一次，如果不是7日，则不执行
    # if dayStr:
    #     today = datetime.datetime.strptime(dayStr, '%Y%m%d').date()
    # else:
    #     today = datetime.date.today()
    # if today.day == 7:
    #     print(f"Today is {today}, executing the monthly tasks.")
    #     allInOne()
    # else:
    #     print(f"Today is {today}, not the 7th day of the month. Skipping execution.")


if __name__ == "__main__":
    main()

    # mapeDf = getMapeData('202406', '202506')
    # mapeDf = mapeDf.groupby(['country_group', 'mediasource', 'ad_type']).mean().reset_index()
    # mediaList = ['Facebook Ads', 'googleadwords_int','moloco_int','bytedanceglobal_int','applovin_int']
    # mapeDf = mapeDf[mapeDf['mediasource'].isin(mediaList)]
    # # print(mapeDf)
    # mapeDf.to_csv('/src/data/lw_revenue_month_mape.csv', index=False)
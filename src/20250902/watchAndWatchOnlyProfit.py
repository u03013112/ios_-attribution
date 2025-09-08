import pandas as pd

import sys
sys.path.append('/src')
from src.dataBricks import execSql, execSql2

# 基础数据，只是将国家分组，将applovin分成d7和d28
def createAosGpirCohortOnlyProfitRawView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_aos_gpir_cohort_onlyprofit_raw_view_by_j as
SELECT
	'com.fun.lastwar.gp' as app_package,	
	COALESCE(cg.country_group, 'other') AS country_group,
	case
	when roi.mediasource = 'applovin_int' and pub.campaign_name like '%D7%' then 'applovin_int_d7'
	when roi.mediasource = 'applovin_int' and pub.campaign_name like '%D28%' then 'applovin_int_d28'
	when roi.mediasource = 'applovin_int' then 'applovin_int'
	else roi.mediasource end as mediasource,
	'aos_gpir_cohort_onlyprofit_raw' as tag,
    roi.install_day,
	sum(cost_value_usd) as cost,
	SUM(revenue_h24) AS revenue_d1,
	SUM(revenue_h72) AS revenue_d3,
	SUM(revenue_h168) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d135) AS revenue_d135,
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
	1,2,3,4,5
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return


def createIosAfCohortOnlyProfitRawView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_ios_af_cohort_onlyprofit_raw_view_by_j as
SELECT
	'id6448786147' as app_package,
	roi.install_day,
	COALESCE(cg.country_group, 'other') AS country_group,
	CASE
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
        WHEN mediasource in ('Organic', 'organic') THEN 'Organic'
		ELSE mediasource
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
    SUM(revenue_cohort_d135) AS revenue_d135,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_roi_profit roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
	LEFT JOIN (
		SELECT
			campaign_id,
			MAX(campaign_name) AS campaign_name
		FROM
			prodb.public.applovin_campaign_info_new
		GROUP BY
			campaign_id
	) pub ON roi.campaign_id = pub.campaign_id
WHERE
	roi.app_package in ('id6448786147', 'id6736925794')
GROUP BY
	roi.install_day,
	COALESCE(cg.country_group, 'other'),
	CASE
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
		WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
        WHEN mediasource in ('Organic', 'organic') THEN 'Organic'
		ELSE mediasource
	END,
	ad_type;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createIosAfCohortOnlyProfitFixTable():
	sql2 = """
CREATE OR REPLACE TABLE lw_20250903_ios_af_cohort_onlyprofit_fix_view_by_j AS
WITH base_data AS (
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
        revenue_d135,
		revenue_d150
	FROM lw_20250903_ios_af_cohort_onlyprofit_raw_view_by_j
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
        SUM(revenue_d135) AS total_paid_revenue_d135,
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
        SUM(revenue_d135) AS total_revenue_d135,
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
        bd.revenue_d135 AS original_revenue_d135,
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
                    pmt.total_paid_revenue_d135 * (bd.cost / pmt.total_paid_cost)
                ELSE 0 END
            ELSE bd.revenue_d135
        END AS adjusted_revenue_d135,
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
        SUM(adjusted_revenue_d135) AS total_adjusted_paid_revenue_d135,
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
            GREATEST(0, dt.total_revenue_d135 - COALESCE(apt.total_adjusted_paid_revenue_d135, 0))
        ELSE amd.adjusted_revenue_d135
    END AS revenue_d135,
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

def createIosAfCohortOnlyProfitFitTable():
	sql2 = """
CREATE OR REPLACE TABLE lw_20250903_ios_af_cohort_onlyprofit_fit_view_by_j AS
WITH base_data AS (
	SELECT
		app_package,
		install_day,
		country_group,
		mediasource,
		cost,
        revenue_d1,
        revenue_d3,
		revenue_d7,
        revenue_d14,
        revenue_d30,
        revenue_d60,
        revenue_d90,
		revenue_d120,
        revenue_d135,
        revenue_d150
	FROM lw_20250903_ios_af_cohort_onlyprofit_fix_view_by_j
	WHERE mediasource != 'Organic'
),
original_totals AS (
	SELECT
		app_package,
		install_day,
		country_group,
        SUM(revenue_d1) AS total_original_revenue_d1,
        SUM(revenue_d3) AS total_original_revenue_d3,
		SUM(revenue_d7) AS total_original_revenue_d7,
        SUM(revenue_d14) AS total_original_revenue_d14,
        SUM(revenue_d30) AS total_original_revenue_d30,
        SUM(revenue_d60) AS total_original_revenue_d60,
        SUM(revenue_d90) AS total_original_revenue_d90,
		SUM(revenue_d120) AS total_original_revenue_d120,
        SUM(revenue_d135) AS total_original_revenue_d135,
        SUM(revenue_d150) AS total_original_revenue_d150
	FROM lw_20250903_ios_af_cohort_onlyprofit_fix_view_by_j
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
	WHERE tag IN ('20250808_20')
),
fitted_paid_media AS (
	-- 对付费媒体应用拟合系数
	SELECT
		bd.app_package,
		bd.install_day,
		bd.country_group,
		bd.mediasource,
		bd.cost,
		br.tag,
		CASE
			WHEN bd.mediasource = 'applovin_int_d7' THEN 
				bd.revenue_d1 * COALESCE(br.applovin_int_d7_coeff, 1.0)
			WHEN bd.mediasource = 'applovin_int_d28' THEN 
				bd.revenue_d1 * COALESCE(br.applovin_int_d28_coeff, 1.0)
			WHEN bd.mediasource = 'Facebook Ads' THEN 
				bd.revenue_d1 * COALESCE(br.facebook_ads_coeff, 1.0)
			WHEN bd.mediasource = 'moloco_int' THEN 
				bd.revenue_d1 * COALESCE(br.moloco_int_coeff, 1.0)
			WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
				bd.revenue_d1 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
			ELSE bd.revenue_d1
		END AS fitted_revenue_d1,
        CASE
            WHEN bd.mediasource = 'applovin_int_d7' THEN 
                bd.revenue_d3 * COALESCE(br.applovin_int_d7_coeff, 1.0)
            WHEN bd.mediasource = 'applovin_int_d28' THEN 
                bd.revenue_d3 * COALESCE(br.applovin_int_d28_coeff, 1.0)
            WHEN bd.mediasource = 'Facebook Ads' THEN 
                bd.revenue_d3 * COALESCE(br.facebook_ads_coeff, 1.0)
            WHEN bd.mediasource = 'moloco_int' THEN 
                bd.revenue_d3 * COALESCE(br.moloco_int_coeff, 1.0)
            WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
                bd.revenue_d3 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
            ELSE bd.revenue_d3
        END AS fitted_revenue_d3,
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
                bd.revenue_d14 * COALESCE(br.applovin_int_d7_coeff, 1.0)
            WHEN bd.mediasource = 'applovin_int_d28' THEN 
                bd.revenue_d14 * COALESCE(br.applovin_int_d28_coeff, 1.0)
            WHEN bd.mediasource = 'Facebook Ads' THEN 
                bd.revenue_d14 * COALESCE(br.facebook_ads_coeff, 1.0)
            WHEN bd.mediasource = 'moloco_int' THEN 
                bd.revenue_d14 * COALESCE(br.moloco_int_coeff, 1.0)
            WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
                bd.revenue_d14 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
            ELSE bd.revenue_d14
        END AS fitted_revenue_d14,
        CASE
            WHEN bd.mediasource = 'applovin_int_d7' THEN 
                bd.revenue_d30 * COALESCE(br.applovin_int_d7_coeff, 1.0)
            WHEN bd.mediasource = 'applovin_int_d28' THEN 
                bd.revenue_d30 * COALESCE(br.applovin_int_d28_coeff, 1.0)
            WHEN bd.mediasource = 'Facebook Ads' THEN 
                bd.revenue_d30 * COALESCE(br.facebook_ads_coeff, 1.0)
            WHEN bd.mediasource = 'moloco_int' THEN 
                bd.revenue_d30 * COALESCE(br.moloco_int_coeff, 1.0)
            WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
                bd.revenue_d30 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
            ELSE bd.revenue_d30
        END AS fitted_revenue_d30,
        CASE
            WHEN bd.mediasource = 'applovin_int_d7' THEN 
                bd.revenue_d60 * COALESCE(br.applovin_int_d7_coeff, 1.0)
            WHEN bd.mediasource = 'applovin_int_d28' THEN 
                bd.revenue_d60 * COALESCE(br.applovin_int_d28_coeff, 1.0)
            WHEN bd.mediasource = 'Facebook Ads' THEN 
                bd.revenue_d60 * COALESCE(br.facebook_ads_coeff, 1.0)
            WHEN bd.mediasource = 'moloco_int' THEN 
                bd.revenue_d60 * COALESCE(br.moloco_int_coeff, 1.0)
            WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
                bd.revenue_d60 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
            ELSE bd.revenue_d60
        END AS fitted_revenue_d60,
        CASE
            WHEN bd.mediasource = 'applovin_int_d7' THEN 
                bd.revenue_d90 * COALESCE(br.applovin_int_d7_coeff, 1.0)
            WHEN bd.mediasource = 'applovin_int_d28' THEN 
                bd.revenue_d90 * COALESCE(br.applovin_int_d28_coeff, 1.0)
            WHEN bd.mediasource = 'Facebook Ads' THEN 
                bd.revenue_d90 * COALESCE(br.facebook_ads_coeff, 1.0)
            WHEN bd.mediasource = 'moloco_int' THEN 
                bd.revenue_d90 * COALESCE(br.moloco_int_coeff, 1.0)
            WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
                bd.revenue_d90 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
            ELSE bd.revenue_d90
        END AS fitted_revenue_d90,
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
		END AS fitted_revenue_d120,
        CASE
            WHEN bd.mediasource = 'applovin_int_d7' THEN 
                bd.revenue_d135 * COALESCE(br.applovin_int_d7_coeff, 1.0)
            WHEN bd.mediasource = 'applovin_int_d28' THEN 
                bd.revenue_d135 * COALESCE(br.applovin_int_d28_coeff, 1.0)
            WHEN bd.mediasource = 'Facebook Ads' THEN 
                bd.revenue_d135 * COALESCE(br.facebook_ads_coeff, 1.0)
            WHEN bd.mediasource = 'moloco_int' THEN 
                bd.revenue_d135 * COALESCE(br.moloco_int_coeff, 1.0)
            WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
                bd.revenue_d135 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
            ELSE bd.revenue_d135
        END AS fitted_revenue_d135,
        CASE
            WHEN bd.mediasource = 'applovin_int_d7' THEN 
                bd.revenue_d150 * COALESCE(br.applovin_int_d7_coeff, 1.0)
            WHEN bd.mediasource = 'applovin_int_d28' THEN 
                bd.revenue_d150 * COALESCE(br.applovin_int_d28_coeff, 1.0)
            WHEN bd.mediasource = 'Facebook Ads' THEN 
                bd.revenue_d150 * COALESCE(br.facebook_ads_coeff, 1.0)
            WHEN bd.mediasource = 'moloco_int' THEN 
                bd.revenue_d150 * COALESCE(br.moloco_int_coeff, 1.0)
            WHEN bd.mediasource = 'bytedanceglobal_int' THEN 
                bd.revenue_d150 * COALESCE(br.bytedanceglobal_int_coeff, 1.0)
            ELSE bd.revenue_d150
        END AS fitted_revenue_d150
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
        SUM(fitted_revenue_d1) AS total_fitted_paid_revenue_d1,
        SUM(fitted_revenue_d3) AS total_fitted_paid_revenue_d3,
		SUM(fitted_revenue_d7) AS total_fitted_paid_revenue_d7,
        SUM(fitted_revenue_d14) AS total_fitted_paid_revenue_d14,
        SUM(fitted_revenue_d30) AS total_fitted_paid_revenue_d30,
        SUM(fitted_revenue_d60) AS total_fitted_paid_revenue_d60,
        SUM(fitted_revenue_d90) AS total_fitted_paid_revenue_d90,
		SUM(fitted_revenue_d120) AS total_fitted_paid_revenue_d120,
        SUM(fitted_revenue_d135) AS total_fitted_paid_revenue_d135,
        SUM(fitted_revenue_d150) AS total_fitted_paid_revenue_d150
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
		0.0 as cost,
		br.tag,
		GREATEST(0, ot.total_original_revenue_d1 - COALESCE(fpt.total_fitted_paid_revenue_d1, 0)) as fitted_revenue_d1,
        GREATEST(0, ot.total_original_revenue_d3 - COALESCE(fpt.total_fitted_paid_revenue_d3, 0)) as fitted_revenue_d3,
        GREATEST(0, ot.total_original_revenue_d7 - COALESCE(fpt.total_fitted_paid_revenue_d7, 0)) as fitted_revenue_d7,
        GREATEST(0, ot.total_original_revenue_d14 - COALESCE(fpt.total_fitted_paid_revenue_d14, 0)) as fitted_revenue_d14,
        GREATEST(0, ot.total_original_revenue_d30 - COALESCE(fpt.total_fitted_paid_revenue_d30, 0)) as fitted_revenue_d30,
        GREATEST(0, ot.total_original_revenue_d60 - COALESCE(fpt.total_fitted_paid_revenue_d60, 0)) as fitted_revenue_d60,
        GREATEST(0, ot.total_original_revenue_d90 - COALESCE(fpt.total_fitted_paid_revenue_d90, 0)) as fitted_revenue_d90,
		GREATEST(0, ot.total_original_revenue_d120 - COALESCE(fpt.total_fitted_paid_revenue_d120, 0)) as fitted_revenue_d120,
        GREATEST(0, ot.total_original_revenue_d135 - COALESCE(fpt.total_fitted_paid_revenue_d135, 0)) as fitted_revenue_d135,
        GREATEST(0, ot.total_original_revenue_d150 - COALESCE(fpt.total_fitted_paid_revenue_d150, 0)) as fitted_revenue_d150
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
	country_group,
	mediasource,
	tag,
    install_day,
	cost,
    fitted_revenue_d1 as revenue_d1,
    fitted_revenue_d3 as revenue_d3,
	fitted_revenue_d7 as revenue_d7,
    fitted_revenue_d14 as revenue_d14,
    fitted_revenue_d30 as revenue_d30,
    fitted_revenue_d60 as revenue_d60,
    fitted_revenue_d90 as revenue_d90,
	fitted_revenue_d120 as revenue_d120,
    fitted_revenue_d135 as revenue_d135,
    fitted_revenue_d150 as revenue_d150
FROM fitted_paid_media

UNION ALL

SELECT
	app_package,
	country_group,
	mediasource,
	tag,
    install_day,
	cost,
	fitted_revenue_d1 as revenue_d1,
    fitted_revenue_d3 as revenue_d3,
	fitted_revenue_d7 as revenue_d7,
    fitted_revenue_d14 as revenue_d14,
    fitted_revenue_d30 as revenue_d30,
    fitted_revenue_d60 as revenue_d60,
    fitted_revenue_d90 as revenue_d90,
	fitted_revenue_d120 as revenue_d120,
    fitted_revenue_d135 as revenue_d135,
    fitted_revenue_d150 as revenue_d150
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




def createAvgNView(N=21):
    viewName = f"lw_20250903_cohort_onlyprofit_avg_{N}_view_by_j"
    sql = f"""
CREATE OR REPLACE VIEW {viewName} as
with base_data as (
    select
        app_package,
        country_group,
        mediasource,
        tag,
        install_day,
        cost,
        revenue_d1,
        revenue_d3,
        revenue_d7,
        revenue_d14,
        revenue_d30,
        revenue_d60,
        revenue_d90,
        revenue_d120,
        revenue_d135,
        revenue_d150,
        -- 计算每个分组内有多少天的数据
        count(*) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as days_count
    from
        (
			select * from data_science.default.lw_20250903_aos_gpir_cohort_onlyprofit_raw_view_by_j
			union all
			select * from data_science.default.lw_20250903_ios_af_cohort_onlyprofit_fit_view_by_j
        )
        
),
averaged_data as (
    select
        app_package,
        country_group,
        mediasource,
        tag,
        install_day,
        -- 只有当有足够N天数据时才计算平均值，否则为NULL
        case 
            when days_count >= {N} then 
                avg(cost) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_cost,
        case 
            when days_count >= {N} then 
                avg(revenue_d1) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d1,
        case 
            when days_count >= {N} then 
                avg(revenue_d3) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d3,
        case 
            when days_count >= {N} then 
                avg(revenue_d7) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d7,
        case 
            when days_count >= {N} then 
                avg(revenue_d14) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d14,
        case 
            when days_count >= {N} then 
                avg(revenue_d30) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d30,
        case 
            when days_count >= {N} then 
                avg(revenue_d60) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d60,
        case 
            when days_count >= {N} then 
                avg(revenue_d90) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d90,
        case 
            when days_count >= {N} then 
                avg(revenue_d120) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d120,
        case 
            when days_count >= {N} then 
                avg(revenue_d135) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d135,
        case 
            when days_count >= {N} then 
                avg(revenue_d150) over (
                    partition by app_package, country_group, mediasource, tag
                    order by install_day 
                    rows between {N-1} preceding and current row
                )
            else null 
        end as avg_revenue_d150
    from
        base_data
)
select
    app_package,
    country_group,
    mediasource,
    'aos_gpir_cohort_avg_{N}' as tag,
    install_day,
    avg_cost as cost,
    avg_revenue_d1 as revenue_d1,
    avg_revenue_d3 as revenue_d3,
    avg_revenue_d7 as revenue_d7,
    avg_revenue_d14 as revenue_d14,
    avg_revenue_d30 as revenue_d30,
    avg_revenue_d60 as revenue_d60,
    avg_revenue_d90 as revenue_d90,
    avg_revenue_d120 as revenue_d120,
    avg_revenue_d135 as revenue_d135,
    avg_revenue_d150 as revenue_d150
from
    averaged_data;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return viewName

def createEMAView(N=21):
    alpha = 2.0 / (N + 1)  # 平滑因子
    one_minus_alpha = 1 - alpha
    viewName = f"lw_20250903_cohort_onlyprofit_ema_{N}_view_by_j"
    sql = f"""
CREATE OR REPLACE VIEW {viewName} as
with base_data as (
    select
        app_package,
        country_group,
        mediasource,
        tag,
        install_day,
        cost,
        revenue_d1,
        revenue_d3,
        revenue_d7,
        revenue_d14,
        revenue_d30,
        revenue_d60,
        revenue_d90,
        revenue_d120,
        revenue_d135,
        revenue_d150,
        row_number() over (
            partition by app_package, country_group, mediasource, tag
            order by install_day
        ) as rn
    from
        (
			select * from data_science.default.lw_20250903_aos_gpir_cohort_onlyprofit_raw_view_by_j
			union all
			select * from data_science.default.lw_20250903_ios_af_cohort_onlyprofit_fit_view_by_j
        )
),
windowed_data as (
    select 
        *,
        -- 为窗口内每行计算相对位置（0到N-1）
        collect_list(cost) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as cost_window,
        collect_list(revenue_d1) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d1_window,
        collect_list(revenue_d3) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d3_window,
        collect_list(revenue_d7) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d7_window,
        collect_list(revenue_d14) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d14_window,
        collect_list(revenue_d30) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d30_window,
        collect_list(revenue_d60) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d60_window,
        collect_list(revenue_d90) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d90_window,
        collect_list(revenue_d120) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d120_window,
        collect_list(revenue_d135) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d135_window,
        collect_list(revenue_d150) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        ) as revenue_d150_window,
        size(collect_list(cost) over (
            partition by app_package, country_group, mediasource, tag
            order by install_day 
            rows between {N-1} preceding and current row
        )) as window_size
    from base_data
)
select
    app_package,
    country_group,
    mediasource,
    'aos_gpir_cohort_ema_{N}' as tag,
    install_day,
    -- 只有当窗口大小达到N时才计算EMA
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + cost_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as cost,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d1_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d1,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d3_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d3,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d7_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d7,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d14_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d14,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d30_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d30,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d60_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d60,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d90_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d90,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d120_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d120,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d135_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d135,
    
    case when window_size >= {N} then
        aggregate(
            sequence(0, {N-1}),
            cast(0.0 as double),
            (acc, i) -> acc + revenue_d150_window[i] * {alpha} * power({one_minus_alpha}, {N-1} - i)
        ) / (1 - power({one_minus_alpha}, {N}))
    else null end as revenue_d150
from
    windowed_data;
    """
    print(f"Executing SQL for EMA with N={N}, alpha={alpha:.4f}")
    execSql2(sql)
    return viewName

def createAosGpirCohortOnlyprofitAllFuncView(viewNames):
    sql = """
CREATE OR REPLACE VIEW lw_20250903_aos_gpir_cohort_onlyprofit_all_func_view_by_j as
SELECT
*
FROM lw_20250903_aos_gpir_cohort_onlyprofit_avg_28_view_by_j
UNION ALL
SELECT
*
FROM lw_20250903_aos_gpir_cohort_onlyprofit_avg_56_view_by_j
UNION ALL
SELECT
*
FROM lw_20250903_aos_gpir_cohort_onlyprofit_avg_84_view_by_j
UNION ALL
SELECT
*   
FROM lw_20250903_aos_gpir_cohort_onlyprofit_ema_28_view_by_j
UNION ALL
SELECT
*   
FROM lw_20250903_aos_gpir_cohort_onlyprofit_ema_56_view_by_j
UNION ALL
SELECT
*   
FROM lw_20250903_aos_gpir_cohort_onlyprofit_ema_84_view_by_j
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createRevenueGrowthRateView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_aos_gpir_cohort_onlyprofit_revenue_growth_rate_view_by_j as
select 
    app_package,
    country_group,
    mediasource,
    tag,
    install_day,
    try_divide(revenue_d3, revenue_d1) as r3_r1,
    try_divide(revenue_d7, revenue_d3) as r7_r3,
    try_divide(revenue_d14, revenue_d7) as r14_r7,
    try_divide(revenue_d30, revenue_d14) as r30_r14,
    try_divide(revenue_d60, revenue_d30) as r60_r30,
    try_divide(revenue_d90, revenue_d60) as r90_r60,
    try_divide(revenue_d120, revenue_d90) as r120_r90,
    try_divide(revenue_d135, revenue_d120) as r135_r120,
    try_divide(revenue_d150, revenue_d135) as r150_r135
from lw_20250903_aos_gpir_cohort_onlyprofit_all_func_view_by_j
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createPredictRevenueGrowthRateView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_onlyprofit_predict_revenue_growth_rate_view_by_j as
select
    app_package,
    country_group,
    mediasource,
    tag,
    install_day,
    -- 原始数据
    r3_r1, r7_r3, r14_r7, r30_r14, r60_r30, r90_r60, r120_r90, r135_r120, r150_r135,
    -- 预测数据
    lag(r3_r1, 3) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r3_r1,
    lag(r7_r3, 7) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r7_r3,
    lag(r14_r7, 14) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r14_r7,
    lag(r30_r14, 30) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r30_r14,
    lag(r60_r30, 60) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r60_r30,
    lag(r90_r60, 90) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r90_r60,
    lag(r120_r90, 120) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r120_r90,
    lag(r135_r120, 135) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r135_r120,
    lag(r150_r135, 150) over (partition by app_package, country_group, mediasource, tag order by to_date(install_day, 'yyyyMMdd')) as p_r150_r135
from lw_20250903_aos_gpir_cohort_onlyprofit_revenue_growth_rate_view_by_j
order by app_package, country_group, mediasource, tag, to_date(install_day, 'yyyyMMdd')
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createPredictRevenueView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_onlyprofit_predict_revenue_view_by_j as
WITH predict_data AS (
    SELECT
        app_package,
        country_group,
        mediasource,
        tag,
        install_day,
        p_r3_r1,
        p_r7_r3,
        p_r14_r7,
        p_r30_r14,
        p_r60_r30,
        p_r90_r60,
        p_r120_r90,
        p_r135_r120,
        p_r150_r135
    FROM lw_20250903_onlyprofit_predict_revenue_growth_rate_view_by_j
),
revenue_data AS (
    SELECT
        app_package,
        country_group,
        mediasource,
        install_day,
        cost,
        revenue_d1,
        revenue_d3,
        revenue_d7,
        revenue_d14,
        revenue_d30,
        revenue_d60,
        revenue_d90,
        revenue_d120,
        revenue_d135,
        revenue_d150
    FROM lw_20250903_aos_gpir_cohort_onlyprofit_raw_view_by_j
)
SELECT
    r.app_package,
    r.country_group,
    r.mediasource,
    p.tag,
    r.install_day,
    -- 原始数据
    r.cost,
    r.revenue_d1,
    r.revenue_d3,
    r.revenue_d7,
    r.revenue_d14,
    r.revenue_d30,
    r.revenue_d60,
    r.revenue_d90,
    r.revenue_d120,
    r.revenue_d135,
    r.revenue_d150,
    
    -- 基于r1的预测
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) AS r1_p_r3,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) AS r1_p_r7,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) AS r1_p_r14,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) AS r1_p_r30,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) AS r1_p_r60,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) AS r1_p_r90,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) AS r1_p_r120,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) AS r1_p_r135,
    r.revenue_d1 * COALESCE(p.p_r3_r1, 1) * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r1_p_r150,
    
    -- 基于r3的预测
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) AS r3_p_r7,
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) AS r3_p_r14,
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) AS r3_p_r30,
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) AS r3_p_r60,
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) AS r3_p_r90,
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) AS r3_p_r120,
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) AS r3_p_r135,
    r.revenue_d3 * COALESCE(p.p_r7_r3, 1) * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r3_p_r150,
    
    -- 基于r7的预测
    r.revenue_d7 * COALESCE(p.p_r14_r7, 1) AS r7_p_r14,
    r.revenue_d7 * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) AS r7_p_r30,
    r.revenue_d7 * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) AS r7_p_r60,
    r.revenue_d7 * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) AS r7_p_r90,
    r.revenue_d7 * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) AS r7_p_r120,
    r.revenue_d7 * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) AS r7_p_r135,
    r.revenue_d7 * COALESCE(p.p_r14_r7, 1) * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r7_p_r150,
    
    -- 基于r14的预测
    r.revenue_d14 * COALESCE(p.p_r30_r14, 1) AS r14_p_r30,
    r.revenue_d14 * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) AS r14_p_r60,
    r.revenue_d14 * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) AS r14_p_r90,
    r.revenue_d14 * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) AS r14_p_r120,
    r.revenue_d14 * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) AS r14_p_r135,
    r.revenue_d14 * COALESCE(p.p_r30_r14, 1) * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r14_p_r150,
    
    -- 基于r30的预测
    r.revenue_d30 * COALESCE(p.p_r60_r30, 1) AS r30_p_r60,
    r.revenue_d30 * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) AS r30_p_r90,
    r.revenue_d30 * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) AS r30_p_r120,
    r.revenue_d30 * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) AS r30_p_r135,
    r.revenue_d30 * COALESCE(p.p_r60_r30, 1) * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r30_p_r150,
    
    -- 基于r60的预测
    r.revenue_d60 * COALESCE(p.p_r90_r60, 1) AS r60_p_r90,
    r.revenue_d60 * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) AS r60_p_r120,
    r.revenue_d60 * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) AS r60_p_r135,
    r.revenue_d60 * COALESCE(p.p_r90_r60, 1) * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r60_p_r150,
    
    -- 基于r90的预测
    r.revenue_d90 * COALESCE(p.p_r120_r90, 1) AS r90_p_r120,
    r.revenue_d90 * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) AS r90_p_r135,
    r.revenue_d90 * COALESCE(p.p_r120_r90, 1) * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r90_p_r150,
    
    -- 基于r120的预测
    r.revenue_d120 * COALESCE(p.p_r135_r120, 1) AS r120_p_r135,
    r.revenue_d120 * COALESCE(p.p_r135_r120, 1) * COALESCE(p.p_r150_r135, 1) AS r120_p_r150,
    
    -- 基于r135的预测
    r.revenue_d135 * COALESCE(p.p_r150_r135, 1) AS r135_p_r150,
    
    -- 基于r150的预测（实际值）
    r.revenue_d150 AS r150_p_r150

FROM revenue_data r
LEFT JOIN predict_data p ON 
    r.app_package = p.app_package
    AND r.country_group = p.country_group
    AND r.mediasource = p.mediasource
    AND r.install_day = p.install_day
ORDER BY 
    r.app_package, 
    r.country_group, 
    r.mediasource, 
    p.tag, 
    r.install_day;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createRealAndPredictRevenueView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_onlyprofit_real_and_predict_revenue_view_by_j as
SELECT
    app_package,
    country_group,
    mediasource,
    tag,
    install_day,
    cost,
    -- 计算日期差值
    datediff(current_date(), to_date(install_day, 'yyyyMMdd')) as days_diff,
    
    -- revenue_d1: 差值>1时用真实值，否则用r1预测（实际上r1总是真实值）
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 1 
        THEN revenue_d1 
        ELSE revenue_d1 
    END AS revenue_d1,
    
    -- revenue_d3: 差值>3时用真实值，否则用r1预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN revenue_d3 
        ELSE r1_p_r3 
    END AS revenue_d3,
    
    -- revenue_d7: 差值>7时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN revenue_d7 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r7 
        ELSE r1_p_r7 
    END AS revenue_d7,
    
    -- revenue_d14: 差值>14时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 14 
        THEN revenue_d14 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN r7_p_r14 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r14 
        ELSE r1_p_r14 
    END AS revenue_d14,
    
    -- revenue_d30: 差值>30时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 30 
        THEN revenue_d30 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 14 
        THEN r14_p_r30 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN r7_p_r30 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r30 
        ELSE r1_p_r30 
    END AS revenue_d30,
    
    -- revenue_d60: 差值>60时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 60 
        THEN revenue_d60 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 30 
        THEN r30_p_r60 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 14 
        THEN r14_p_r60 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN r7_p_r60 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r60 
        ELSE r1_p_r60 
    END AS revenue_d60,
    
    -- revenue_d90: 差值>90时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 90 
        THEN revenue_d90 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 60 
        THEN r60_p_r90 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 30 
        THEN r30_p_r90 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 14 
        THEN r14_p_r90 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN r7_p_r90 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r90 
        ELSE r1_p_r90 
    END AS revenue_d90,
    
    -- revenue_d120: 差值>120时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 120 
        THEN revenue_d120 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 90 
        THEN r90_p_r120 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 60 
        THEN r60_p_r120 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 30 
        THEN r30_p_r120 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 14 
        THEN r14_p_r120 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN r7_p_r120 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r120 
        ELSE r1_p_r120 
    END AS revenue_d120,
    
    -- revenue_d135: 差值>135时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 135 
        THEN revenue_d135 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 120 
        THEN r120_p_r135 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 90 
        THEN r90_p_r135 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 60 
        THEN r60_p_r135 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 30 
        THEN r30_p_r135 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 14 
        THEN r14_p_r135 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN r7_p_r135 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r135 
        ELSE r1_p_r135 
    END AS revenue_d135,
    
    -- revenue_d150: 差值>150时用真实值，否则用最佳预测
    CASE 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 150 
        THEN revenue_d150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 135 
        THEN r135_p_r150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 120 
        THEN r120_p_r150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 90 
        THEN r90_p_r150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 60 
        THEN r60_p_r150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 30 
        THEN r30_p_r150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 14 
        THEN r14_p_r150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 7 
        THEN r7_p_r150 
        WHEN datediff(current_date(), to_date(install_day, 'yyyyMMdd')) > 3 
        THEN r3_p_r150 
        ELSE r1_p_r150 
    END AS revenue_d150

FROM lw_20250903_onlyprofit_predict_revenue_view_by_j
ORDER BY 
    app_package, 
    country_group, 
    mediasource, 
    tag, 
    to_date(install_day, 'yyyyMMdd');
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return


def createOrganicRevenueRateView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_onlyprofit_organic_revenue_rate_view_by_j as
SELECT
    app_package,
    country_group,
    tag,
    install_day,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d7 ELSE 0 END) / SUM(revenue_d7) AS organic_revenue_rate_d7,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d14 ELSE 0 END) / SUM(revenue_d14) AS organic_revenue_rate_d14,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d30 ELSE 0 END) / SUM(revenue_d30) AS organic_revenue_rate_d30,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d60 ELSE 0 END) / SUM(revenue_d60) AS organic_revenue_rate_d60,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d90 ELSE 0 END) / SUM(revenue_d90) AS organic_revenue_rate_d90,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d120 ELSE 0 END) / SUM(revenue_d120) AS organic_revenue_rate_d120,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d135 ELSE 0 END) / SUM(revenue_d135) AS organic_revenue_rate_d135,
    SUM(CASE WHEN mediasource IN ('organic', 'Organic') THEN revenue_d150 ELSE 0 END) / SUM(revenue_d150) AS organic_revenue_rate_d150
FROM data_science.default.lw_20250903_onlyprofit_real_and_predict_revenue_table_by_j
GROUP BY app_package, country_group, tag, install_day
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createRealAndPredictRevenueTable():
    sql = """
create or replace table data_science.default.lw_20250903_onlyprofit_real_and_predict_revenue_table_by_j as 
select * from data_science.default.lw_20250903_onlyprofit_real_and_predict_revenue_view_by_j
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return


def main():
    # createAosGpirCohortOnlyProfitRawView()
    # createIosAfCohortOnlyProfitRawView()
    # createIosAfCohortOnlyProfitFixTable()
    # createIosAfCohortOnlyProfitFitTable()

    createAvgNView(28)
    # createAvgNView(56)
    # createAvgNView(84)
    # createEMAView(28)
    # createEMAView(56)
    # createEMAView(84)

    createAosGpirCohortOnlyprofitAllFuncView()
    # createRevenueGrowthRateView()
    # createPredictRevenueGrowthRateView()
    # createPredictRevenueView()
    # createRealAndPredictRevenueView()
    # createOrganicRevenueRateView()

    # createRealAndPredictRevenueTable()

    

    
    

if __name__ == "__main__":
    main()
    
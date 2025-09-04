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
	app_package,
	roi.install_day,
	COALESCE(cg.country_group, 'other'),
	case
	when roi.mediasource = 'applovin_int' and pub.campaign_name like '%D7%' then 'applovin_int_d7'
	when roi.mediasource = 'applovin_int' and pub.campaign_name like '%D28%' then 'applovin_int_d28'
	when roi.mediasource = 'applovin_int' then 'applovin_int'
	else roi.mediasource end
;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return


def createAosGpirCohortOnlyProfitAvgNView(N=21):
	sql = f"""
CREATE OR REPLACE VIEW lw_20250903_aos_gpir_cohort_onlyprofit_avg_{N}_view_by_j as
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
        data_science.default.lw_20250903_aos_gpir_cohort_onlyprofit_raw_view_by_j
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
	return


def createAosGpirCohortOnlyprofitAllFuncView():
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

def main():
    # createAosGpirCohortOnlyProfitRawView()
    createAosGpirCohortOnlyProfitAvgNView(28)
    createAosGpirCohortOnlyProfitAvgNView(56)
    createAosGpirCohortOnlyProfitAvgNView(84)
    createAosGpirCohortOnlyprofitAllFuncView()
    createRevenueGrowthRateView()
    createPredictRevenueGrowthRateView()
    createPredictRevenueView()
    createRealAndPredictRevenueView()
    

if __name__ == "__main__":
    main()
    
import pandas as pd

import sys
sys.path.append('/src')
from src.dataBricks import execSql, execSql2

# 基础数据，只是将国家分组，将applovin分成d7和d28
def createAosGpirCohortRawView():
    sql = """
CREATE OR REPLACE VIEW lw_20250903_aos_gpir_cohort_raw_view_by_j as
SELECT
	'com.fun.lastwar.gp' as app_package,
	roi.install_day,
	COALESCE(cg.country_group, 'other') AS country_group,
	case
	when roi.mediasource = 'applovin_int' and roi.campaign_name like '%D7%' then 'applovin_int_d7'
	when roi.mediasource = 'applovin_int' and roi.campaign_name like '%D28%' then 'applovin_int_d28'
	when roi.mediasource = 'applovin_int' then 'applovin_int'
	else roi.mediasource end as mediasource,
	'aos_gpir_cohort_raw' as tag,
	sum(cost_value_usd) as cost,
	SUM(revenue_cohort_d1) AS revenue_d1,
	SUM(revenue_cohort_d3) AS revenue_d3,
	SUM(revenue_cohort_d7) AS revenue_d7,
	SUM(revenue_cohort_d14) AS revenue_d14,
	SUM(revenue_cohort_d30) AS revenue_d30,
	SUM(revenue_cohort_d60) AS revenue_d60,
	SUM(revenue_cohort_d90) AS revenue_d90,
	SUM(revenue_cohort_d120) AS revenue_d120,
	SUM(revenue_cohort_d135) AS revenue_d135,
	SUM(revenue_cohort_d150) AS revenue_d150
FROM
	marketing.attribution.dws_overseas_gpir_roi roi
	LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.facebook_segment IN ('country', 'N/A')
	and roi.app = '502'
GROUP BY
	app_package,
	roi.install_day,
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


def createAosGpirCohortAvgNView(N=21):
# 下面是我当时给AI的提示语，保留在此处以备后用 
# -- 帮我写一个按照 app_package,country_group,mediasource,tag进行分组
# -- 分组中按照install_day排序，然后计算每个分组中install_day与最近N天的各组数据的平均值，N=21,不满N天的为空
# -- 包括cost,revenue_d1,revenue_d3,revenue_d7,revenue_d14,revenue_d30,revenue_d60,revenue_d90,revenue_d120,revenue_d135,revenue_d150
# -- 最终输出列 app_package,country_group,mediasource,tag,install_day,cost,revenue_d1,revenue_d3,revenue_d7,revenue_d14,revenue_d30,revenue_d60,revenue_d90,revenue_d120,revenue_d135,revenue_d150

	sql = f"""
CREATE OR REPLACE VIEW lw_20250903_aos_gpir_cohort_avg_{N}_view_by_j as
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
        data_science.default.lw_20250903_aos_gpir_cohort_raw_view_by_j
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

def main():
    # createAosGpirCohortRawView()
    createAosGpirCohortAvgNView(21)
    

if __name__ == "__main__":
    main()
    
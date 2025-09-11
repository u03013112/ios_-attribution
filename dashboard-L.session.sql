select
-- install_day,
sum(case WHEN mediasource = 'Organic' THEN revenue_d120 ELSE 0 END) as organic_revenue_d120,
sum(revenue_d120) as total_revenue_d120,
try_divide(
    sum(case WHEN mediasource = 'Organic' THEN revenue_d120 ELSE 0 END),
    sum(revenue_d120)
) as organic_revenue_d120_rate
-- from data_science.default.lw_20250903_real_and_predict_revenue_table_by_j
from data_science.default.lw_20250903_aos_gpir_cohort_raw_table_by_j
where app_package = 'com.fun.lastwar.gp'
and country_group = 'other'
and install_day between '20250801' and '20250830'
-- group by install_day
-- order by install_day asc
;




SELECT
	'com.fun.lastwar.gp' as app_package,
	COALESCE(cg.country_group, 'other') AS country_group,
	case
		when roi.mediasource = 'applovin_int'
		and roi.campaign_name like '%D7%' then 'applovin_int_d7'
		when roi.mediasource = 'applovin_int'
		and roi.campaign_name like '%D28%' then 'applovin_int_d28'
		when roi.mediasource in ('Organic', 'organic') THEN 'Organic'
		else roi.mediasource
	end as mediasource,
	'aos_gpir_cohort_raw' as tag,
	roi.install_day,
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
	LEFT JOIN data_science.default.lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
WHERE
	roi.facebook_segment IN ('country', 'N/A')
	and roi.app = '502'
	and roi.app_package in ('com.fun.lastwar.gp', 'com.fun.lastwar.vn.gp')
GROUP BY
	1,
	2,
	3,
	4,
	5;
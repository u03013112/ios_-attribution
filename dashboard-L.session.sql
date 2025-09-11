select
install_day,
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
group by install_day
order by install_day asc
;
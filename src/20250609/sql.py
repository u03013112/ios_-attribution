# 用于测试获得mc数据

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO


sql = """
SELECT
	*
FROM
    lw_20250703_applovin_ratio_view_by_j
where
    install_month >= '202501'
    -- and ad_type = 'ALL'
    -- and tag in ('for_ua','gpir_cohort')
    and country_group in ('US')
order by
	app_package,
	install_month,
	country_group
limit
	1000;
"""

print(f"Executing SQL: {sql}")
df = execSql(sql)

print(df)

df.to_csv('/src/data/sql_data_2.csv', index=False)
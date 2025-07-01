# https://qianxian.feishu.cn/docx/Uqk7do0lGodS0yxXGoKc6OMrnbh
# 基于上述文档进行开发
import os
import datetime
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

def createRealDailyData():
    sql = """
CREATE VIEW IF NOT EXISTS lw_real_cost_roi_country_group_ad_type_view_by_j AS
select
	install_day,
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
	CASE
		WHEN mediasource = 'Facebook Ads' THEN CASE
			WHEN campaign_name LIKE '%BAU%' THEN 'BAU'
			WHEN campaign_name LIKE '%AAA%'
			OR campaign_name LIKE '%3A%' THEN 'AAA'
			ELSE 'other'
		END
		WHEN mediasource = 'googleadwords_int' THEN CASE
			WHEN campaign_name LIKE '%3.0%' THEN '3.0'
			WHEN campaign_name LIKE '%2.5%' THEN '2.5'
			ELSE 'other'
		END
		ELSE 'other'
	END AS ad_type,
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
	and facebook_segment in ('country', ' ')
group by
	install_day,
	country_group,
	mediasource,
	ad_type;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def createRealMonthyData():
    sql = """
CREATE VIEW IF NOT EXISTS lw_real_cost_roi_country_group_ad_type_month_view_by_j AS
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
	CASE
		WHEN mediasource = 'Facebook Ads' THEN CASE
			WHEN campaign_name LIKE '%BAU%' THEN 'BAU'
			WHEN campaign_name LIKE '%AAA%'
			OR campaign_name LIKE '%3A%' THEN 'AAA'
			ELSE 'other'
		END
		WHEN mediasource = 'googleadwords_int' THEN CASE
			WHEN campaign_name LIKE '%3.0%' THEN '3.0'
			WHEN campaign_name LIKE '%2.5%' THEN '2.5'
			ELSE 'other'
		END
		ELSE 'other'
	END AS ad_type,
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
	and facebook_segment in ('country', ' ')
group by
	install_month,
	country_group,
	mediasource,
	ad_type;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

def main():
    # # 创建每日数据视图
    # createRealDailyData()

    # 创建每月数据视图
    createRealMonthyData()



if __name__ == "__main__":
    main()
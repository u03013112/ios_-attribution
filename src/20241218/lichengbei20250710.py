# 佳玥新需求
# 大盘部分仍旧是分国家里程碑，与之前保持一致，暂时不变
# 媒体部分按照新订的媒体KPI进行
# 注意，其中国家分组不一致
import os
import datetime

import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO


# 需要表格主要列
# 维度：平台、安装日期、国家、媒体
# 数值：当日花费、累计花费、有效累计花费、累计ROI7、目标ROI7

# KPI表格，在不改动kpi的时候不需要重新创建
def createMediaKpiTable():
    sql = """
DROP TABLE IF EXISTS lw_media_kpi_table_by_j_20250703;
CREATE TABLE lw_media_kpi_table_by_j_20250703 (
    app_package VARCHAR(255),
    mediasource VARCHAR(255),
    country VARCHAR(255),
    kpi1 DECIMAL(10,4),
    kpi3 DECIMAL(10,4),
    kpi7 DECIMAL(10,4)
);

INSERT INTO lw_media_kpi_table_by_j_20250703 (app_package, mediasource, country, kpi1, kpi3, kpi7) VALUES
('com.fun.lastwar.gp', 'googleadwords_int', 'US', 0.0093, 0.0287, 0.0632),
('com.fun.lastwar.gp', 'applovin_int', 'US', 0.0080, 0.0230, 0.0550),
('com.fun.lastwar.gp', 'Facebook Ads', 'US', 0.0080, 0.0240, 0.0550),
('com.fun.lastwar.gp', 'bytedanceglobal_int', 'US', 0.0140, 0.0370, 0.0670),
('com.fun.lastwar.gp', 'snapchat_int', 'US', 0.0100, 0.0270, 0.0550),
('com.fun.lastwar.gp', 'moloco_int', 'US', 0.0100, 0.0500, 0.1000),

('com.fun.lastwar.gp', 'googleadwords_int', 'KR', 0.0090, 0.0260, 0.0650),
('com.fun.lastwar.gp', 'applovin_int', 'KR', 0.0100, 0.0300, 0.0740),
('com.fun.lastwar.gp', 'Facebook Ads', 'KR', 0.0120, 0.0320, 0.0770),
('com.fun.lastwar.gp', 'bytedanceglobal_int', 'KR', 0.0130, 0.0420, 0.0850),
('com.fun.lastwar.gp', 'moloco_int', 'KR', 0.0140, 0.0400, 0.0900),

('com.fun.lastwar.gp', 'googleadwords_int', 'JP', 0.0110, 0.0340, 0.0830),
('com.fun.lastwar.gp', 'applovin_int', 'JP', 0.0100, 0.0300, 0.0650),
('com.fun.lastwar.gp', 'Facebook Ads', 'JP', 0.0100, 0.0300, 0.0780),
('com.fun.lastwar.gp', 'bytedanceglobal_int', 'JP', 0.0130, 0.0500, 0.0900),

('com.fun.lastwar.gp', 'googleadwords_int', 'GCC', 0.0060, 0.0200, 0.0450),
('com.fun.lastwar.gp', 'applovin_int', 'GCC', 0.0050, 0.0180, 0.0450),
('com.fun.lastwar.gp', 'Facebook Ads', 'GCC', 0.0030, 0.0120, 0.0300),
('com.fun.lastwar.gp', 'bytedanceglobal_int', 'GCC', 0.0050, 0.0200, 0.0400),
('com.fun.lastwar.gp', 'snapchat_int', 'GCC', 0.0030, 0.0150, 0.0400),

('com.fun.lastwar.gp', 'googleadwords_int', 'T1', 0.0100, 0.0350, 0.0770),
('com.fun.lastwar.gp', 'applovin_int', 'T1', 0.0100, 0.0360, 0.0820),
('com.fun.lastwar.gp', 'Facebook Ads', 'T1', 0.0110, 0.0330, 0.0700),
('com.fun.lastwar.gp', 'bytedanceglobal_int', 'T1', 0.0140, 0.0440, 0.0970),
('com.fun.lastwar.gp', 'moloco_int', 'T1', 0.0137, 0.0407, 0.0815),

('com.fun.lastwar.gp', 'googleadwords_int', 'other', 0.0120, 0.0350, 0.0760),
('com.fun.lastwar.gp', 'applovin_int', 'other', 0.0100, 0.0280, 0.0670),
('com.fun.lastwar.gp', 'Facebook Ads', 'other', 0.0100, 0.0300, 0.0700),
('com.fun.lastwar.gp', 'bytedanceglobal_int', 'other', 0.0170, 0.0500, 0.1000),
('com.fun.lastwar.gp', 'moloco_int', 'other', 0.0140, 0.0420, 0.0840);

    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 创建里程碑起始日期表，由于odps不支持between join，所以用python写
# 每天都需要重新创建
def createMilestoneStartdayTable():
    sql1 = """
    SELECT DISTINCT install_day FROM ads_lastwar_mediasource_reattribution WHERE install_day >= '20240101';
    """
    installDayDf = execSql(sql1)

    sql2 = """
    SELECT DISTINCT startday, endday FROM ads_application_lastwar_milestones;
    """
    milestoneDf = execSql(sql2)

    # 确保日期格式为datetime，方便比较
    installDayDf['install_day'] = pd.to_datetime(installDayDf['install_day'], format='%Y%m%d')
    milestoneDf['startday'] = pd.to_datetime(milestoneDf['startday'], format='%Y%m%d')
    milestoneDf['endday'] = pd.to_datetime(milestoneDf['endday'], format='%Y%m%d')
    # 合并数据：将install_day与每个milestone日期范围做匹配
    merged_df = pd.merge(
        installDayDf.assign(key=1),  # 增加辅助列key，便于笛卡尔积
        milestoneDf.assign(key=1), 
        on='key'
    ).drop('key', axis=1)
    # 筛选出符合条件的行
    result_df = merged_df[
        (merged_df['install_day'] >= merged_df['startday']) &
        (merged_df['install_day'] <= merged_df['endday'])
    ][['install_day', 'startday']].drop_duplicates()
    # 如果需要，将日期转回字符串格式
    result_df['install_day'] = result_df['install_day'].dt.strftime('%Y%m%d')
    result_df['startday'] = result_df['startday'].dt.strftime('%Y%m%d')
    
    # 写数据库
    table_name = 'lw_milestone_startday_table_by_j_20250703'
    o = getO()
    o.delete_table(table_name, if_exists=True)
    columns = 'install_day string, startday string'
    new_table = o.create_table(table_name, columns)

    # 写入数据
    with new_table.open_writer() as writer:
        for _, row in result_df.iterrows():
            writer.write([row['install_day'], row['startday']])


    return 

# 按照里程碑计算累计花费与收入
def createMilestoneMediaCostRevenueView():
    sql = """
CREATE OR REPLACE VIEW lw_milestone_media_cost_revenue_view_by_j_20250703 AS
WITH base_data AS (
	SELECT
		roi.app_package,
		roi.install_day,
		COALESCE(cg.country_group, 'other') AS country_group,
		CASE
			WHEN roi.mediasource IN ('restricted', 'Facebook Ads') THEN 'Facebook Ads'
			ELSE roi.mediasource
		END AS mediasource,
		milestone.startday,
		SUM(roi.cost_value_usd) AS cost,
		SUM(roi.revenue_d1) AS revenue_d1,
		SUM(roi.revenue_d3) AS revenue_d3,
		SUM(roi.revenue_d7) AS revenue_d7
	FROM
		ads_lastwar_mediasource_reattribution roi
		LEFT JOIN lw_country_group_table_by_j_20250703 cg ON roi.country = cg.country
		LEFT JOIN lw_milestone_startday_table_by_j_20250703 milestone ON roi.install_day = milestone.install_day
	WHERE
		roi.facebook_segment IN ('country', 'N/A')
		AND roi.install_day >= '20240401'
	GROUP BY
		roi.app_package,
		roi.install_day,
		COALESCE(cg.country_group, 'other'),
		CASE
			WHEN roi.mediasource IN ('restricted', 'Facebook Ads') THEN 'Facebook Ads'
			ELSE roi.mediasource
		END,
		milestone.startday
	HAVING
		SUM(roi.cost_value_usd) > 0
) -- 在上面的基础数据上，计算累计和
SELECT
	app_package,
	install_day,
	country_group,
	mediasource,
	startday,
	cost,
	revenue_d1,
	revenue_d3,
	revenue_d7,
	SUM(cost) OVER (
		PARTITION BY app_package,
		country_group,
		mediasource,
		startday
		ORDER BY
			install_day ASC ROWS BETWEEN UNBOUNDED PRECEDING
			AND CURRENT ROW
	) AS cumulative_cost,
	SUM(revenue_d1) OVER (
		PARTITION BY app_package,
		country_group,
		mediasource,
		startday
		ORDER BY
			install_day ASC ROWS BETWEEN UNBOUNDED PRECEDING
			AND CURRENT ROW
	) AS cumulative_revenue_d1,
	SUM(revenue_d3) OVER (
		PARTITION BY app_package,
		country_group,
		mediasource,
		startday
		ORDER BY
			install_day ASC ROWS BETWEEN UNBOUNDED PRECEDING
			AND CURRENT ROW
	) AS cumulative_revenue_d3,
	SUM(revenue_d7) OVER (
		PARTITION BY app_package,
		country_group,
		mediasource,
		startday
		ORDER BY
			install_day ASC ROWS BETWEEN UNBOUNDED PRECEDING
			AND CURRENT ROW
	) AS cumulative_revenue_d7
FROM
	base_data
ORDER BY
	app_package,
	country_group,
	mediasource,
	startday,
	install_day;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return

# 与KPI表格关联，并创建有效花费收入1、3、7列，其中如果累计ROI1、3、7大于KPI1、3、7则有效花费=累计花费，否则有效话费=0
def createMilestoneMediaTable():
    sql = """
DROP TABLE IF EXISTS lw_milestone_media_table_by_j_20250703;
CREATE TABLE lw_milestone_media_table_by_j_20250703 AS
SELECT
    a.app_package,
    a.install_day,
    a.country_group,
    a.mediasource,
    a.startday,
    a.cost,
    a.revenue_d1,
    a.revenue_d3,
    a.revenue_d7,
    a.cumulative_cost,
    a.cumulative_revenue_d1,
    a.cumulative_revenue_d3,
    a.cumulative_revenue_d7,
    -- 计算累计ROI
    CASE WHEN a.cumulative_cost > 0 THEN a.cumulative_revenue_d1 / a.cumulative_cost ELSE 0 END AS cumulative_roi_d1,
    CASE WHEN a.cumulative_cost > 0 THEN a.cumulative_revenue_d3 / a.cumulative_cost ELSE 0 END AS cumulative_roi_d3,
    CASE WHEN a.cumulative_cost > 0 THEN a.cumulative_revenue_d7 / a.cumulative_cost ELSE 0 END AS cumulative_roi_d7,
    -- 计算有效花费
    CASE 
        WHEN a.cumulative_cost > 0 AND (a.cumulative_revenue_d1 / a.cumulative_cost) >= COALESCE(b.kpi1, 0) 
        THEN a.cumulative_cost ELSE 0 
    END AS valid_cost_d1,
    CASE 
        WHEN a.cumulative_cost > 0 AND (a.cumulative_revenue_d3 / a.cumulative_cost) >= COALESCE(b.kpi3, 0) 
        THEN a.cumulative_cost ELSE 0 
    END AS valid_cost_d3,
    CASE 
        WHEN a.cumulative_cost > 0 AND (a.cumulative_revenue_d7 / a.cumulative_cost) >= COALESCE(b.kpi7, 0) 
        THEN a.cumulative_cost ELSE 0 
    END AS valid_cost_d7,
    COALESCE(b.kpi1, 0) AS kpi1,
    COALESCE(b.kpi3, 0) AS kpi3,
    COALESCE(b.kpi7, 0) AS kpi7
FROM
    lw_milestone_media_cost_revenue_view_by_j_20250703 a
LEFT JOIN
    lw_media_kpi_table_by_j_20250703 b
ON
    a.app_package = b.app_package
    AND LOWER(a.mediasource) = LOWER(b.mediasource)
    AND a.country_group = b.country
ORDER BY
    a.app_package,
    a.country_group,
    a.mediasource,
    a.startday,
    a.install_day;
    """
    print(f"Executing SQL: {sql}")
    execSql2(sql)
    return
    


createMediaKpiTable()

# createMilestoneStartdayTable()
# createMilestoneMediaCostRevenueView()

createMilestoneMediaTable()
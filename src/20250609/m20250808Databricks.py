import os
import datetime
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.dataBricks import execSql, execSql2


def createTable():
	sql = """
CREATE TABLE IF NOT EXISTS lw_20250703_ios_bayesian_result_by_j (
	country_group STRING COMMENT '国家组',
	organic_revenue DOUBLE COMMENT '自然量收入',
	applovin_int_d7_coeff DOUBLE COMMENT 'applovin_int_d7系数',
	applovin_int_d28_coeff DOUBLE COMMENT 'applovin_int_d28系数',
	facebook_ads_coeff DOUBLE COMMENT 'Facebook Ads系数',
	moloco_int_coeff DOUBLE COMMENT 'moloco_int系数',
	bytedanceglobal_int_coeff DOUBLE COMMENT 'bytedanceglobal_int系数',
	tag STRING COMMENT '标签分区，格式：20250808_20'
)USING delta
PARTITIONED BY (tag)
;
	"""
	execSql2(sql)
	print("表创建成功")


def m20250808():
	sql1 = """
-- 删除已存在的分区数据（如果存在）
DELETE FROM marketing.attribution.lw_20250703_ios_bayesian_result_by_j WHERE tag = '20250808_20';	
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)

	sql2 = """
-- 插入数据到新分区
INSERT INTO marketing.attribution.lw_20250703_ios_bayesian_result_by_j PARTITION (tag='20250808_20')
VALUES
	('GCC', 823.5, 1.0, 1.0, 1.122, 1.106, 1.25),
	('JP', 1405.0, 1.0, 1.0, 1.146, 1.202, 0.939),
	('KR', 848.4, 1.0, 1.0, 1.004, 1.088, 1.059),
	('T1', 4094.0, 1.0, 1.0, 1.401, 0.978, 1.25),
	('US', 4504.0, 1.0, 1.0, 1.263, 1.105, 1.25),
	('other', 2725.0, 1.0, 1.0, 1.25, 1.25, 1.25)
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)
	return

# 数据填充，目前代码中涉及到的tag有'20250806_10', '20250806_20'
# 先使用全1数据进行填充，之后更新算法后再进行更新
def m20250806():
	sql1 = """
-- 删除已存在的分区数据（如果存在）
DELETE FROM marketing.attribution.lw_20250703_ios_bayesian_result_by_j WHERE tag in ('20250806_10','20250806_20');	
	"""
	print(f"Executing SQL: {sql1}")
	execSql2(sql1)

	sql2 = """
-- 插入数据到新分区
INSERT INTO marketing.attribution.lw_20250703_ios_bayesian_result_by_j PARTITION (tag='20250806_10')
VALUES
	('GCC', 823.5, 1.0, 1.0, 1.122, 1.106, 1.25),
	('JP', 1405.0, 1.0, 1.0, 1.146, 1.202, 0.939),
	('KR', 848.4, 1.0, 1.0, 1.004, 1.088, 1.059),
	('T1', 4094.0, 1.0, 1.0, 1.401, 0.978, 1.25),
	('US', 4504.0, 1.0, 1.0, 1.263, 1.105, 1.25),
	('other', 2725.0, 1.0, 1.0, 1.25, 1.25, 1.25)
;
	"""
	print(f"Executing SQL: {sql2}")
	execSql2(sql2)

	sql3 = """
-- 插入数据到新分区
INSERT INTO marketing.attribution.lw_20250703_ios_bayesian_result_by_j PARTITION (tag='20250806_20')
VALUES
	('GCC', 823.5, 1.0, 1.0, 1.122, 1.106, 1.25),
	('JP', 1405.0, 1.0, 1.0, 1.146, 1.202, 0.939),
	('KR', 848.4, 1.0, 1.0, 1.004, 1.088, 1.059),
	('T1', 4094.0, 1.0, 1.0, 1.401, 0.978, 1.25),
	('US', 4504.0, 1.0, 1.0, 1.263, 1.105, 1.25),
	('other', 2725.0, 1.0, 1.0, 1.25, 1.25, 1.25)
;
	"""
	print(f"Executing SQL: {sql3}")
	execSql2(sql3)
	return



if __name__ == '__main__':
	# createTable()
	m20250806()
	# m20250808()
		
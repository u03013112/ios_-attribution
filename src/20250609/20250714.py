# 关于佳玥提出的里程碑大盘 与 KPI 分媒体主观感受差异问题调查

# 基础调查思路，用US为例进行调查
# 计算剩余空间金额，分别计算大盘US剩余空间金额 与 分媒体剩余空间金额的和 的关系
# 确认是否真的有明显的剩余空间差异，如果有，再进一步调查为什么

import os
import datetime
from re import S
import numpy as np
import pandas as pd

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO



# 获得US大盘数据
def getTotalUsData():
	filename = '/src/data/lastwar_lcb_pic2_20250714.csv'
	if os.path.exists(filename):
		print(f"Reading data from {filename}")
		df = pd.read_csv(filename)
		print(f"Data read successfully, shape: {df.shape}")
		return df
	else:
		sql = '''
select 
	*
from lastwar_lcb_pic2
where
	platform = 'AOS'
	and country = 'US'
	and startday = '20250605'
	and day >= '20250605'
;
	'''
		print(f"Executing SQL: {sql}")
		df = execSql(sql)
		df.to_csv(filename, index=False)

	return df

def getMilestoneData():
	filename = '/src/data/lastwar_lcb_pic1_20250714.csv'
	if os.path.exists(filename):
		print(f"Reading data from {filename}")
		df = pd.read_csv(filename)
		print(f"Data read successfully, shape: {df.shape}")
		return df
	else:
		sql = '''
select
	*
from lw_milestone_media_table_by_j_20250703
where
	app_package = 'com.fun.lastwar.gp'
	and country_group = 'US'
	and startday = '20250605'
;
		'''
		print(f"Executing SQL: {sql}")
		df = execSql(sql)
		df.to_csv(filename, index=False)

	return df



def main():
	totalUsDf = getTotalUsData()

	# 整理数据
	totalUsDf = totalUsDf[['install_day','cost','sum_cost','sum_d7roi','target_d7roi']]
	totalUsDf = totalUsDf.sort_values(by=['install_day'])
	totalUsDf['cumulative_cost'] = totalUsDf['cost'].cumsum()
	totalUsDf['cumulative_d7roi'] = totalUsDf['sum_d7roi']
	totalUsDf['space'] = (totalUsDf['sum_d7roi'] - totalUsDf['target_d7roi'])/ totalUsDf['target_d7roi'] * totalUsDf['cumulative_cost']
	totalUsDf = totalUsDf[['install_day', 'cost', 'cumulative_cost', 'cumulative_d7roi', 'target_d7roi', 'space']]
	# space 必须大于0，否则等于0
	totalUsDf['space'] = totalUsDf['space'].clip(lower=0)
	totalUsDf.to_csv('/src/data/20250714_totalUs.csv', index=False)

	milestoneDf = getMilestoneData()

	# 媒体过滤
	milestoneDf = milestoneDf[milestoneDf['mediasource'].isin([
		'Facebook Ads', 'applovin_int', 'bytedanceglobal_int', 'googleadwords_int'
		, 'moloco_int','snapchat_int'
		])]

	milestoneDf = milestoneDf[['install_day', 'mediasource', 'cost', 'cumulative_cost', 'cumulative_roi_d7', 'kpi7']]
	milestoneDf.rename(columns={
		'cumulative_roi_d7': 'cumulative_d7roi',
		'kpi7': 'target_d7roi'
	}, inplace=True)
	milestoneDf = milestoneDf.sort_values(by=['install_day','mediasource'])

	# 强制类型转换
	milestoneDf['cumulative_d7roi'] = milestoneDf['cumulative_d7roi'].astype(float)
	milestoneDf['target_d7roi'] = milestoneDf['target_d7roi'].astype(float)
	milestoneDf['cumulative_cost'] = milestoneDf['cumulative_cost'].astype(float)

	milestoneDf['space'] = (milestoneDf['cumulative_d7roi'] - milestoneDf['target_d7roi']) / milestoneDf['target_d7roi'] * milestoneDf['cumulative_cost']
	milestoneDf = milestoneDf[['install_day', 'mediasource', 'cost', 'cumulative_cost', 'cumulative_d7roi', 'target_d7roi', 'space']]
	# 将milestoneDf中含有空值的行去掉
	milestoneDf = milestoneDf.dropna()
	# 将target_d7roi小于等于0的行去掉
	milestoneDf = milestoneDf[milestoneDf['target_d7roi'] > 0]

	# space 必须大于0，否则等于0
	milestoneDf['space'] = milestoneDf['space'].clip(lower=0)
	milestoneDf.to_csv('/src/data/20250714_milestone.csv', index=False)

	
	df = milestoneDf.groupby('install_day').agg({
		'space': 'sum',
	}).reset_index()

	df = totalUsDf[['install_day','space']].merge(df, on='install_day', how='left', suffixes=('_total', '_mediaSum'))
	df['space_diff_ratio'] = (df['space_total'] - df['space_mediaSum']) / df['space_total']
	df.to_csv('/src/data/20250714_space.csv', index=False)


	# 将milestoneDf拆分，保留 install_day,mediasource,space
	# 然后将mediasource拆分成列
	milestoneDf = milestoneDf[['install_day', 'mediasource', 'space']]
	milestoneDf = milestoneDf.pivot(index='install_day', columns='mediasource', values='space').reset_index()
	milestoneDf.columns.name = None  # 重置列名
	milestoneDf = milestoneDf.fillna(0)  # 将NaN替换为0
	milestoneDf.to_csv('/src/data/20250714_milestone_split.csv', index=False)

	df2 = totalUsDf[['install_day','space']].merge(milestoneDf, on='install_day', how='left')
	df2.rename(columns={
		'space': 'total_space',
		'Facebook Ads': 'facebook_space',
		'applovin_int': 'applovin_space',
		'bytedanceglobal_int': 'bytedance_space',
		'googleadwords_int': 'googleadwords_space',
		'moloco_int': 'moloco_space',
		'snapchat_int': 'snapchat_space'	
	}, inplace=True)
	df2.to_csv('/src/data/20250714_totalUs_split.csv', index=False)


if __name__ == "__main__":
	main()
import os
import datetime
import numpy as np
from odps import DataFrame
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import sys

sys.path.append('/src')
from src.maxCompute import execSql,execSql2,getO

def getBiData(startDayStr, endDayStr):
    filename = f'/src/data/iOS20250729_Bi_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        print(f"File {filename} already exists, skipping download.")
        return pd.read_csv(filename)
    else:
        sql = f"""
select
	'id6448786147' as app_package,
	install_day,
	mediasource,
	country_group,
	sum(cost) as cost,
	sum(installs) as installs,
	sum(revenue_h24) as revenue_h24,
	sum(revenue_h72) as revenue_h72,
	sum(revenue_h168) as revenue_h168
from
	(
		select
			install_day,
			CASE 
				WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D7%' THEN 'applovin_int_d7'
				WHEN mediasource = 'applovin_int' AND UPPER(campaign_name) LIKE '%D28%' THEN 'applovin_int_d28'
				ELSE mediasource
			END as mediasource,
			COALESCE(cc.country_group, 'OTHER') AS country_group,
			cost_value_usd as cost,
			installs,
			revenue_h24 as revenue_h24,
			revenue_h72 as revenue_h72,
			revenue_h168 as revenue_h168
		from
			dws_overseas_public_roi t1
			left join lw_country_group_table_by_j_20250703 cc on t1.country = cc.country
		where
			app = '502'
			and app_package in ('id6448786147', 'id6736925794')
			and facebook_segment in ('country', 'N/A')
			and install_day between '{startDayStr}'
			and '{endDayStr}'
	)
group by
	install_day,
	mediasource,
	country_group;
    """
        df = execSql(sql)
        df.to_csv(filename, index=False)
        return df
    
def getTotalData(df):
    totalDf = df.groupby(['install_day','country_group']).sum().reset_index()
    totalDf =  totalDf.rename(columns={
        'cost':'total_cost',
        'installs':'total_installs',
        'revenue_h24':'total_revenue_h24',
        'revenue_h72':'total_revenue_h72',
        'revenue_h168':'total_revenue_h168'
        })
    return totalDf


def getMediaList(df):
    df = df.groupby(['mediasource']).sum().reset_index()
    df = df[df['cost'] > 0]
    return df['mediasource'].unique()

# 从BiData中获取AF数据
# 拆分媒体，获得媒体的模糊归因数据
def getAfData(df,mediaList):
    afDf = pd.DataFrame()
    for media in mediaList:
        df0 = df[df['mediasource'] == media]
        df0 = df0.groupby(['install_day','country_group']).sum().reset_index()
        df0 = df0.rename(columns={
            'cost': f'af_{media}_cost',
            'installs': f'af_{media}_installs',
            'revenue_h24': f'af_{media}_revenue_h24',
            'revenue_h72': f'af_{media}_revenue_h72',
            'revenue_h168': f'af_{media}_revenue_h168'
        })
        if afDf.empty:
            afDf = df0
        else:
            afDf = pd.merge(afDf, df0, on=['install_day', 'country_group'], how='outer')
    afDf = afDf.fillna(0)
    return afDf

def getData(startDayStr, endDayStr):
    df = getBiData(startDayStr, endDayStr)
    
    mediaList = getMediaList(df)

    totalDf = getTotalData(df)
    afDf = getAfData(df,mediaList)

    mergedDf = pd.merge(totalDf, afDf, on=['install_day', 'country_group'], how='outer')
    mergedDf['install_day'] = pd.to_datetime(mergedDf['install_day'].astype(str), format='%Y%m%d')

    return mergedDf

def main():
    startDayStr = '20240729'
    endDayStr = '20250729'
    df = getData(startDayStr, endDayStr)
    
    # 暂时只拟合h168数据
    # 暂时只拟合部分媒体
    df = df[[
        'install_day',
        'country_group',
        'total_revenue_h168',
        'af_Apple Search Ads_revenue_h168',
        'af_Facebook Ads_revenue_h168',
        'af_Twitter_revenue_h168',
        'af_applovin_int_revenue_h168',
        'af_applovin_int_d28_revenue_h168',
        'af_applovin_int_d7_revenue_h168',
        'af_bytedanceglobal_int_revenue_h168',
        'af_googleads_int_revenue_h168',
        'af_googleadwords_int_revenue_h168',
        'af_liftoff_int_revenue_h168',
        'af_mintegral_int_revenue_h168',
        'af_moloco_int_revenue_h168',
        'af_smartnewsads_int_revenue_h168',
        'af_snapchat_int_revenue_h168',
        'af_unityads_int_revenue_h168'
    ]]

    # 进行适度过滤，不要去太长时间，过滤一下，install_day > '20250101'
    df = df[df['install_day'] > '20250101']

    countryList = df['country_group'].unique()
    # for quick test，测试完成后，注释下面一行
    countryList = ['US']

    for country in countryList:
        print(f"country: {country}")
        countryDf = df[df['country_group'] == country]

        # 计算目前的自然量占比
        organicRevenue = countryDf['total_revenue_h168'].sum() - \
            countryDf['af_Facebook Ads_revenue_h168'].sum() - \
            countryDf['af_applovin_int_revenue_h168'].sum() - \
            countryDf['af_applovin_int_d28_revenue_h168'].sum() - \
            countryDf['af_applovin_int_d7_revenue_h168'].sum() - \
            countryDf['af_bytedanceglobal_int_revenue_h168'].sum() - \
            countryDf['af_moloco_int_revenue_h168'].sum()
        organicPercent = organicRevenue / countryDf['total_revenue_h168'].sum()
        print(f"organicPercent: {organicPercent}")

        print('Facebook Ads percent:',countryDf['af_Facebook Ads_revenue_h168'].sum()/countryDf['total_revenue_h168'].sum())
        print('applovin_int percent:',countryDf['af_applovin_int_revenue_h168'].sum()/countryDf['total_revenue_h168'].sum())
        print('applovin_int_d28 percent:',countryDf['af_applovin_int_d28_revenue_h168'].sum()/countryDf['total_revenue_h168'].sum())
        print('applovin_int_d7 percent:',countryDf['af_applovin_int_d7_revenue_h168'].sum()/countryDf['total_revenue_h168'].sum())
        print('bytedanceglobal_int percent:',countryDf['af_bytedanceglobal_int_revenue_h168'].sum()/countryDf['total_revenue_h168'].sum())
        print('moloco_int percent:',countryDf['af_moloco_int_revenue_h168'].sum()/countryDf['total_revenue_h168'].sum())


if __name__ == '__main__':
    main()
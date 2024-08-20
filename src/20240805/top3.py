import os
import numpy as np
import pandas as pd
from datetime import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

lwAppId = '64075e77537c41636a8e1c58'

def getDataFromSt(startMonth = '202401', endMonth = '202406'):
    filename = f'/src/data/st20240806_{startMonth}_{endMonth}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:

        sql = f'''
select
	country,
	app_id,
	sum(units_absolute) as units,
	sum(revenue_absolute) / 100 as revenue
from
	(
		select
			month,
			country,
			get_json_object(json, "$.app_id") as app_id,
			get_json_object(json, "$.units_absolute") as units_absolute,
			cast(
				get_json_object(json, "$.revenue_absolute") as double
			) as revenue_absolute
		from
			rg_bi.ods_platform_sensortower_monthtoopapps
		where
			month between '{startMonth}' and '{endMonth}'
	)
group by
	country,
	app_id;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename,index=False)

    return df

def getCountryGroupList():
    countryGroupList = [
        {'name':'T1', 'countries':['AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','GB','MO','IL']},
        {'name':'T2', 'countries':['BG','BV','BY','EE','ES','GL','GR','HU','ID','KZ','LT','LV','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA']},
        {'name':'T3', 'countries':['AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','HU','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','RS','SM','SR','UA','UY','XK']},
        {'name':'GCC', 'countries':['SA','AE','QA','KW','BH','OM']},
        {'name':'US', 'countries':['US']},
        {'name':'JP', 'countries':['JP']},
        {'name':'KR', 'countries':['KR']},
        {'name':'TW', 'countries':['TW']}
    ]
    return countryGroupList

# 获取每个国家收入的前三个游戏
def getRevenueTop3(df):
    df['countryGroup'] = 'Others'
    countryGroupList = getCountryGroupList()
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            df.loc[df['country'] == country, 'countryGroup'] = countryGroup['name']

    # for debug
    # print(df[df['countryGroup'] == 'Others'])

    df = df.groupby(['countryGroup','app_id']).agg(
        {
            # 'units':'sum',
            'revenue':'sum'
        }
    ).reset_index()

    # 不包含lastwar
    df = df[df['app_id'] != lwAppId]
        
    # print('debug countryGroup:',df['countryGroup'].unique())

    top3 = df.groupby('countryGroup').apply(
        lambda x: x.nlargest(3, 'revenue')
    ).reset_index(drop=True)
    
    return top3

def getlastwarRevenue(df):
    df['countryGroup'] = 'Others'
    countryGroupList = getCountryGroupList()
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            df.loc[df['country'] == country, 'countryGroup'] = countryGroup['name']

    # for debug
    # print(df[df['countryGroup'] == 'Others'])

    df = df.groupby(['countryGroup','app_id']).agg(
        {
            'units':'sum',
            'revenue':'sum'
        }
    ).reset_index()

    return df[df['app_id'] == lwAppId]

def main():
    # 设置浮点数显示格式
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

    sdData = getDataFromSt()
    top3Df = getRevenueTop3(sdData)
    top3Df = top3Df.groupby(['countryGroup']).agg(
        {
            # 'units':'mean',
            'revenue':'mean'
        }
    ).reset_index()
    top3Df = top3Df[['countryGroup','revenue']]
    # print(top3Df)

    lwData = getlastwarRevenue(sdData)
    lwData = lwData[['countryGroup','revenue']]
    # print(lwData)

    mergeDf = pd.merge(top3Df, lwData, on='countryGroup', how='left', suffixes=('_top3', '_lw'))
    mergeDf['lw/top3'] = mergeDf['revenue_lw'] / mergeDf['revenue_top3']

    mergeDf['lw/top3'] = mergeDf['lw/top3'].apply(lambda x: '%.2f%%'%(x*100))
    print(mergeDf)



if __name__ == '__main__':
    main()
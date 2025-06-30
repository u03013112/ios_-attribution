from fileinput import filename
import os
import datetime
from tracemalloc import start
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
sys.path.append('../..')
from src.maxCompute import execSql,getO


def getRevenueData(startDayStr, endDayStr):
    filename = f'/src/data/lw_cost_revenue_country_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f"""
select
    install_day,
    app_package,
    country,
    mediasource,
    campaign_name,
    sum(cost_value_usd) as cost,
    sum(revenue_h24) as revenue_h24,
    sum(revenue_d1) as revenue_d1,
    sum(revenue_d3) as revenue_d3,
    sum(revenue_d7) as revenue_d7,
    sum(revenue_d14) as revenue_d14,
    sum(revenue_d30) as revenue_d30,
    sum(revenue_d60) as revenue_d60,
    sum(revenue_d90) as revenue_d90,
    sum(revenue_d120) as revenue_d120,
    sum(revenue_d150) as revenue_d150
from
    dws_overseas_public_roi
where
    app = '502'
    and facebook_segment in ('country', ' ')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    app_package,
    country,
    mediasource,
    campaign_name
;
        """
        print(f"Executing SQL: {sql}")
        df = execSql(sql)

        df.to_csv(filename, index=False)

    return df

# 在getRevenueData基础上直接在sql中将step1的部分逻辑完成
def getRevenueData2(startDayStr, endDayStr):
    filename = f'/src/data/lw_cost_revenue_country2_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = f"""
SELECT
	install_date,
	mediasource,
	country_group,
	campaign_id,
	campaign_name,
	ad_type,
	SUM(revenue_1d) AS revenue_1d,
	SUM(revenue_3d) AS revenue_3d,
	SUM(revenue_7d) AS revenue_7d,
	SUM(revenue_14d) AS revenue_14d,
	SUM(revenue_28d) AS revenue_28d,
	SUM(revenue_60d) AS revenue_60d,
	SUM(revenue_90d) AS revenue_90d,
	SUM(revenue_120d) AS revenue_120d,
	SUM(revenue_150d) AS revenue_150d
FROM
	(
		SELECT
			a.install_date,
			a.mediasource,
			CASE
				WHEN a.country IN (
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
					'IL'
				) THEN 'T1'
				WHEN a.country IN (
					'BG',
					'BV',
					'BY',
					'ES',
					'GR',
					'HU',
					'ID',
					'KZ',
					'LT',
					'MA',
					'MY',
					'PH',
					'PL',
					'PT',
					'RO',
					'RS',
					'SI',
					'SK',
					'TH',
					'TM',
					'TR',
					'UZ',
					'ZA'
				) THEN 'T2'
				WHEN a.country IN (
					'AL',
					'AR',
					'BA',
					'BO',
					'BR',
					'CL',
					'CO',
					'CR',
					'CZ',
					'DZ',
					'EC',
					'EE',
					'EG',
					'FO',
					'GG',
					'GI',
					'GL',
					'GT',
					'HR',
					'IM',
					'IN',
					'IQ',
					'JE',
					'LV',
					'MD',
					'ME',
					'MK',
					'MT',
					'MX',
					'PA',
					'PE',
					'PY',
					'SM',
					'SR',
					'UA',
					'UY',
					'XK'
				) THEN 'T3'
				WHEN a.country = 'US' THEN 'US'
				WHEN a.country = 'JP' THEN 'JP'
				WHEN a.country = 'KR' THEN 'KR'
				WHEN a.country = 'TW' THEN 'TW'
				WHEN a.country IN ('SA', 'AE', 'QA', 'KW', 'BH', 'OM') THEN 'GCC'
				ELSE 'T3'
			END AS country_group,
			CASE
				WHEN a.mediasource = 'Facebook Ads' THEN CASE
					WHEN b.campaign_name LIKE '%BAU%' THEN 'BAU'
					WHEN b.campaign_name LIKE '%AAA%'
					OR b.campaign_name LIKE '%3A%' THEN 'AAA'
					ELSE ' '
				END
				WHEN a.mediasource = 'googleadwords_int' THEN CASE
					WHEN b.campaign_name LIKE '%3.0%' THEN '3.0'
					WHEN b.campaign_name LIKE '%2.5%' THEN '2.5'
					WHEN b.campaign_name LIKE '%1.0%' THEN '1.0'
					WHEN LOWER(b.campaign_name) LIKE '%smart%' THEN 'smart'
					ELSE ' '
				END
				ELSE ' '
			END AS ad_type,
			a.campaign_id,
			b.campaign_name as campaign_name,
			revenue_1d,
			revenue_3d,
			revenue_7d,
			revenue_14d,
			revenue_28d,
			revenue_60d,
			revenue_90d,
			revenue_120d,
			revenue_150d
		FROM
			(
				SELECT
					install_day AS install_date,
					mediasource,
					campaign_id,
					country,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 0 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_1d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 2 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_3d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 6 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_7d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 13 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_14d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 27 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_28d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 59 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_60d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 89 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_90d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 119 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_120d,
					SUM(
						CASE
							WHEN datediff(
								to_date(event_day, 'yyyymmdd'),
								to_date(install_day, 'yyyymmdd'),
								'dd'
							) <= 149 THEN revenue_value_usd
							ELSE 0
						END
					) AS revenue_150d
				FROM
					rg_bi.dwd_overseas_revenue_allproject
				WHERE
					zone = '0'
					AND app = '502'
					AND app_package = 'com.fun.lastwar.gp'
					AND day BETWEEN '{startDayStr}' AND '{endDayStr}'
				GROUP BY
					install_day,
					mediasource,
					campaign_id,
					country
			) a
			LEFT JOIN (
				select
					mediasource,
					campaign_id,
					campaign_name
				from
					dwb_overseas_mediasource_campaign_map
				group by
					mediasource,
					campaign_id,
					campaign_name
			) b ON a.mediasource = b.mediasource
			AND a.campaign_id = b.campaign_id
	) final_table
GROUP BY
	install_date,
	mediasource,
	country_group,
	campaign_id,
	campaign_name,
	ad_type;
        """
        df = execSql(sql)
        print(f"Executing SQL: {sql}")
        df.to_csv(filename, index=False)
        return df

def step1():
    df = getRevenueData('20240101', '20250501')
    print(df.head())

    # 将不满日的数据置为空
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

    # 添加广告类型
    # 只针对 'Facebook Ads' 和 'googleadwords_int'
    # 其他媒体广告类型都置' '
    df['ad_type'] = ' '
    # Facebook Ads 中 campaign_name 中包含 'BAU' 的置为 'BAU' , 包含 'AAA' 的置为 'AAA', 包含 '3A' 的置为 'AAA',
    # 其他置为 ' '
    df.loc[df['mediasource'] == 'Facebook Ads', 'ad_type'] = df['campaign_name'].apply(
        lambda x: 'BAU' if 'BAU' in x else ('AAA' if 'AAA' in x else ('AAA' if '3A' in x else ' '))
    )
    print('facebook 广告类型未成功匹配的共有：', df[(df['mediasource'] == 'Facebook Ads') & (df['ad_type'] == ' ')].shape[0])
    print(df[(df['mediasource'] == 'Facebook Ads') & (df['ad_type'] == ' ')]['campaign_name'].unique())
    # googleadwords_int 中 campaign_name 中包含 '3.0' 的置为 '3.0' , 包含 '2.5' 的置为 '2.5', 包含 '1.0' 的置为 '1.0', 包含 'smart' 的置为 'smart'
    # 其他置为 ' '
    df.loc[df['mediasource'] == 'googleadwords_int', 'ad_type'] = df['campaign_name'].apply(
        lambda x: '3.0' if '3.0' in x else ('2.5' if '2.5' in x else ('1.0' if '1.0' in x else ('smart' if 'smart' in x else ' ')))
    )
    print('google 广告类型未成功匹配的共有：', df[(df['mediasource'] == 'googleadwords_int') & (df['ad_type'] == ' ')].shape[0])
    print(df[(df['mediasource'] == 'googleadwords_int') & (df['ad_type'] == ' ')]['campaign_name'].unique())

    # 按照mediasource, country, ad_type, install_day 分组
    group_cols = ['app_package','mediasource', 'country', 'ad_type', 'install_day']
    df = df.groupby(group_cols).sum().reset_index()
    print('按广告类型分组后数据预览：')
    print(df.head())

    # 国家分组
    countryGroupMap = {
        'T1': ['AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','MO','IL'],
        'T2': ['BG','BV','BY','ES','GR','HU','ID','KZ','LT','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA'],
        'T3': ['AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','SM','SR','UA','UY','XK'],
        'US':['US'],
        'JP':['JP'],
        'KR':['KR'],
        'TW':['TW'],
        'GCC':['SA','AE','QA','KW','BH','OM']
    }
    df['country_group'] = 'T3'  # 默认分组为 T3
    for group, countries in countryGroupMap.items():
        df.loc[df['country'].isin(countries), 'country_group'] = group
    group_cols = ['app_package','mediasource', 'country_group', 'ad_type', 'install_day']
    df = df.groupby(group_cols).sum().reset_index()
    print('国家分组后数据预览：')
    print(df.head())

    for days in [1, 3, 7, 14, 30, 60, 90, 120, 150]:
        df[f'revenue_d{days}'] = df.apply(
            lambda row: row[f'revenue_d{days}'] if (yesterday - row['install_day']).days >= days else np.nan,
            axis=1
        )
    df = df.sort_values(by='install_day')
    print('将不满日的数据置为空 ok')

    df.to_csv('/src/data/lw_20250619_step1.csv', index=False)

# 按天汇总，分析'r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120'
def step2():
    df = pd.read_csv('/src/data/lw_20250619_step1.csv')
    # print(df.head())  
    # 暂时只看安卓
    df = df[df['app_package'] == 'com.fun.lastwar.gp'].copy()

    # 过滤掉收入较低的媒体
    mediaDf = df.groupby(['mediasource']).agg({'revenue_d7':'sum'}).reset_index()
    mediaDf = mediaDf.sort_values(by='revenue_d7', ascending=False)
    mediaDf = mediaDf[mediaDf['revenue_d7'] > 10000]
    mediaList = mediaDf['mediasource'].tolist()
    df = df[df['mediasource'].isin(mediaList)].copy()
    

    # 计算收入比率
    df = df[[
        'app_package', 'mediasource', 'country_group', 'ad_type', 'install_day',
        'revenue_d1', 'revenue_d3', 'revenue_d7', 'revenue_d14', 'revenue_d30', 
        'revenue_d60', 'revenue_d90', 'revenue_d120', 'revenue_d150'
    ]].copy()

    df['r3/r1'] = df['revenue_d3'] / df['revenue_d1']
    df['r7/r3'] = df['revenue_d7'] / df['revenue_d3']
    df['r14/r7'] = df['revenue_d14'] / df['revenue_d7']
    df['r30/r14'] = df['revenue_d30'] / df['revenue_d14']
    df['r60/r30'] = df['revenue_d60'] / df['revenue_d30']
    df['r90/r60'] = df['revenue_d90'] / df['revenue_d60']
    df['r120/r90'] = df['revenue_d120'] / df['revenue_d90']
    df['r150/r120'] = df['revenue_d150'] / df['revenue_d120']
    
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    resaultDf = pd.DataFrame(columns=[
        'app_package', 'mediasource', 'country_group', 'ad_type',
        'std_r3/r1', 'std_r7/r3', 'std_r14/r7', 'std_r30/r14',
        'std_r60/r30', 'std_r90/r60', 'std_r120/r90', 'std_r150/r120',
        'mean_r3/r1', 'mean_r7/r3', 'mean_r14/r7', 'mean_r30/r14',
        'mean_r60/r30', 'mean_r90/r60', 'mean_r120/r90', 'mean_r150/r120',
        'cv_r3/r1', 'cv_r7/r3', 'cv_r14/r7', 'cv_r30/r14',
        'cv_r60/r30', 'cv_r90/r60', 'cv_r120/r90', 'cv_r150/r120',
        'iqr_r3/r1', 'iqr_r7/r3', 'iqr_r14/r7', 'iqr_r30/r14',
        'iqr_r60/r30', 'iqr_r90/r60', 'iqr_r120/r90', 'iqr_r150/r120'
    ])
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type']
    groupedDf = df.groupby(group_cols)
    for name, group in groupedDf:
        print(f"Processing group: {name}")
        # # for debug
        # if name[0] == 'com.fun.lastwar.gp' and name[1] == 'Facebook Ads' and name[2] == 'GCC' and name[3] == 'AAA':
        #     group.to_csv('/src/data/lw_debug_group.csv', index=False)
        #     break
            

        df0 = group[['r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120']].copy()

        std_values = df0.std(skipna=True).tolist()
        mean_values = df0.mean(skipna=True).tolist()
        cv_values = (df0.std(skipna=True) / df0.mean(skipna=True)).tolist()
        iqr_values = df0.quantile(0.75) - df0.quantile(0.25)
        iqr_values = iqr_values.tolist()

        # 将结果添加到resaultDf
        resaultDf = resaultDf.append({
            'app_package': name[0],
            'mediasource': name[1],
            'country_group': name[2],
            'ad_type': name[3],
            'std_r3/r1': std_values[0],
            'std_r7/r3': std_values[1],
            'std_r14/r7': std_values[2],
            'std_r30/r14': std_values[3],
            'std_r60/r30': std_values[4],
            'std_r90/r60': std_values[5],
            'std_r120/r90': std_values[6],
            'std_r150/r120': std_values[7],
            'mean_r3/r1': mean_values[0],
            'mean_r7/r3': mean_values[1],
            'mean_r14/r7': mean_values[2],
            'mean_r30/r14': mean_values[3],
            'mean_r60/r30': mean_values[4],
            'mean_r90/r60': mean_values[5],
            'mean_r120/r90': mean_values[6],
            'mean_r150/r120': mean_values[7],
            'cv_r3/r1': cv_values[0],
            'cv_r7/r3': cv_values[1],
            'cv_r14/r7': cv_values[2],
            'cv_r30/r14': cv_values[3],
            'cv_r60/r30': cv_values[4],
            'cv_r90/r60': cv_values[5],
            'cv_r120/r90': cv_values[6],
            'cv_r150/r120': cv_values[7],
            'iqr_r3/r1': iqr_values[0],
            'iqr_r7/r3': iqr_values[1],
            'iqr_r14/r7': iqr_values[2],
            'iqr_r30/r14': iqr_values[3],
            'iqr_r60/r30': iqr_values[4],
            'iqr_r90/r60': iqr_values[5],
            'iqr_r120/r90': iqr_values[6],
            'iqr_r150/r120': iqr_values[7]
        }, ignore_index=True)

        # 绘图
        group['install_day'] = pd.to_datetime(group['install_day'], format='%Y-%m-%d')
        plt.figure(figsize=(36, 6))
        for col in ['r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120']:
            plt.plot(group['install_day'], group[col], label=col)
        plt.xlabel('Install Day')
        plt.ylabel('Revenue Ratios')
        plt.title(f'Ratios over time\n{name[0]} | {name[1]} | {name[2]} | {name[3]}')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # 文件名处理
        safe_name = "_".join([str(n).replace(" ", "_") for n in name])
        img_path = os.path.join('/src/data/', f"{safe_name}.png")
        plt.savefig(img_path)
        plt.close()
        print(f"Plot saved to {img_path}")


    resaultDf = resaultDf.sort_values(by=['app_package', 'mediasource', 'country_group', 'ad_type'])
    resaultDf.to_csv('/src/data/lw_20250619_step2.csv', index=False)
    print('CV计算完成，结果已保存到 /src/data/lw_20250619_step2.csv')

# 按天汇总，分析'r30/r7', 'r150/r30'
def step2_f():
    df = pd.read_csv('/src/data/lw_20250619_step1.csv')
    # print(df.head())  
    # 暂时只看安卓
    df = df[df['app_package'] == 'com.fun.lastwar.gp'].copy()

    # 过滤掉收入较低的媒体
    mediaDf = df.groupby(['mediasource']).agg({'revenue_d7':'sum'}).reset_index()
    mediaDf = mediaDf.sort_values(by='revenue_d7', ascending=False)
    mediaDf = mediaDf[mediaDf['revenue_d7'] > 10000]
    mediaList = mediaDf['mediasource'].tolist()
    df = df[df['mediasource'].isin(mediaList)].copy()
    

    # 计算收入比率
    df = df[[
        'app_package', 'mediasource', 'country_group', 'ad_type', 'install_day',
        'revenue_d1', 'revenue_d3', 'revenue_d7', 'revenue_d14', 'revenue_d30', 
        'revenue_d60', 'revenue_d90', 'revenue_d120', 'revenue_d150'
    ]].copy()

    df['r30/r7'] = df['revenue_d30'] / df['revenue_d7']
    df['r150/r30'] = df['revenue_d150'] / df['revenue_d30']
    
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    resaultDf = pd.DataFrame(columns=[
        'app_package', 'mediasource', 'country_group', 'ad_type',
        'std_r30/r7', 'std_r150/r30',
        'mean_r30/r7', 'mean_r150/r30',
        'cv_r30/r7', 'cv_r150/r30',
        'iqr_r30/r7', 'iqr_r150/r30'
    ])
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type']
    groupedDf = df.groupby(group_cols)
    for name, group in groupedDf:
        print(f"Processing group: {name}")
        # # for debug
        # if name[0] == 'com.fun.lastwar.gp' and name[1] == 'Facebook Ads' and name[2] == 'GCC' and name[3] == 'AAA':
        #     group.to_csv('/src/data/lw_debug_group.csv', index=False)
        #     break
            

        df0 = group[['r30/r7', 'r150/r30']].copy()

        std_values = df0.std(skipna=True).tolist()
        mean_values = df0.mean(skipna=True).tolist()
        cv_values = (df0.std(skipna=True) / df0.mean(skipna=True)).tolist()
        iqr_values = df0.quantile(0.75) - df0.quantile(0.25)
        iqr_values = iqr_values.tolist()

        # 将结果添加到resaultDf
        resaultDf = resaultDf.append({
            'app_package': name[0],
            'mediasource': name[1],
            'country_group': name[2],
            'ad_type': name[3],
            'std_r30/r7': std_values[0],
            'std_r150/r30': std_values[1],
            'mean_r30/r7': mean_values[0],
            'mean_r150/r30': mean_values[1],
            'cv_r30/r7': cv_values[0],
            'cv_r150/r30': cv_values[1],
            'iqr_r30/r7': iqr_values[0],
            'iqr_r150/r30': iqr_values[1]
        }, ignore_index=True)

        # 绘图
        group['install_day'] = pd.to_datetime(group['install_day'], format='%Y-%m-%d')
        plt.figure(figsize=(36, 6))
        for col in ['r30/r7', 'r150/r30']:
            plt.plot(group['install_day'], group[col], label=col)
        plt.xlabel('Install Day')
        plt.ylabel('Revenue Ratios')
        plt.title(f'Ratios over time\n{name[0]} | {name[1]} | {name[2]} | {name[3]}')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # 文件名处理
        safe_name = "_".join([str(n).replace(" ", "_") for n in name])
        img_path = os.path.join('/src/data/', f"{safe_name}_f.png")
        plt.savefig(img_path)
        plt.close()
        print(f"Plot saved to {img_path}")


    resaultDf = resaultDf.sort_values(by=['app_package', 'mediasource', 'country_group', 'ad_type'])
    resaultDf.to_csv('/src/data/lw_20250619_step2f.csv', index=False)
    print('CV计算完成，结果已保存到 /src/data/lw_20250619_step2f.csv')


# 按周汇总，分析'r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120'
def step3():
    df = pd.read_csv('/src/data/lw_20250619_step1.csv')
    # 暂时只看安卓
    df = df[df['app_package'] == 'com.fun.lastwar.gp'].copy()
    # 过滤掉收入较低的媒体
    mediaDf = df.groupby(['mediasource']).agg({'revenue_d7':'sum'}).reset_index()
    mediaDf = mediaDf.sort_values(by='revenue_d7', ascending=False)
    mediaDf = mediaDf[mediaDf['revenue_d7'] > 10000]
    mediaList = mediaDf['mediasource'].tolist()
    df = df[df['mediasource'].isin(mediaList)].copy()

    # 日期转为周
    df['install_day'] = pd.to_datetime(df['install_day'])
    df['install_week'] = df['install_day'].dt.to_period('W').apply(lambda r: r.start_time)
    # 按周汇总数据
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type', 'install_week']
    agg_cols = [
        'revenue_d1', 'revenue_d3', 'revenue_d7', 'revenue_d14', 'revenue_d30', 
        'revenue_d60', 'revenue_d90', 'revenue_d120', 'revenue_d150'
    ]
    df_weekly = df.groupby(['app_package', 'mediasource', 'country_group', 'ad_type', 'install_week'], as_index=False)[agg_cols].sum()
    # 计算收入比率
    df_weekly['r3/r1'] = df_weekly['revenue_d3'] / df_weekly['revenue_d1']
    df_weekly['r7/r3'] = df_weekly['revenue_d7'] / df_weekly['revenue_d3']
    df_weekly['r14/r7'] = df_weekly['revenue_d14'] / df_weekly['revenue_d7']
    df_weekly['r30/r14'] = df_weekly['revenue_d30'] / df_weekly['revenue_d14']
    df_weekly['r60/r30'] = df_weekly['revenue_d60'] / df_weekly['revenue_d30']
    df_weekly['r90/r60'] = df_weekly['revenue_d90'] / df_weekly['revenue_d60']
    df_weekly['r120/r90'] = df_weekly['revenue_d120'] / df_weekly['revenue_d90']
    df_weekly['r150/r120'] = df_weekly['revenue_d150'] / df_weekly['revenue_d120']
    df_weekly.replace([np.inf, -np.inf], np.nan, inplace=True)
    # 存放结果的DataFrame
    resaultDf = pd.DataFrame(columns=[
        'app_package', 'mediasource', 'country_group', 'ad_type',
        'std_r3/r1', 'std_r7/r3', 'std_r14/r7', 'std_r30/r14',
        'std_r60/r30', 'std_r90/r60', 'std_r120/r90', 'std_r150/r120',
        'mean_r3/r1', 'mean_r7/r3', 'mean_r14/r7', 'mean_r30/r14',
        'mean_r60/r30', 'mean_r90/r60', 'mean_r120/r90', 'mean_r150/r120',
        'cv_r3/r1', 'cv_r7/r3', 'cv_r14/r7', 'cv_r30/r14',
        'cv_r60/r30', 'cv_r90/r60', 'cv_r120/r90', 'cv_r150/r120',
        'iqr_r3/r1', 'iqr_r7/r3', 'iqr_r14/r7', 'iqr_r30/r14',
        'iqr_r60/r30', 'iqr_r90/r60', 'iqr_r120/r90', 'iqr_r150/r120'
    ])
    df_weekly.replace([np.inf, -np.inf], np.nan, inplace=True)
    # 创建目录保存图片
    img_dir = '/src/data/group_weekly_plots'
    os.makedirs(img_dir, exist_ok=True)
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type']
    groupedDf = df_weekly.groupby(group_cols)
    for name, group in groupedDf:
        print(f"Processing weekly group: {name}")
        df0 = group[['install_week','r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120']].copy()
        df0.replace([np.inf, -np.inf], np.nan, inplace=True)
        # 计算统计指标
        std_values = df0.iloc[:, 1:].std(skipna=True).tolist()
        mean_values = df0.iloc[:, 1:].mean(skipna=True).tolist()
        cv_values = (df0.iloc[:, 1:].std(skipna=True) / df0.iloc[:, 1:].mean(skipna=True)).tolist()
        iqr_values = (df0.iloc[:, 1:].quantile(0.75) - df0.iloc[:, 1:].quantile(0.25)).tolist()
        # 追加到结果DataFrame
        resaultDf = resaultDf.append({
            'app_package': name[0],
            'mediasource': name[1],
            'country_group': name[2],
            'ad_type': name[3],
            'std_r3/r1': std_values[0], 'std_r7/r3': std_values[1], 'std_r14/r7': std_values[2], 'std_r30/r14': std_values[3],
            'std_r60/r30': std_values[4], 'std_r90/r60': std_values[5], 'std_r120/r90': std_values[6], 'std_r150/r120': std_values[7],
            'mean_r3/r1': mean_values[0], 'mean_r7/r3': mean_values[1], 'mean_r14/r7': mean_values[2], 'mean_r30/r14': mean_values[3],
            'mean_r60/r30': mean_values[4], 'mean_r90/r60': mean_values[5], 'mean_r120/r90': mean_values[6], 'mean_r150/r120': mean_values[7],
            'cv_r3/r1': cv_values[0], 'cv_r7/r3': cv_values[1], 'cv_r14/r7': cv_values[2], 'cv_r30/r14': cv_values[3],
            'cv_r60/r30': cv_values[4], 'cv_r90/r60': cv_values[5], 'cv_r120/r90': cv_values[6], 'cv_r150/r120': cv_values[7],
            'iqr_r3/r1': iqr_values[0], 'iqr_r7/r3': iqr_values[1], 'iqr_r14/r7': iqr_values[2], 'iqr_r30/r14': iqr_values[3],
            'iqr_r60/r30': iqr_values[4], 'iqr_r90/r60': iqr_values[5], 'iqr_r120/r90': iqr_values[6], 'iqr_r150/r120': iqr_values[7]
        }, ignore_index=True)
        # 绘图
        plt.figure(figsize=(36, 6))
        for col in ['r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120']:
            plt.plot(df0['install_week'].astype(str), df0[col],label=col)
        plt.xlabel('Install Week')
        plt.ylabel('Revenue Ratios')
        plt.title(f'Weekly Ratios - {name[0]} | {name[1]} | {name[2]} | {name[3]}')
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # 文件名处理
        safe_name = "_".join([str(n).replace(" ", "_") for n in name])
        img_path = os.path.join(img_dir, f"{safe_name}_weekly.png")
        plt.savefig(img_path)
        plt.close()
        print(f"Weekly plot saved to {img_path}")
    resaultDf = resaultDf.sort_values(by=['app_package', 'mediasource', 'country_group', 'ad_type'])
    resaultDf.to_csv('/src/data/lw_20250619_step3_weekly.csv', index=False)
    print('Weekly CV计算完成，结果已保存到 /src/data/lw_20250619_step3.csv')
    print(f'所有分组图表已保存至 {img_dir}')

# 按周汇总，分析'r30/r7', 'r150/r30'
def step3_f():
    df = pd.read_csv('/src/data/lw_20250619_step1.csv')
    # 暂时只看安卓
    df = df[df['app_package'] == 'com.fun.lastwar.gp'].copy()
    # 过滤掉收入较低的媒体
    mediaDf = df.groupby(['mediasource']).agg({'revenue_d7':'sum'}).reset_index()
    mediaDf = mediaDf.sort_values(by='revenue_d7', ascending=False)
    mediaDf = mediaDf[mediaDf['revenue_d7'] > 10000]
    mediaList = mediaDf['mediasource'].tolist()
    df = df[df['mediasource'].isin(mediaList)].copy()
    # 日期转为周
    df['install_day'] = pd.to_datetime(df['install_day'])
    df['install_week'] = df['install_day'].dt.to_period('W').apply(lambda r: r.start_time)
    # 按周汇总数据
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type', 'install_week']
    agg_cols = [
        'revenue_d7', 'revenue_d30', 'revenue_d150'
    ]
    df_weekly = df.groupby(group_cols, as_index=False)[agg_cols].sum()
    # 计算收入比率
    df_weekly['r30/r7'] = df_weekly['revenue_d30'] / df_weekly['revenue_d7']
    df_weekly['r150/r30'] = df_weekly['revenue_d150'] / df_weekly['revenue_d30']
    df_weekly.replace([np.inf, -np.inf], np.nan, inplace=True)
    # 存放结果的DataFrame
    resaultDf = pd.DataFrame(columns=[
        'app_package', 'mediasource', 'country_group', 'ad_type',
        'std_r30/r7', 'std_r150/r30',
        'mean_r30/r7', 'mean_r150/r30',
        'cv_r30/r7', 'cv_r150/r30',
        'iqr_r30/r7', 'iqr_r150/r30'
    ])
    # 创建目录保存图片
    img_dir = '/src/data/group_weekly_plots_f'
    os.makedirs(img_dir, exist_ok=True)
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type']
    groupedDf = df_weekly.groupby(group_cols)
    for name, group in groupedDf:
        print(f"Processing weekly group: {name}")
        df0 = group[['install_week', 'r30/r7', 'r150/r30']].copy()
        df0.replace([np.inf, -np.inf], np.nan, inplace=True)
        # 计算统计指标
        std_values = df0.iloc[:, 1:].std(skipna=True).tolist()
        mean_values = df0.iloc[:, 1:].mean(skipna=True).tolist()
        cv_values = (df0.iloc[:, 1:].std(skipna=True) / df0.iloc[:, 1:].mean(skipna=True)).tolist()
        iqr_values = (df0.iloc[:, 1:].quantile(0.75) - df0.iloc[:, 1:].quantile(0.25)).tolist()
        # 追加到结果DataFrame
        resaultDf = resaultDf.append({
            'app_package': name[0],
            'mediasource': name[1],
            'country_group': name[2],
            'ad_type': name[3],
            'std_r30/r7': std_values[0], 'std_r150/r30': std_values[1],
            'mean_r30/r7': mean_values[0], 'mean_r150/r30': mean_values[1],
            'cv_r30/r7': cv_values[0], 'cv_r150/r30': cv_values[1],
            'iqr_r30/r7': iqr_values[0], 'iqr_r150/r30': iqr_values[1]
        }, ignore_index=True)
        # 绘图
        plt.figure(figsize=(24, 6))
        for col in ['r30/r7', 'r150/r30']:
            plt.plot(df0['install_week'].astype(str), df0[col], marker='o', label=col)
        plt.xlabel('Install Week')
        plt.ylabel('Revenue Ratios')
        plt.title(f'Weekly Ratios - {name[0]} | {name[1]} | {name[2]} | {name[3]}')
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # 文件名处理
        safe_name = "_".join([str(n).replace(" ", "_") for n in name])
        img_path = os.path.join(img_dir, f"{safe_name}_weekly_f.png")
        plt.savefig(img_path)
        plt.close()
        print(f"Weekly plot saved to {img_path}")
    resaultDf = resaultDf.sort_values(by=['app_package', 'mediasource', 'country_group', 'ad_type'])
    resaultDf.to_csv('/src/data/lw_20250619_step3f_weekly.csv', index=False)
    print('Weekly CV计算完成，结果已保存到 /src/data/lw_20250619_step3f_weekly.csv')
    print(f'所有分组图表已保存至 {img_dir}')

# 按月汇总，分析'r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120'     
def step4():
    df = pd.read_csv('/src/data/lw_20250619_step1.csv')
    # 暂时只看安卓
    df = df[df['app_package'] == 'com.fun.lastwar.gp'].copy()
    # 过滤掉收入较低的媒体 (和step3一致)
    mediaDf = df.groupby(['mediasource']).agg({'revenue_d7':'sum'}).reset_index()
    mediaDf = mediaDf.sort_values(by='revenue_d7', ascending=False)
    mediaDf = mediaDf[mediaDf['revenue_d7'] > 10000]
    mediaList = mediaDf['mediasource'].tolist()
    df = df[df['mediasource'].isin(mediaList)].copy()
    # 日期转为月（每月第一天）
    df['install_day'] = pd.to_datetime(df['install_day'])
    df['install_month'] = df['install_day'].dt.to_period('M').apply(lambda r: r.start_time)
    # 按月汇总数据
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type', 'install_month']
    agg_cols = [
        'revenue_d1', 'revenue_d3', 'revenue_d7', 'revenue_d14', 'revenue_d30', 
        'revenue_d60', 'revenue_d90', 'revenue_d120', 'revenue_d150'
    ]
    df_monthly = df.groupby(group_cols, as_index=False)[agg_cols].sum()
    # 计算收入比率
    df_monthly['r3/r1'] = df_monthly['revenue_d3'] / df_monthly['revenue_d1']
    df_monthly['r7/r3'] = df_monthly['revenue_d7'] / df_monthly['revenue_d3']
    df_monthly['r14/r7'] = df_monthly['revenue_d14'] / df_monthly['revenue_d7']
    df_monthly['r30/r14'] = df_monthly['revenue_d30'] / df_monthly['revenue_d14']
    df_monthly['r60/r30'] = df_monthly['revenue_d60'] / df_monthly['revenue_d30']
    df_monthly['r90/r60'] = df_monthly['revenue_d90'] / df_monthly['revenue_d60']
    df_monthly['r120/r90'] = df_monthly['revenue_d120'] / df_monthly['revenue_d90']
    df_monthly['r150/r120'] = df_monthly['revenue_d150'] / df_monthly['revenue_d120']
    df_monthly.replace([np.inf, -np.inf], np.nan, inplace=True)
    # 存放结果的DataFrame
    resaultDf = pd.DataFrame(columns=[
        'app_package', 'mediasource', 'country_group', 'ad_type',
        'std_r3/r1', 'std_r7/r3', 'std_r14/r7', 'std_r30/r14',
        'std_r60/r30', 'std_r90/r60', 'std_r120/r90', 'std_r150/r120',
        'mean_r3/r1', 'mean_r7/r3', 'mean_r14/r7', 'mean_r30/r14',
        'mean_r60/r30', 'mean_r90/r60', 'mean_r120/r90', 'mean_r150/r120',
        'cv_r3/r1', 'cv_r7/r3', 'cv_r14/r7', 'cv_r30/r14',
        'cv_r60/r30', 'cv_r90/r60', 'cv_r120/r90', 'cv_r150/r120',
        'iqr_r3/r1', 'iqr_r7/r3', 'iqr_r14/r7', 'iqr_r30/r14',
        'iqr_r60/r30', 'iqr_r90/r60', 'iqr_r120/r90', 'iqr_r150/r120'
    ])
    # 创建目录保存图片
    img_dir = '/src/data/group_monthly_plots'
    os.makedirs(img_dir, exist_ok=True)
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type']
    groupedDf = df_monthly.groupby(group_cols)
    for name, group in groupedDf:
        print(f"Processing monthly group: {name}")
        df0 = group[['install_month', 'r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120']].copy()
        df0.replace([np.inf, -np.inf], np.nan, inplace=True)
        # 计算统计指标
        std_values = df0.iloc[:, 1:].std(skipna=True).tolist()
        mean_values = df0.iloc[:, 1:].mean(skipna=True).tolist()
        cv_values = (df0.iloc[:, 1:].std(skipna=True) / df0.iloc[:, 1:].mean(skipna=True)).tolist()
        iqr_values = (df0.iloc[:, 1:].quantile(0.75) - df0.iloc[:, 1:].quantile(0.25)).tolist()
        # 追加到结果DataFrame
        resaultDf = resaultDf.append({
            'app_package': name[0], 'mediasource': name[1], 'country_group': name[2], 'ad_type': name[3],
            'std_r3/r1': std_values[0], 'std_r7/r3': std_values[1], 'std_r14/r7': std_values[2], 'std_r30/r14': std_values[3],
            'std_r60/r30': std_values[4], 'std_r90/r60': std_values[5], 'std_r120/r90': std_values[6], 'std_r150/r120': std_values[7],
            'mean_r3/r1': mean_values[0], 'mean_r7/r3': mean_values[1], 'mean_r14/r7': mean_values[2], 'mean_r30/r14': mean_values[3],
            'mean_r60/r30': mean_values[4], 'mean_r90/r60': mean_values[5], 'mean_r120/r90': mean_values[6], 'mean_r150/r120': mean_values[7],
            'cv_r3/r1': cv_values[0], 'cv_r7/r3': cv_values[1], 'cv_r14/r7': cv_values[2], 'cv_r30/r14': cv_values[3],
            'cv_r60/r30': cv_values[4], 'cv_r90/r60': cv_values[5], 'cv_r120/r90': cv_values[6], 'cv_r150/r120': cv_values[7],
            'iqr_r3/r1': iqr_values[0], 'iqr_r7/r3': iqr_values[1], 'iqr_r14/r7': iqr_values[2], 'iqr_r30/r14': iqr_values[3],
            'iqr_r60/r30': iqr_values[4], 'iqr_r90/r60': iqr_values[5], 'iqr_r120/r90': iqr_values[6], 'iqr_r150/r120': iqr_values[7]
        }, ignore_index=True)
        # 绘图
        plt.figure(figsize=(36, 6))
        for col in ['r3/r1', 'r7/r3', 'r14/r7', 'r30/r14', 'r60/r30', 'r90/r60', 'r120/r90', 'r150/r120']:
            plt.plot(df0['install_month'].astype(str), df0[col], label=col)
        plt.xlabel('Install Month')
        plt.ylabel('Revenue Ratios')
        plt.title(f'Monthly Ratios - {name[0]} | {name[1]} | {name[2]} | {name[3]}')
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # 保存图片
        safe_name = "_".join([str(n).replace(" ", "_") for n in name])
        img_path = os.path.join(img_dir, f"{safe_name}_monthly.png")
        plt.savefig(img_path)
        plt.close()
        print(f"Monthly plot saved to {img_path}")
    resaultDf.to_csv('/src/data/lw_20250619_step4_monthly.csv', index=False)
    print('Monthly CV计算完成，结果已保存到 /src/data/lw_20250619_step4_monthly.csv')
    print(f'所有分组图表已保存至 {img_dir}')

# 按月汇总，分析'r30/r7', 'r150/r30'
def step4_f():
    df = pd.read_csv('/src/data/lw_20250619_step1.csv')
    # 暂时只看安卓
    df = df[df['app_package'] == 'com.fun.lastwar.gp'].copy()
    # 过滤掉收入较低的媒体
    mediaDf = df.groupby(['mediasource']).agg({'revenue_d7':'sum'}).reset_index()
    mediaDf = mediaDf.sort_values(by='revenue_d7', ascending=False)
    mediaDf = mediaDf[mediaDf['revenue_d7'] > 10000]
    mediaList = mediaDf['mediasource'].tolist()
    df = df[df['mediasource'].isin(mediaList)].copy()
    # 日期转为月（每月第一天）
    df['install_day'] = pd.to_datetime(df['install_day'])
    df['install_month'] = df['install_day'].dt.to_period('M').apply(lambda r: r.start_time)
    # 按月汇总数据（仅需revenue_d7, revenue_d30, revenue_d150）
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type', 'install_month']
    agg_cols = ['revenue_d7', 'revenue_d30', 'revenue_d150']
    df_monthly = df.groupby(group_cols, as_index=False)[agg_cols].sum()
    # 计算收入比率
    df_monthly['r30/r7'] = df_monthly['revenue_d30'] / df_monthly['revenue_d7']
    df_monthly['r150/r30'] = df_monthly['revenue_d150'] / df_monthly['revenue_d30']
    df_monthly.replace([np.inf, -np.inf], np.nan, inplace=True)
    # 存放结果的DataFrame
    resaultDf = pd.DataFrame(columns=[
        'app_package', 'mediasource', 'country_group', 'ad_type',
        'std_r30/r7', 'std_r150/r30',
        'mean_r30/r7', 'mean_r150/r30',
        'cv_r30/r7', 'cv_r150/r30',
        'iqr_r30/r7', 'iqr_r150/r30'
    ])
    # 创建目录保存图片
    img_dir = '/src/data/group_monthly_plots_f'
    os.makedirs(img_dir, exist_ok=True)
    group_cols = ['app_package', 'mediasource', 'country_group', 'ad_type']
    groupedDf = df_monthly.groupby(group_cols)
    for name, group in groupedDf:
        print(f"Processing monthly group: {name}")
        df0 = group[['install_month', 'r30/r7', 'r150/r30']].copy()
        df0.replace([np.inf, -np.inf], np.nan, inplace=True)
        # 计算统计指标
        std_values = df0.iloc[:, 1:].std(skipna=True).tolist()
        mean_values = df0.iloc[:, 1:].mean(skipna=True).tolist()
        cv_values = (df0.iloc[:, 1:].std(skipna=True) / df0.iloc[:, 1:].mean(skipna=True)).tolist()
        iqr_values = (df0.iloc[:, 1:].quantile(0.75) - df0.iloc[:, 1:].quantile(0.25)).tolist()
        # 追加到结果DataFrame
        resaultDf = resaultDf.append({
            'app_package': name[0],
            'mediasource': name[1],
            'country_group': name[2],
            'ad_type': name[3],
            'std_r30/r7': std_values[0], 'std_r150/r30': std_values[1],
            'mean_r30/r7': mean_values[0], 'mean_r150/r30': mean_values[1],
            'cv_r30/r7': cv_values[0], 'cv_r150/r30': cv_values[1],
            'iqr_r30/r7': iqr_values[0], 'iqr_r150/r30': iqr_values[1]
        }, ignore_index=True)
        # 绘图
        plt.figure(figsize=(36, 6))
        for col in ['r30/r7', 'r150/r30']:
            plt.plot(df0['install_month'].astype(str), df0[col], marker='o', label=col)
        plt.xlabel('Install Month')
        plt.ylabel('Revenue Ratios')
        plt.title(f'Monthly Ratios - {name[0]} | {name[1]} | {name[2]} | {name[3]}')
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # 保存图片
        safe_name = "_".join([str(n).replace(" ", "_") for n in name])
        img_path = os.path.join(img_dir, f"{safe_name}_monthly_f.png")
        plt.savefig(img_path)
        plt.close()
        print(f"Monthly plot saved to {img_path}")
    resaultDf = resaultDf.sort_values(by=['app_package', 'mediasource', 'country_group', 'ad_type'])
    resaultDf.to_csv('/src/data/lw_20250619_step4f_monthly.csv', index=False)
    print('Monthly CV计算完成，结果已保存到 /src/data/lw_20250619_step4f_monthly.csv')
    print(f'所有分组图表已保存至 {img_dir}')


def getRevenueDataNerf(startDayStr, endDayStr):
    filename = f'/src/data/lw_revenue_nerf_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        sql1 = f'''
SELECT
    install_date,
    mediasource,
    country_group,
    ad_type,

    SUM(revenue_7d) AS revenue_7d,
    COUNT(DISTINCT CASE WHEN revenue_7d > 0 THEN game_uid ELSE NULL END) AS puser_count_7d,
    MAX(revenue_7d) AS max_one_user_revenue_7d,

    SUM(revenue_28d) AS revenue_28d,
    COUNT(DISTINCT CASE WHEN revenue_28d > 0 THEN game_uid ELSE NULL END) AS puser_count_28d,
    MAX(revenue_28d) AS max_one_user_revenue_28d,

    SUM(revenue_150d) AS revenue_150d,
    COUNT(DISTINCT CASE WHEN revenue_150d > 0 THEN game_uid ELSE NULL END) AS puser_count_150d,
    MAX(revenue_150d) AS max_one_user_revenue_150d

FROM
(
    SELECT
        a.install_day AS install_date,
        a.mediasource,
        a.campaign_id,
        a.country,
        a.game_uid,
        b.campaign_name,

        CASE
            WHEN a.country IN ('AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','MO','IL') THEN 'T1'
            WHEN a.country IN ('BG','BV','BY','ES','GR','HU','ID','KZ','LT','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA') THEN 'T2'
            WHEN a.country IN ('AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','SM','SR','UA','UY','XK') THEN 'T3'
            WHEN a.country = 'US' THEN 'US'
            WHEN a.country = 'JP' THEN 'JP'
            WHEN a.country = 'KR' THEN 'KR'
            WHEN a.country = 'TW' THEN 'TW'
            WHEN a.country IN ('SA','AE','QA','KW','BH','OM') THEN 'GCC'
            ELSE 'T3'
        END AS country_group,

        CASE
            WHEN a.mediasource = 'Facebook Ads' THEN CASE
                WHEN b.campaign_name LIKE '%BAU%' THEN 'BAU'
                WHEN b.campaign_name LIKE '%AAA%' OR b.campaign_name LIKE '%3A%' THEN 'AAA'
                ELSE ' '
            END
            WHEN a.mediasource = 'googleadwords_int' THEN CASE
                WHEN b.campaign_name LIKE '%3.0%' THEN '3.0'
                WHEN b.campaign_name LIKE '%2.5%' THEN '2.5'
                WHEN b.campaign_name LIKE '%1.0%' THEN '1.0'
                WHEN LOWER(b.campaign_name) LIKE '%smart%' THEN 'smart'
                ELSE ' '
            END
            ELSE ' '
        END AS ad_type,

        SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 6 THEN revenue_value_usd ELSE 0 END) AS revenue_7d,
        SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 27 THEN revenue_value_usd ELSE 0 END) AS revenue_28d,
        SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 149 THEN revenue_value_usd ELSE 0 END) AS revenue_150d

    FROM
        rg_bi.dwd_overseas_revenue_allproject a
    LEFT JOIN (
        SELECT mediasource, campaign_id, campaign_name
        FROM dwb_overseas_mediasource_campaign_map
        GROUP BY mediasource, campaign_id, campaign_name
    ) b
    ON a.mediasource = b.mediasource AND a.campaign_id = b.campaign_id

    WHERE
        a.zone = '0'
        AND a.app = '502'
        AND a.app_package = 'com.fun.lastwar.gp'
        AND a.day BETWEEN '{startDayStr}' AND '{endDayStr}'
        AND a.game_uid IS NOT NULL

    GROUP BY
        a.install_day,
        a.mediasource,
        a.campaign_id,
        a.country,
        a.game_uid,
        b.campaign_name
) user_level_revenue

GROUP BY
    install_date,
    mediasource,
    country_group,
    ad_type
;
        '''
        print(f"Executing SQL: {sql1}")
        df = execSql(sql1)
        df.to_csv(filename, index=False)
    return df

def getRevenueData0Raw(startDayStr, endDayStr):
    filename = f'/src/data/lw_revenue_0_raw_{startDayStr}_{endDayStr}.csv'
    if os.path.exists(filename):
        # install_date 按照字符串读取
        return pd.read_csv(filename, dtype={'install_date': str})
    else:
        startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
        endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')
        # 由于需要135天的数据，所以endDay需要加上135天，保守起见，加140天
        endDay += datetime.timedelta(days=140)
        endDayStr135 = endDay.strftime('%Y%m%d')
        sql = f'''
SELECT
    install_date,
    mediasource,
    country_group,
    ad_type,

    SUM(revenue_1d) AS revenue_1d,
    COUNT(DISTINCT CASE WHEN revenue_1d > 0 THEN game_uid ELSE NULL END) AS puser_count_1d,
    MAX(revenue_1d) AS max_one_user_revenue_1d,

    SUM(revenue_7d) AS revenue_7d,
    COUNT(DISTINCT CASE WHEN revenue_7d > 0 THEN game_uid ELSE NULL END) AS puser_count_7d,
    MAX(revenue_7d) AS max_one_user_revenue_7d,

    SUM(revenue_120d) AS revenue_120d,
    COUNT(DISTINCT CASE WHEN revenue_120d > 0 THEN game_uid ELSE NULL END) AS puser_count_120d,
    MAX(revenue_120d) AS max_one_user_revenue_120d,

    SUM(revenue_135d) AS revenue_135d,
    COUNT(DISTINCT CASE WHEN revenue_135d > 0 THEN game_uid ELSE NULL END) AS puser_count_135d,
    MAX(revenue_135d) AS max_one_user_revenue_135d

FROM
(
    SELECT
        install_date,
        mediasource,
        country_group,
        ad_type,
        game_uid,

        SUM(revenue_1d) AS revenue_1d,
        SUM(revenue_7d) AS revenue_7d,
        SUM(revenue_120d) AS revenue_120d,
        SUM(revenue_135d) AS revenue_135d
    FROM
    (
        SELECT
            a.install_day AS install_date,
            a.mediasource,
            a.country,
            a.game_uid,
            CASE
                WHEN a.country IN ('AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','MO','IL') THEN 'T1'
                WHEN a.country IN ('BG','BV','BY','ES','GR','HU','ID','KZ','LT','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA') THEN 'T2'
                WHEN a.country IN ('AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','SM','SR','UA','UY','XK') THEN 'T3'
                WHEN a.country = 'US' THEN 'US'
                WHEN a.country = 'JP' THEN 'JP'
                WHEN a.country = 'KR' THEN 'KR'
                WHEN a.country = 'TW' THEN 'TW'
                WHEN a.country IN ('SA','AE','QA','KW','BH','OM') THEN 'GCC'
                ELSE 'T3'
            END AS country_group,

            CASE
                WHEN a.mediasource = 'Facebook Ads' THEN CASE
                    WHEN b.campaign_name LIKE '%BAU%' THEN 'BAU'
                    WHEN b.campaign_name LIKE '%AAA%' OR b.campaign_name LIKE '%3A%' THEN 'AAA'
                    ELSE 'other'
                END
                WHEN a.mediasource = 'googleadwords_int' THEN CASE
                    WHEN b.campaign_name LIKE '%3.0%' THEN '3.0'
                    WHEN b.campaign_name LIKE '%2.5%' THEN '2.5'
                    WHEN b.campaign_name LIKE '%1.0%' THEN '1.0'
                    WHEN LOWER(b.campaign_name) LIKE '%smart%' THEN 'smart'
                    ELSE 'other'
                END
                ELSE 'other'
            END AS ad_type,

            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 0 THEN revenue_value_usd ELSE 0 END) AS revenue_1d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 6 THEN revenue_value_usd ELSE 0 END) AS revenue_7d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 119 THEN revenue_value_usd ELSE 0 END) AS revenue_120d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 134 THEN revenue_value_usd ELSE 0 END) AS revenue_135d

        FROM rg_bi.dwd_overseas_revenue_allproject a
        LEFT JOIN (
            SELECT mediasource, campaign_id, campaign_name
            FROM dwb_overseas_mediasource_campaign_map
            GROUP BY mediasource, campaign_id, campaign_name
        ) b
        ON a.mediasource = b.mediasource AND a.campaign_id = b.campaign_id

        WHERE
            a.zone = '0'
            AND a.app = '502'
            AND a.app_package = 'com.fun.lastwar.gp'
            AND a.day BETWEEN '{startDayStr}' AND '{endDayStr135}'
            AND a.install_day BETWEEN '{startDayStr}' AND '{endDayStr}'
            AND a.game_uid IS NOT NULL

        GROUP BY
            a.install_day,
            a.mediasource,
            a.country,
            a.game_uid,
            b.campaign_name
    ) user_level_revenue

    GROUP BY
        install_date,
        mediasource,
        country_group,
        ad_type,
        game_uid
) grouped_user_revenue

GROUP BY
    install_date,
    mediasource,
    country_group,
    ad_type
;
        '''
        print(f"Executing SQL: {sql}")
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df

def getRevenueData1Percentile(startDayStr, endDayStr,percentile=0.99):
    filename = f'/src/data/lw_revenue_1_percentile_{startDayStr}_{endDayStr}_{percentile}.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
        endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')
        # 由于需要135天的数据，所以endDay需要加上135天，保守起见，加140天
        endDay += datetime.timedelta(days=140)
        endDayStr135 = endDay.strftime('%Y%m%d')

        percentileStr = str(percentile).replace('.', '_')
        sql = f'''
SELECT
    mediasource,
    country_group,
    ad_type,

    percentile_approx(revenue_1d, {percentile}) AS revenue_1d_{percentileStr},
    percentile_approx(revenue_7d, {percentile}) AS revenue_7d_{percentileStr},
    percentile_approx(revenue_120d, {percentile}) AS revenue_120d_{percentileStr},
    percentile_approx(revenue_135d, {percentile}) AS revenue_135d_{percentileStr}

FROM
(
    SELECT
        install_date,
        mediasource,
        country_group,
        ad_type,
        game_uid,

        SUM(revenue_1d) AS revenue_1d,
        SUM(revenue_7d) AS revenue_7d,
        SUM(revenue_120d) AS revenue_120d,
        SUM(revenue_135d) AS revenue_135d
    FROM
    (
        SELECT
            a.install_day AS install_date,
            a.mediasource,
            a.country,
            a.game_uid,
            CASE
                WHEN a.country IN ('AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','MO','IL') THEN 'T1'
                WHEN a.country IN ('BG','BV','BY','ES','GR','HU','ID','KZ','LT','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA') THEN 'T2'
                WHEN a.country IN ('AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','SM','SR','UA','UY','XK') THEN 'T3'
                WHEN a.country = 'US' THEN 'US'
                WHEN a.country = 'JP' THEN 'JP'
                WHEN a.country = 'KR' THEN 'KR'
                WHEN a.country = 'TW' THEN 'TW'
                WHEN a.country IN ('SA','AE','QA','KW','BH','OM') THEN 'GCC'
                ELSE 'T3'
            END AS country_group,

            CASE
                WHEN a.mediasource = 'Facebook Ads' THEN CASE
                    WHEN b.campaign_name LIKE '%BAU%' THEN 'BAU'
                    WHEN b.campaign_name LIKE '%AAA%' OR b.campaign_name LIKE '%3A%' THEN 'AAA'
                    ELSE 'other'
                END
                WHEN a.mediasource = 'googleadwords_int' THEN CASE
                    WHEN b.campaign_name LIKE '%3.0%' THEN '3.0'
                    WHEN b.campaign_name LIKE '%2.5%' THEN '2.5'
                    WHEN b.campaign_name LIKE '%1.0%' THEN '1.0'
                    WHEN LOWER(b.campaign_name) LIKE '%smart%' THEN 'smart'
                    ELSE 'other'
                END
                ELSE 'other'
            END AS ad_type,

            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 0 THEN revenue_value_usd ELSE 0 END) AS revenue_1d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 6 THEN revenue_value_usd ELSE 0 END) AS revenue_7d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 119 THEN revenue_value_usd ELSE 0 END) AS revenue_120d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 134 THEN revenue_value_usd ELSE 0 END) AS revenue_135d

        FROM rg_bi.dwd_overseas_revenue_allproject a
        LEFT JOIN (
            SELECT mediasource, campaign_id, campaign_name
            FROM dwb_overseas_mediasource_campaign_map
            GROUP BY mediasource, campaign_id, campaign_name
        ) b
        ON a.mediasource = b.mediasource AND a.campaign_id = b.campaign_id

        WHERE
            a.zone = '0'
            AND a.app = '502'
            AND a.app_package = 'com.fun.lastwar.gp'
            AND a.day BETWEEN '{startDayStr}' AND '{endDayStr135}'
            AND a.install_day BETWEEN '{startDayStr}' AND '{endDayStr}'
            AND a.game_uid IS NOT NULL

        GROUP BY
            a.install_day,
            a.mediasource,
            a.country,
            a.game_uid,
            b.campaign_name
    ) user_level_revenue

    GROUP BY
        install_date,
        mediasource,
        country_group,
        ad_type,
        game_uid
) grouped_user_revenue

GROUP BY
    mediasource,
    country_group,
    ad_type
;
        '''
        print(f"Executing SQL: {sql}")
        df = execSql(sql)
        df.to_csv(filename, index=False)
    return df

def getRevenueData2Nerf(startDayStr, endDayStr, mediasourceList = [],percentile=0.99):
    filename = f'/src/data/lw_revenue_2_nerf_{startDayStr}_{endDayStr}_{percentile}.csv'
    if os.path.exists(filename):
        return pd.read_csv(filename, dtype={'install_date': str})
    else:
        # Step1: 获取分位数阈值
        percentile_df = getRevenueData1Percentile(startDayStr, endDayStr, percentile)
        if len(mediasourceList) == 0:
            # mediasourceList = percentile_df['mediasource'].unique().tolist()
            mediasourceList = ['Facebook Ads', 'googleadwords_int']
        percentile_df = percentile_df[percentile_df['mediasource'].isin(mediasourceList)].copy()
    
        # percentile_df = percentile_df[percentile_df['country_group'].isin([
        #     'T1', 
        #     # 'T2', 'T3', 'US', 'JP', 'KR', 'TW', 'GCC'
        #     ])].copy()

        # Step2: 将阈值拼接到SQL中，构造CASE WHEN语句
        def generate_case_when(field, percentile_field):
            case_when = f'''
            CASE
            '''
            for _, row in percentile_df.iterrows():
                mediasource, country_group, ad_type, threshold = row['mediasource'], row['country_group'], row['ad_type'], row[percentile_field]
                # 防止阈值为null
                if pd.isnull(threshold):
                    continue
                case_when += f'''
                WHEN mediasource='{mediasource}' AND country_group='{country_group}' AND ad_type='{ad_type}' AND {field} > {threshold} THEN {threshold}
                '''
            case_when += f'''
                ELSE {field}
            END
            '''
            return case_when

        revenue_1d_case = generate_case_when('revenue_1d', f'revenue_1d_{str(percentile).replace(".","_")}')
        revenue_7d_case = generate_case_when('revenue_7d', f'revenue_7d_{str(percentile).replace(".","_")}')
        revenue_120d_case = generate_case_when('revenue_120d', f'revenue_120d_{str(percentile).replace(".","_")}')
        revenue_135d_case = generate_case_when('revenue_135d', f'revenue_135d_{str(percentile).replace(".","_")}')

        startDay = datetime.datetime.strptime(startDayStr, '%Y%m%d')
        endDay = datetime.datetime.strptime(endDayStr, '%Y%m%d')
        # 由于需要135天的数据，所以endDay需要加上135天，保守起见，加140天
        endDay += datetime.timedelta(days=140)
        endDayStr135 = endDay.strftime('%Y%m%d')

        # Step3: 拼接完整SQL
        sql = f'''
SELECT
    install_date,
    mediasource,
    country_group,
    ad_type,

    SUM(revenue_1d) AS revenue_1d_nerf,
    SUM(revenue_7d) AS revenue_7d_nerf,
    SUM(revenue_120d) AS revenue_120d_nerf,
    SUM(revenue_135d) AS revenue_135d_nerf
FROM
(
    SELECT
        install_date,
        mediasource,
        country_group,
        ad_type,
        game_uid,

        SUM({revenue_1d_case}) AS revenue_1d,
        SUM({revenue_7d_case}) AS revenue_7d,
        SUM({revenue_120d_case}) AS revenue_120d,
        SUM({revenue_135d_case}) AS revenue_135d
    FROM
    (
        SELECT
            a.install_day AS install_date,
            a.mediasource,
            a.country,
            a.game_uid,
            CASE
                WHEN a.country IN ('AD','AT','AU','BE','CA','CH','DE','DK','FI','FR','HK','IE','IS','IT','LI','LU','MC','NL','NO','NZ','SE','SG','UK','MO','IL') THEN 'T1'
                WHEN a.country IN ('BG','BV','BY','ES','GR','HU','ID','KZ','LT','MA','MY','PH','PL','PT','RO','RS','SI','SK','TH','TM','TR','UZ','ZA') THEN 'T2'
                WHEN a.country IN ('AL','AR','BA','BO','BR','CL','CO','CR','CZ','DZ','EC','EE','EG','FO','GG','GI','GL','GT','HR','IM','IN','IQ','JE','LV','MD','ME','MK','MT','MX','PA','PE','PY','SM','SR','UA','UY','XK') THEN 'T3'
                WHEN a.country = 'US' THEN 'US'
                WHEN a.country = 'JP' THEN 'JP'
                WHEN a.country = 'KR' THEN 'KR'
                WHEN a.country = 'TW' THEN 'TW'
                WHEN a.country IN ('SA','AE','QA','KW','BH','OM') THEN 'GCC'
                ELSE 'T3'
            END AS country_group,

            CASE
                WHEN a.mediasource = 'Facebook Ads' THEN CASE
                    WHEN b.campaign_name LIKE '%BAU%' THEN 'BAU'
                    WHEN b.campaign_name LIKE '%AAA%' OR b.campaign_name LIKE '%3A%' THEN 'AAA'
                    ELSE 'other'
                END
                WHEN a.mediasource = 'googleadwords_int' THEN CASE
                    WHEN b.campaign_name LIKE '%3.0%' THEN '3.0'
                    WHEN b.campaign_name LIKE '%2.5%' THEN '2.5'
                    WHEN b.campaign_name LIKE '%1.0%' THEN '1.0'
                    WHEN LOWER(b.campaign_name) LIKE '%smart%' THEN 'smart'
                    ELSE 'other'
                END
                ELSE 'other'
            END AS ad_type,

            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 0 THEN revenue_value_usd ELSE 0 END) AS revenue_1d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 6 THEN revenue_value_usd ELSE 0 END) AS revenue_7d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 119 THEN revenue_value_usd ELSE 0 END) AS revenue_120d,
            SUM(CASE WHEN datediff(to_date(event_day,'yyyymmdd'), to_date(install_day,'yyyymmdd'),'dd') <= 134 THEN revenue_value_usd ELSE 0 END) AS revenue_135d

        FROM rg_bi.dwd_overseas_revenue_allproject a
        LEFT JOIN (
            SELECT mediasource, campaign_id, campaign_name
            FROM dwb_overseas_mediasource_campaign_map
            GROUP BY mediasource, campaign_id, campaign_name
        ) b
        ON a.mediasource = b.mediasource AND a.campaign_id = b.campaign_id

        WHERE
            a.zone = '0'
            AND a.app = '502'
            AND a.app_package = 'com.fun.lastwar.gp'
            AND a.day BETWEEN '{startDayStr}' AND '{endDayStr135}'
            AND a.install_day BETWEEN '{startDayStr}' AND '{endDayStr}'
            AND a.game_uid IS NOT NULL

        GROUP BY
            a.install_day,
            a.mediasource,
            a.country,
            a.game_uid,
            b.campaign_name
    ) user_level_revenue

    GROUP BY
        install_date,
        mediasource,
        country_group,
        ad_type,
        game_uid
) grouped_user_revenue

GROUP BY
    install_date,
    mediasource,
    country_group,
    ad_type
;
        '''
        print(f"Executing SQL: {sql}")
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df


def main():
    startDayStr, endDayStr = '20240101', '20250228'

    df = getRevenueData0Raw(startDayStr, endDayStr)
    # 简单处理
    df = df[df['install_date'] >= startDayStr]
    mediaDf = df.groupby(['mediasource']).agg({'revenue_7d':'sum'}).reset_index()
    mediaDf = mediaDf.sort_values(by='revenue_7d', ascending=False)
    mediasourceList = mediaDf[mediaDf['revenue_7d'] > 10000]['mediasource'].tolist()
    df = df[df['mediasource'].isin(mediasourceList)]
    df = df[[
        'install_date', 'mediasource', 'country_group', 'ad_type',
        'revenue_1d', 'revenue_7d', 'revenue_120d', 'revenue_135d']].copy()


    percentileList = [0.99,0.995,0.999]
    for percentile in percentileList:
        nerfDf = getRevenueData2Nerf(startDayStr, endDayStr, mediasourceList, percentile)
        nerfDf = df.merge(nerfDf, on=['install_date', 'mediasource', 'country_group', 'ad_type'], how='left')
        
        # 按天统计
        nerfDayDf = nerfDf.copy()
        # 过滤，排除r7小于等于0的记录，排除r7或者r120为空
        nerfDayDf = nerfDayDf[(nerfDayDf['revenue_7d'] > 0) &
                                (nerfDayDf['revenue_120d'] > 0) &
                                (nerfDayDf['revenue_7d_nerf'] > 0) &
                                (nerfDayDf['revenue_120d_nerf'] > 0)].copy()
        nerfDayDf['r120/r7'] = nerfDayDf['revenue_120d'] / nerfDayDf['revenue_7d']
        nerfDayDf['r120/r7_mean'] = nerfDayDf.groupby(['mediasource', 'country_group', 'ad_type'])['r120/r7'].transform('mean')
        nerfDayDf['mape_r120/r7'] = abs(nerfDayDf['r120/r7'] - nerfDayDf['r120/r7_mean']) / nerfDayDf['r120/r7_mean']
        
        nerfDayDf['nerf_r120/r7'] = nerfDayDf['revenue_120d_nerf'] / nerfDayDf['revenue_7d_nerf']
        nerfDayDf['nerf_r120/r7_mean'] = nerfDayDf.groupby(['mediasource', 'country_group', 'ad_type'])['nerf_r120/r7'].transform('mean')
        nerfDayDf['mape_nerf_r120/r7'] = abs(nerfDayDf['nerf_r120/r7'] - nerfDayDf['nerf_r120/r7_mean']) / nerfDayDf['nerf_r120/r7_mean']
        nerfDayDf.to_csv(f'/src/data/lw_revenue_day_{startDayStr}_{endDayStr}_{percentile}.csv', index=False)

        # 保存结果，记录所有分组的Mape均值
        nerfDayDfResult = nerfDayDf.groupby(['mediasource', 'country_group', 'ad_type']).agg({
            'r120/r7_mean': 'mean',
            'nerf_r120/r7_mean': 'mean',
            'mape_r120/r7': 'mean',
            'mape_nerf_r120/r7': 'mean',
            'revenue_7d': 'sum',
            'revenue_120d': 'sum',
            'revenue_7d_nerf': 'sum',
            'revenue_120d_nerf': 'sum'
        }).reset_index()
        nerfDayDfResult['nerf_r7'] = 1 - nerfDayDfResult['revenue_7d_nerf'] / nerfDayDfResult['revenue_7d']
        nerfDayDfResult['nerf_r120'] = 1 - nerfDayDfResult['revenue_120d_nerf'] / nerfDayDfResult['revenue_120d']

        nerfDayDfResult = nerfDayDfResult[[
            'mediasource', 'country_group', 'ad_type',
            'mape_r120/r7', 'mape_nerf_r120/r7',
            'nerf_r7', 'nerf_r120'
            ]]
        nerfDayDfResult.to_csv(f'/src/data/lw_revenue_day_result_{startDayStr}_{endDayStr}_{percentile}.csv', index=False)

        # 按周统计
        nerfWeekDf = nerfDf.copy()
        # install_date 转成 datetime 类型
        nerfWeekDf['install_date'] = pd.to_datetime(nerfWeekDf['install_date'], format='%Y%m%d')
        nerfWeekDf['install_week'] = nerfWeekDf['install_date'].dt.to_period('W').astype(str)
        nerfWeekDf = nerfWeekDf.groupby(['install_week', 'mediasource', 'country_group', 'ad_type']).sum().reset_index()

        # 过滤，排除r7小于等于0的记录，排除r7或者r120为空
        nerfWeekDf = nerfWeekDf[(nerfWeekDf['revenue_7d'] > 0) &
                                (nerfWeekDf['revenue_120d'] > 0) &
                                (nerfWeekDf['revenue_7d_nerf'] > 0) &
                                (nerfWeekDf['revenue_120d_nerf'] > 0)].copy()
        nerfWeekDf['r120/r7'] = nerfWeekDf['revenue_120d'] / nerfWeekDf['revenue_7d']
        nerfWeekDf['r120/r7_mean'] = nerfWeekDf.groupby(['mediasource', 'country_group', 'ad_type'])['r120/r7'].transform('mean')
        nerfWeekDf['mape_r120/r7'] = abs(nerfWeekDf['r120/r7'] - nerfWeekDf['r120/r7_mean']) / nerfWeekDf['r120/r7_mean']
        nerfWeekDf['nerf_r120/r7'] = nerfWeekDf['revenue_120d_nerf'] / nerfWeekDf['revenue_7d_nerf']
        nerfWeekDf['nerf_r120/r7_mean'] = nerfWeekDf.groupby(['mediasource', 'country_group', 'ad_type'])['nerf_r120/r7'].transform('mean')
        nerfWeekDf['mape_nerf_r120/r7'] = abs(nerfWeekDf['nerf_r120/r7'] - nerfWeekDf['nerf_r120/r7_mean']) / nerfWeekDf['nerf_r120/r7_mean']
        nerfWeekDf.to_csv(f'/src/data/lw_revenue_week_{startDayStr}_{endDayStr}_{percentile}.csv', index=False)
        # 保存结果，记录所有分组的Mape均值
        nerfWeekDfResult = nerfWeekDf.groupby(['mediasource', 'country_group', 'ad_type']).agg({
            'r120/r7_mean': 'mean',
            'nerf_r120/r7_mean': 'mean',
            'mape_r120/r7': 'mean',
            'mape_nerf_r120/r7': 'mean',
            'revenue_7d': 'sum',
            'revenue_120d': 'sum',
            'revenue_7d_nerf': 'sum',
            'revenue_120d_nerf': 'sum'
        }).reset_index()
        nerfWeekDfResult['nerf_r7'] = 1 - nerfWeekDfResult['revenue_7d_nerf'] / nerfWeekDfResult['revenue_7d']
        nerfWeekDfResult['nerf_r120'] = 1 - nerfWeekDfResult['revenue_120d_nerf'] / nerfWeekDfResult['revenue_120d']
        nerfWeekDfResult = nerfWeekDfResult[[
            'mediasource', 'country_group', 'ad_type',
            'mape_r120/r7', 'mape_nerf_r120/r7',
            'nerf_r7', 'nerf_r120'
            ]]
        nerfWeekDfResult.to_csv(f'/src/data/lw_revenue_week_result_{startDayStr}_{endDayStr}_{percentile}.csv', index=False)

        # 按月统计
        nerfMonthDf = nerfDf.copy()
        # install_date 转成 datetime 类型
        nerfMonthDf['install_date'] = pd.to_datetime(nerfMonthDf['install_date'], format='%Y%m%d')
        nerfMonthDf['install_month'] = nerfMonthDf['install_date'].dt.to_period('M').astype(str)
        nerfMonthDf = nerfMonthDf.groupby(['install_month', 'mediasource', 'country_group', 'ad_type']).sum().reset_index()
        # 过滤，排除r7小于等于0的记录，排除r7或者r120为空
        nerfMonthDf = nerfMonthDf[
            (nerfMonthDf['revenue_7d'] > 0) &
            (nerfMonthDf['revenue_120d'] > 0) &
            (nerfMonthDf['revenue_7d_nerf'] > 0) &
            (nerfMonthDf['revenue_120d_nerf'] > 0)].copy()
        nerfMonthDf['r120/r7'] = nerfMonthDf['revenue_120d'] / nerfMonthDf['revenue_7d']
        nerfMonthDf['r120/r7_mean'] = nerfMonthDf.groupby(['mediasource', 'country_group', 'ad_type'])['r120/r7'].transform('mean')
        nerfMonthDf['mape_r120/r7'] = abs(nerfMonthDf['r120/r7'] - nerfMonthDf['r120/r7_mean']) / nerfMonthDf['r120/r7_mean']
        nerfMonthDf['nerf_r120/r7'] = nerfMonthDf['revenue_120d_nerf'] / nerfMonthDf['revenue_7d_nerf']
        nerfMonthDf['nerf_r120/r7_mean'] = nerfMonthDf.groupby(['mediasource', 'country_group', 'ad_type'])['nerf_r120/r7'].transform('mean')
        nerfMonthDf['mape_nerf_r120/r7'] = abs(nerfMonthDf['nerf_r120/r7'] - nerfMonthDf['nerf_r120/r7_mean']) / nerfMonthDf['nerf_r120/r7_mean']
        nerfMonthDf.to_csv(f'/src/data/lw_revenue_month_{startDayStr}_{endDayStr}_{percentile}.csv', index=False)
        # 保存结果，记录所有分组的Mape均值
        nerfMonthDfResult = nerfMonthDf.groupby(['mediasource', 'country_group', 'ad_type']).agg({
            'r120/r7_mean': 'mean',
            'nerf_r120/r7_mean': 'mean',
            'mape_r120/r7': 'mean',
            'mape_nerf_r120/r7': 'mean',
            'revenue_7d': 'sum',
            'revenue_120d': 'sum',
            'revenue_7d_nerf': 'sum',
            'revenue_120d_nerf': 'sum'
        }).reset_index()
        nerfMonthDfResult['nerf_r7'] = 1 - nerfMonthDfResult['revenue_7d_nerf'] / nerfMonthDfResult['revenue_7d']
        nerfMonthDfResult['nerf_r120'] = 1 - nerfMonthDfResult['revenue_120d_nerf'] / nerfMonthDfResult['revenue_120d']
        nerfMonthDfResult = nerfMonthDfResult[[
            'mediasource', 'country_group', 'ad_type',
            'mape_r120/r7', 'mape_nerf_r120/r7',
            'nerf_r7', 'nerf_r120'
            ]]
        nerfMonthDfResult.to_csv(f'/src/data/lw_revenue_month_result_{startDayStr}_{endDayStr}_{percentile}.csv', index=False)

if __name__ == "__main__":
    # step1()
    # step2()
    # step2_f()
    # step3()
    # step3_f()
    # step4()
    # step4_f()
    # getRevenueDataNerf('20240101', '20250201')
    # getRevenueData0Raw('20240101', '20250201')
    # getRevenueData1Percentile('20240101', '20250201', 0.99)
    # getRevenueData2Nerf('20240101', '20250201', 0.99)
    main()
    print("Script executed successfully.")
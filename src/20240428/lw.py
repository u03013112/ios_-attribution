# lastwar iOS
# 各媒体的花费，与大盘收入，他们之前的相关系数

import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getLwAdCost(startDayStr,endDayStr,appPackage='id6448786147'):
    filename = f'/src/data/lwAdCost{appPackage}_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
select
    install_day,
    mediasource,
    sum(
    cost_value_usd
    ) as cost
from 
    dwd_overseas_cost_allproject
where
    app = '502'
    AND app_package = '{appPackage}'
    AND cost_value_usd > 0
    AND facebook_segment in ('country', 'N/A')
    and install_day between '{startDayStr}' and '{endDayStr}'
group by
    install_day,
    mediasource
;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

def getTwAdCost(startDayStr,endDayStr,appPackage='id1479198816'):
    filename = f'/src/data/twAdCost{appPackage}_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
SELECT
	install_day,
	mediasource,
	sum(cost_value_usd) as cost
FROM
	dwd_overseas_cost_new
WHERE
	app = '102'
	AND zone = '0'
	AND app_package = '{appPackage}'
	AND window_cycle = 9999
	AND facebook_segment in ('country', 'N/A')
	AND install_day between {startDayStr} and {endDayStr}
GROUP BY
	install_day,
	mediasource
;
		'''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

# com.fun.lastwar.gp
def getLwRevenue(startDayStr,endDayStr,appPackage='id6448786147'):
    filename = f'/src/data/lwRevenue{appPackage}_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
SELECT
	COALESCE(
		SUM(
			CASE
				WHEN event_time <= install_timestamp + 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r1usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time <= install_timestamp + 3 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r3usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time <= install_timestamp + 7 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r7usd,
	install_day
FROM
	dwd_overseas_revenue_allproject
WHERE
	app = '502'
	and zone = 0
	and app_package = '{appPackage}'
	and install_day between {startDayStr} and {endDayStr}
GROUP BY
	install_day;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df

# com.topwar.gp
def getTwRevenue(startDayStr,endDayStr,appPackage='id1479198816'):
    filename = f'/src/data/twRevenue{appPackage}_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename):
        sql = f'''
SELECT
	COALESCE(
		SUM(
			CASE
				WHEN event_time <= install_timestamp + 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r1usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time <= install_timestamp + 3 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r3usd,
	COALESCE(
		SUM(
			CASE
				WHEN event_time <= install_timestamp + 7 * 24 * 3600 THEN revenue_value_usd
				ELSE 0
			END
		),
		0
	) as r7usd,
	install_day
FROM
	dwd_overseas_revenue_afattribution_realtime
WHERE
	app = '102'
	and zone = 0
	and window_cycle = 9999
	and app_package = '{appPackage}'
	and install_day between {startDayStr} and {endDayStr}
GROUP BY
	install_day;
        '''
        print(sql)
        df = execSql(sql)
        df.to_csv(filename, index=False)
    else:
        print('read from file:',filename)
        df = pd.read_csv(filename, dtype={'install_day':str})
    return df


def main(adCostDf,revenueDf):
    # 过滤 由于需要计算24小时内的数据，所以需要保证数据的完整性，所以安装时间要在endDayStr往前推7天
    # install_day >= startDayStr and install_day <= endDayStr - 1day
    endInstallDayStr = (pd.to_datetime(endDayStr) - pd.Timedelta(days=7)).strftime('%Y%m%d')
    adCostDf = adCostDf[(adCostDf['install_day'] >= startDayStr) & (adCostDf['install_day'] <= endInstallDayStr)]
    revenueDf = revenueDf[(revenueDf['install_day'] >= startDayStr) & (revenueDf['install_day'] <= endInstallDayStr)]

    
    adCostDf2 = adCostDf.groupby('mediasource').sum()
    adCostDf2['cost'] = adCostDf2['cost'].map(lambda x: round(x, 2))
    adCostDf2.to_csv(f'/src/data/lwAdCostSum_{startDayStr}_{endDayStr}.csv', index=True)
    
    adCostSumDf = adCostDf.groupby('install_day').agg({'cost':'sum'}).reset_index()
    # 将数据透视以获得所需格式
    adCostDf = adCostDf.pivot(index='install_day', columns='mediasource', values='cost').reset_index()

    # 合并数据
    df = pd.merge(adCostDf, revenueDf, how='outer', on='install_day')
    df = df.merge(adCostSumDf, how='outer', on='install_day')

    df['facebook cost rate'] = df['Facebook Ads'] / df['cost']
    df['applovin cost rate'] = df['applovin_int'] / df['cost']
    df['googleadwords cost rate'] = df['googleadwords_int'] / df['cost']

    print('facebook cost rate:',df['Facebook Ads'].sum()/df['cost'].sum())
    print('applovin cost rate:',df['applovin_int'].sum()/df['cost'].sum())
    print('googleadwords cost rate:',df['googleadwords_int'].sum()/df['cost'].sum())
    
    corr = df.corr()['r1usd']
    print(f'共计{len(df)}天')
    print('与24小时收入线性相关系数：')
    selected_corr = corr.loc[['Facebook Ads', 'applovin_int', 'googleadwords_int']]
    print(selected_corr)

    N = 10
    
    print(f'google > {N}%')
    df1 = df[df['googleadwords cost rate'] > (N/100)]
    print(f'共计{len(df1)}天')
    print('与24小时收入线性相关系数：')
    corr = df1.corr()['r1usd']
    selected_corr = corr.loc[['Facebook Ads', 'applovin_int', 'googleadwords_int']]
    print(selected_corr)

    print(f'applovin > {N}%')
    df2 = df[df['applovin cost rate'] > (N/100)]
    print(f'共计{len(df2)}天')
    corr = df2.corr()['r1usd']
    selected_corr = corr.loc[['Facebook Ads', 'applovin_int', 'googleadwords_int']]
    print(selected_corr)

    return

    df.corr().to_csv(f'/src/data/lwCorr_{startDayStr}_{endDayStr}.csv', index=True)
    # print(df.corr())

    df['install_day'] = pd.to_datetime(df['install_day'])
    # 画图，用install_day做x轴，y轴是各媒体的花费，与大盘收入

    # 设置图像大小
    plt.figure(figsize=(12, 6))

    # 绘制每个媒体来源的广告成本折线图
    for column in df.columns:
        if column != 'install_day' and column != 'r1usd' and column != 'r3usd' and column != 'r7usd':
            plt.plot(df['install_day'], df[column], label=column)

    # 绘制总收入折线图
    plt.plot(df['install_day'], df['r1usd'], label='Total Revenue', linewidth=2, linestyle='--')

    # 设置图例、标题和坐标轴标签
    plt.legend()
    plt.title('Ad Cost and Total Revenue Over Time')
    plt.xlabel('Install Day')
    plt.ylabel('Amount')

    # 保存图像
    plt.savefig(f'/src/data/lwAdCostRevenue1_{startDayStr}_{endDayStr}.png')
    plt.clf()

    # 由于各媒体的花费差异较大，为了可以反应出总体的趋势，需要将花费数据进行归一化处理
    df = df.set_index('install_day')
    df = (df - df.min()) / (df.max() - df.min())
    df = df.reset_index()

    # 重新绘制图像
    for column in df.columns:
        if column != 'install_day' and column != 'r1usd' and column != 'r3usd' and column != 'r7usd':
            if column == 'Facebook Ads' or column == 'googleadwords_int' or column == 'applovin_int' or column == 'bytedanceglobal_int':
                plt.plot(df['install_day'], df[column], label=column)

                plt.plot(df['install_day'], df['r1usd'], label='Total Revenue', linewidth=2)

                plt.legend()
                plt.title('Normalized Ad Cost and Total Revenue Over Time')
                plt.xlabel('Install Day')
                plt.ylabel('Amount')

                plt.savefig(f'/src/data/lwAdCostRevenue2_{startDayStr}_{endDayStr}_{column}.png')
                plt.clf()
    

    for column in df.columns:
        if column != 'install_day' and column != 'r1usd' and column != 'r3usd' and column != 'r7usd':
            if column == 'Facebook Ads' or column == 'googleadwords_int' or column == 'applovin_int' or column == 'bytedanceglobal_int':
                # 对数据按照当前列进行升序排列
                sorted_df = df.sort_values(by=column)
                
                # 使用整数序列作为x轴
                x_values = range(len(sorted_df))
                
                plt.plot(x_values, sorted_df[column], label=column)
                plt.plot(x_values, sorted_df['r1usd'], label='Total Revenue', linewidth=2)

                plt.legend()
                plt.title('Normalized Ad Cost and Total Revenue Over Time')
                plt.xlabel('Sorted Index')
                plt.ylabel('Amount')

                plt.savefig(f'/src/data/lwAdCostRevenue3_{startDayStr}_{endDayStr}_{column}.png')
                plt.clf()

if __name__ == '__main__':
    startDayStr = '20240101'
    endDayStr = '20240430'

    print('lastwar iOS')
    adCostDf = getLwAdCost(startDayStr,endDayStr)
    revenueDf = getLwRevenue(startDayStr,endDayStr)
    main(adCostDf,revenueDf)

    print('lastwar Android')
    adCostDf = getLwAdCost(startDayStr,endDayStr,appPackage='com.fun.lastwar.gp')
    revenueDf = getLwRevenue(startDayStr,endDayStr,appPackage='com.fun.lastwar.gp')
    main(adCostDf,revenueDf)

    print('topwar iOS')
    adCostDf = getTwAdCost(startDayStr,endDayStr)
    revenueDf = getTwRevenue(startDayStr,endDayStr)
    main(adCostDf,revenueDf)

    print('topwar Android')
    adCostDf = getTwAdCost(startDayStr,endDayStr,appPackage='com.topwar.gp')
    revenueDf = getTwRevenue(startDayStr,endDayStr,appPackage='com.topwar.gp')
    main(adCostDf,revenueDf)
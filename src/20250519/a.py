import os
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getCostData():
    filename = '/src/data/lw_20250519_costdata.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = """
select
	install_day,
	mediasource,
	country,
	sum(usd) as usd
from
	(
		select
			install_day,
			mediasource,
			COALESCE(cdm_laswwar_country_map.countrygroup, 'OTHER') AS country,
			sum(cost_value_usd) as usd
		from
			dws_overseas_public_roi
			left join cdm_laswwar_country_map on dws_overseas_public_roi.country = cdm_laswwar_country_map.country
		where
			app = '502'
			and facebook_segment in ('country', 'N/A')
			and app_package = 'id6448786147'
            and install_day >= '2025-01-01'
		group by
			install_day,
			mediasource,
			countrygroup
	)
group by
	install_day,
	mediasource,
	country
order by
	install_day desc;
        """
        df = execSql(sql)
        df.to_csv(filename, index=False)
    
    return df

def getRevenueData():
    filename = '/src/data/lw_20250519_revenuedata.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = """
SELECT
	install_day,
	SUM(r24h_usd) AS r24h_usd,
	country
FROM
	(SELECT
		install_day,
		COALESCE(
			SUM(
				CASE
					WHEN event_timestamp - install_timestamp between 0
					and 24 * 3600 THEN revenue_value_usd
					ELSE 0
				END
			),
			0
		) as r24h_usd,
		COALESCE(cdm_laswwar_country_map.countrygroup, 'OTHER') AS country
	FROM
		rg_bi.ads_lastwar_ios_purchase_adv
		left join cdm_laswwar_country_map on rg_bi.ads_lastwar_ios_purchase_adv.country = cdm_laswwar_country_map.country
	WHERE
		install_day >= '20250101'
	GROUP BY
		install_day,
		countrygroup
	)
GROUP BY
	install_day,
	country
;
        """
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df


def getSKARevenue():
    filename = '/src/data/lw_20250519_ska_revenue.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = """
SELECT
	install_date,
	country,
	media_source,
	SUM(revenue) AS ska_revenue
FROM
	(
		with t1 as(
			SELECT
				REPLACE(install_date, '-', '') AS install_date,
				media_source,
				SUBSTRING_INDEX(
					SUBSTRING_INDEX(ad_network_campaign_name, '_', 2),
					'_',
					-1
				) AS country,
				SUM(skad_revenue) AS revenue
			FROM
				ods_platform_appsflyer_skad_details
			WHERE
				app_id = 'id6448786147'
				AND day > 20241201
				AND event_name = 'af_purchase_update_skan_on'
			GROUP BY
				install_date,
				media_source,
				country
		)
		SELECT
			install_date,
			COALESCE(cdm_laswwar_country_map.countrygroup, 'OTHER') AS country,
			media_source,
			SUM(revenue) AS revenue
		FROM
			t1
			left join cdm_laswwar_country_map on t1.country = cdm_laswwar_country_map.country
		group by
			install_date,
			countrygroup,
			media_source
	)
GROUP BY
	install_date,
	country,
	media_source	
;
        """
        df = execSql(sql)
        df.to_csv(filename, index=False)
    return df

# 大盘，不分国家
def main():
    mediaList = [
        'googleadwords_int','Facebook Ads','applovin_int','tiktokglobal_int','other'
    ]

    costDf = getCostData()
    costDf.replace({
        'mediasource': {
            'bytedanceglobal_int':'tiktokglobal_int',
        }
    }, inplace=True)
    costDf.loc[costDf['mediasource'].isin(mediaList) == False, 'mediasource'] = 'other'
    costDf = costDf.groupby(['install_day', 'mediasource']).agg({'usd': 'sum'}).reset_index()
    costDf = costDf.sort_values(by=['install_day', 'mediasource'], ascending=[False, True])
    costDf = costDf.rename(columns={'usd': 'cost'})
    print(costDf.head(10))

    revenueDf = getRevenueData()
    revenueDf = revenueDf.groupby(['install_day']).agg({'r24h_usd': 'sum'}).reset_index()
    revenueDf = revenueDf.sort_values(by=['install_day'], ascending=[False])
    print(revenueDf.head(10))

    skaRevenueDf = getSKARevenue()
    # skaRevenueDf.replace({'media_source': {
    #     ''
    # }}, inplace=True)
    skaRevenueDf.loc[skaRevenueDf['media_source'].isin(mediaList) == False, 'media_source'] = 'other'
    skaRevenueDf = skaRevenueDf.groupby(['install_date', 'media_source']).agg({'ska_revenue': 'sum'}).reset_index()
    skaRevenueDf = skaRevenueDf.sort_values(by=['install_date', 'media_source'], ascending=[False, True])
    skaRevenueDf = skaRevenueDf.rename(columns={'install_date': 'install_day'})
    skaRevenueDf = skaRevenueDf[skaRevenueDf['install_day'] >= 20250101]
    print(skaRevenueDf.head(10))

    # 将所有数据汇总到一个表中，画图进行主观观察
    costSumDf = costDf.groupby(['install_day']).agg({'cost': 'sum'}).reset_index()
    skaRevenueSumDf = skaRevenueDf.groupby(['install_day']).agg({'ska_revenue': 'sum'}).reset_index()
    totalDf = pd.merge(skaRevenueSumDf, revenueDf, how='left', left_on='install_day', right_on='install_day')
    totalDf = pd.merge(totalDf, costSumDf, how='left', left_on='install_day', right_on='install_day')

    totalDf.to_csv('/src/data/lw_20250519_total.csv', index=False)

    totalDf = totalDf[
        (totalDf['install_day'] >= 20250106)
        & (totalDf['install_day'] <= 20250323)
    ]
    totalDf['install_day'] = pd.to_datetime(totalDf['install_day'].astype(str), format='%Y%m%d')
    totalDf['install_week'] = totalDf['install_day'].dt.strftime('%Y-%W')
    # 计算totalDf 中r24h_usd和ska_revenue的相关性
    correlation = totalDf['r24h_usd'].corr(totalDf['ska_revenue'])
    print(f"Correlation between r24h_usd and ska_revenue: {correlation}")

    totalGroupByWeekDf = totalDf.groupby(['install_week']).agg({
        'r24h_usd': 'sum',
        'ska_revenue': 'sum',
        'cost': 'sum'
    }).reset_index()
    totalGroupByWeekDf = totalGroupByWeekDf.sort_values(by=['install_week'], ascending=[False])
    totalGroupByWeekDf.to_csv('/src/data/lw_20250519_total_groupbyweek.csv', index=False)
    correlation = totalGroupByWeekDf['r24h_usd'].corr(totalGroupByWeekDf['ska_revenue'])
    print(f"Correlation between r24h_usd and ska_revenue (weekly): {correlation}")
    
def androidOrganic():
    filename = '/src/data/lw_20250519_android_organic.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
    else:
        sql = """
SELECT
    install_day,
    SUM(
        CASE
            WHEN event_time - install_timestamp BETWEEN 0
            AND 24 * 3600 THEN revenue_value_usd
            ELSE 0
        END
    ) AS all_r24h_usd,
    SUM(
        CASE
            WHEN mediasource = 'Organic'
            AND event_time - install_timestamp BETWEEN 0
            AND 24 * 3600 THEN revenue_value_usd
            ELSE 0
        END
    ) AS o_r24h_usd
FROM
    dwd_overseas_revenue_allproject
WHERE
    zone = '0'
    AND app = 502
    AND app_package = 'com.fun.lastwar.gp'
    AND day >= '20250101'
    and install_day >= '20250101'
GROUP BY
    install_day
;
        """
        df = execSql(sql)
        df.to_csv(filename, index=False)

    return df

if __name__ == "__main__":
    # main()
    androidOrganic()
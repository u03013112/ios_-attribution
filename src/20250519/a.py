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

def main():
    costDf = getCostData()
    print(costDf.head(10))

    revenueDf = getRevenueData()
    print(revenueDf.head(10))

    skaRevenueDf = getSKARevenue()
    print(skaRevenueDf.head(10))

if __name__ == "__main__":
    main()
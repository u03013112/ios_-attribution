import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

import sys
sys.path.append('/src')
from src.maxCompute import execSql


def getHistoricalData2(startDate, endDate):
    filename = f'/src/data/lw_{startDate}_{endDate}_hour.csv'
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        sql = f'''

@d1_purchase_events :=
SELECT
    install_day,
    country,
    mediasource,
    event_time,
    install_timestamp,
    revenue_value_usd
FROM dwd_overseas_revenue_allproject
WHERE
    app = 502
    AND app_package = 'com.fun.lastwar.gp'
    AND zone = 0
    AND day > '{startDate}'
    AND install_day BETWEEN '{startDate}' AND '{endDate}'
    AND DATEDIFF(FROM_UNIXTIME(event_time), FROM_UNIXTIME(CAST(install_timestamp AS BIGINT)), 'dd') = 0
;

@country_map :=
select
    d1.install_day,
    d1.country,
    d1.mediasource,
    d1.event_time,
    d1.install_timestamp,
    d1.revenue_value_usd,
    map.countrygroup as countrygroup
from @d1_purchase_events as d1 
left join cdm_laswwar_country_map as map on d1.country = map.country
;

SELECT
    install_day,
    countrygroup as country,
    mediasource,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 1 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_1,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 2 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_2,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 3 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_3,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 4 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_4,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 5 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_5,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 6 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_6,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 7 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_7,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 8 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_8,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 9 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_9,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 10 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_10,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 11 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_11,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 12 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_12,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 13 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_13,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 14 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_14,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 15 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_15,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 16 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_16,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 17 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_17,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 18 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_18,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 19 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_19,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 20 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_20,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 21 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_21,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 22 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_22,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 23 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_23,
    SUM(CASE WHEN HOUR(FROM_UNIXTIME(event_time)) < 24 THEN revenue_value_usd ELSE 0 END) AS revenue_1d_24,
    SUM(revenue_value_usd) AS revenue_1d
FROM @country_map
GROUP BY 
    install_day, countrygroup, mediasource
;
        '''
        print(sql)
        data = execSql(sql)
        data.to_csv(filename, index=False)

    return data

def getCorr():
    df = getHistoricalData2('20240401', '20241001')
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

    groupAllDf = df.groupby(['install_day']).sum().reset_index()
    print(groupAllDf)

    print(groupAllDf.corr()['revenue_1d'])

def getMediaCorr():
    df = getHistoricalData2('20240401', '20241001')
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

    groupAllDf = df.groupby(['install_day', 'mediasource']).sum().reset_index()
    print(groupAllDf)

    # mediaList = groupAllDf['mediasource'].unique()
    mediaList = ['Facebook Ads','applovin_int','googleadwords_int']
    for media in mediaList:
        print(media)
        mediaDf = groupAllDf[groupAllDf['mediasource'] == media]
        print(mediaDf.corr()['revenue_1d'])

def getCountryCorr():
    df = getHistoricalData2('20240401', '20241001')
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

    groupAllDf = df.groupby(['install_day', 'country']).sum().reset_index()
    print(groupAllDf)

    countryList = groupAllDf['country'].unique()
    for country in countryList:
        print(country)
        countryDf = groupAllDf[groupAllDf['country'] == country]
        print(countryDf.corr()['revenue_1d'])

def getMediaAndCountryCorr():
    df = getHistoricalData2('20240401', '20241001')
    df['install_day'] = pd.to_datetime(df['install_day'], format='%Y%m%d')

    groupAllDf = df.groupby(['install_day', 'mediasource', 'country']).sum().reset_index()
    print(groupAllDf)

    # mediaList = groupAllDf['mediasource'].unique()
    mediaList = ['Facebook Ads','applovin_int','googleadwords_int']
    countryList = groupAllDf['country'].unique()
    for media in mediaList:
        for country in countryList:
            print(media, country)
            mediaCountryDf = groupAllDf[(groupAllDf['mediasource'] == media) & (groupAllDf['country'] == country)]
            print(mediaCountryDf.corr()['revenue_1d'])

if __name__ == '__main__':
    # getCorr()
    # getMediaCorr()
    # getCountryCorr()
    getMediaAndCountryCorr()
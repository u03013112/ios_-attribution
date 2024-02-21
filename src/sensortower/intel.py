import requests
import pandas as pd

import sys
sys.path.append('/src')

from src.config import sensortowerToken

# 获得类别排名
def getAndroidCategoryRanking(appid,category='all',countries='US',chartTypeIds='topgrossing',startDate='2023-12-01',endDate='2023-12-31'):
    url = 'https://api.sensortower.com/v1/android/category/category_history?app_ids={}&category={}&chart_type_ids=topgrossing&countries={}&start_date={}&end_date={}&is_hourly=false&auth_token={}'.format(appid,category,countries,startDate,endDate,sensortowerToken)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getAndroidCategoryRanking failed, status_code:',r.status_code)
        return None
    
    ret = r.json()
    
    graphData = ret[appid][countries][category][chartTypeIds]['graphData']

    unixTimeList = []
    rankList = []

    for item in graphData:
        unixTime = item[0]
        rank = item[1]

        unixTimeList.append(unixTime)
        rankList.append(rank)

    df = pd.DataFrame({'unixTime':unixTimeList,'rank':rankList})
    df['date'] = pd.to_datetime(df['unixTime'],unit='s')
    df = df[['date','rank']]
    return df

# 获得商店推荐导致的下载量
def getAndroidFeaturedDownloads(appid,countries='US',startDate='2023-12-01',endDate='2023-12-31'):
    url = 'https://api.sensortower.com/v1/android/featured/impacts?app_id={}&countries={}&breakdown=country&start_date={}&end_date={}&auth_token={}'.format(appid,countries,startDate,endDate,sensortowerToken)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getAndroidFeaturedDownloads failed, status_code:',r.status_code)
        return None
    
    # 制作一个date的数组，从startDate到endDate
    dateList = []
    date = startDate
    while date <= endDate:
        dateList.append(date)
        date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    ret = r.json()

    df = pd.DataFrame(columns=['country','date','downloads'])

    retCountries = ret['countries']
    for retCountry in retCountries:
        country = retCountry['country']
        downloads = retCountry['downloads_series']
    
        countryDf = pd.DataFrame({'date':dateList,'downloads':downloads})
        countryDf['country'] = country
        countryDf = countryDf[['country','date','downloads']]
        df = df.append(countryDf,ignore_index=True)
    
    return df

# 主要的customFields
# 游戏类型 Game Genre，比如 Strategy
#  
def getCustomeFieldsFilterId(customFields):
    url = 'https://api.sensortower.com/v1/custom_fields_filter?auth_token={}'.format(sensortowerToken)
    data = {
        'custom_fields':customFields
    }
    r = requests.post(url,json=data)
    if r.status_code != 200:
        print('Error: getCustomeFieldsFilterId failed, status_code:',r.status_code)
        return None
    
    ret = r.json()
    custom_fields_filter_id = ret['custom_fields_filter_id']
    return custom_fields_filter_id

def getAndroidDownloadAndRevenue(appid,countries='',startDate='2023-12-01',endDate='2023-12-31'):
    if countries == '':
        url = 'https://api.sensortower.com/v1/android/sales_report_estimates?app_ids={}&date_granularity=monthly&start_date={}&end_date={}&auth_token={}'.format(appid,startDate,endDate,sensortowerToken)
    else:
        url = 'https://api.sensortower.com/v1/android/sales_report_estimates?app_ids={}&countries={}&date_granularity=monthly&start_date={}&end_date={}&auth_token={}'.format(appid,countries,startDate,endDate,sensortowerToken)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getAndroidDownloadAndRevenue failed, status_code:',r.status_code)
        print(r.text)
        return None
    
    # 从startDate到endDate的月份列表，格式类似于['2023-12','2024-01']
    monthList = []
    date = startDate
    while date <= endDate:
        monthList.append(date[:7])
        date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    dateList = list(set(monthList))

    ret = r.json()
    cs = []
    ds = []
    downloads = []
    revenues = []

    for r in ret:
        country = r['c']
        cs.append(country)
        date = r['d']
        ds.append(date)
        # 如果有就取数，没有就取0
        if 'u' not in r or r['u'] == None:
            download = 0
        else:
            download = r['u']
        if 'r' not in r or r['r'] == None:
            revenue = 0
        else:
            revenue = r['r']
        downloads.append(download)
        revenues.append(revenue)

    df = pd.DataFrame({'country':cs,'date':ds,'downloads':downloads,'revenues':revenues})

    # 其中date 是类似 2023-12-01T00:00:00Z 的字符串，改为 2023-12 的格式
    df['date'] = df['date'].apply(lambda x:x[:7])
    df = df.groupby(['date','country']).agg({'downloads':'sum','revenues':'sum'}).reset_index()

    return df
# 获得
def getAndroidTopApp(custom_fields_filter_id='6009d417241bc16eb8e07e9b',limit=10,category='all',countries='US',startDate='2023-12-01',endDate='2023-12-31'):
    url = 'https://api.sensortower.com/v1/android/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range=month&measure=units&category={}&date={}&end_date={}&country={}&limit={}&offset=0&custom_fields_filter_id={}&custom_tags_mode=exclude_unified_apps&auth_token={}'.format(category,startDate,endDate,countries,limit,custom_fields_filter_id,sensortowerToken)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getAndroidTopApp failed, status_code:',r.status_code)
        return None
    
    addIds = []
    downloads = []

    ret = r.json()
    for item in ret:
        appId = item['app_id']
        download = item['current_units_value']
        
        addIds.append(appId)
        downloads.append(download)

    df = pd.DataFrame({'appId':addIds,'downloads':downloads})
    return df

# 兼容ios
def getTopApp(os='android',custom_fields_filter_id='6009d417241',time_range='year',limit=10,category='all',countries='US',startDate='2023-12-01',endDate='2023-12-31'):
    urlTail = ''
    if os == 'ios':
        urlTail = '&device_type=total'
        if category == 'all':
            category = '0'

    url = 'https://api.sensortower.com/v1/{}/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range={}&measure=units&category={}&date={}&end_date={}&country={}&limit={}&custom_fields_filter_id={}&custom_tags_mode=exclude_unified_apps&auth_token={}'.format(os,time_range,category,startDate,endDate,countries,limit,custom_fields_filter_id,sensortowerToken)
    url += urlTail

    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getTopApp failed, status_code:',r.status_code)
        print(r.text)
        return None
    
    addIds = []
    downloads = []

    ret = r.json()
    for item in ret:
        appId = item['app_id']
        download = item['current_units_value']
        
        addIds.append(appId)
        downloads.append(download)

    df = pd.DataFrame({'appId':addIds,'downloads':downloads})
    return df

def getDownloadAndRevenue(appid,os='android',countries='',date_granularity='daily',startDate='2023-12-01',endDate='2023-12-31'):
    if countries == '':
        url = 'https://api.sensortower.com/v1/{}/sales_report_estimates?app_ids={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(os,appid,date_granularity,startDate,endDate,sensortowerToken)
    else:
        url = 'https://api.sensortower.com/v1/{}/sales_report_estimates?app_ids={}&countries={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(os,appid,countries,date_granularity,startDate,endDate,sensortowerToken)
    
    # print(url)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getDownloadAndRevenue failed, status_code:',r.status_code)
        print(r.text)
        return None

    ret = r.json()
    cs = []
    ds = []
    downloads = []
    revenues = []

    countryKey = 'c' if os == 'android' else 'cc'
    downloadKey = 'u' if os == 'android' else 'iu'
    revenueKey = 'r' if os == 'android' else 'ir'

    for r in ret:
        country = r[countryKey]
        cs.append(country)
        date = r['d']
        ds.append(date)
        # 如果有就取数，没有就取0
        if downloadKey not in r or r[downloadKey] == None:
            download = 0
        else:
            download = r[downloadKey]
        if revenueKey not in r or r[revenueKey] == None:
            revenue = 0
        else:
            revenue = r[revenueKey]
        downloads.append(download)
        revenues.append(revenue)

    df = pd.DataFrame({'country':cs,'date':ds,'downloads':downloads,'revenues':revenues})

    # 其中date 是类似 2023-12-01T00:00:00Z 的字符串，改为 2023-12 的格式
    # df['date'] = df['date'].apply(lambda x:x[:7])
    # df = df.groupby(['date','country']).agg({'downloads':'sum','revenues':'sum'}).reset_index()

    return df


if __name__ == '__main__':
    # print(getAndroidCategoryRanking('com.topwar.gp'))

    # print(getAndroidFeaturedDownloads('com.topwar.gp'))

    # print(getAndroidTopApp())

    # print(getCustomeFieldsFilterId(
    #     [
    #         {
    #             "global": True,
    #             "name": "Game Genre",
    #             "values": [
    #                 "Arcade"
    #             ]
    #         }
    #     ]
    # ))

    # print(getAndroidDownloadAndRevenue('com.gtarcade.ioe.global'))

    # print(getTopApp('android',custom_fields_filter_id='6009d417241bc16eb8e07e9b',limit=10,category='all',countries='US',startDate='2023-12-01',endDate='2023-12-31'))
    # print(getTopApp('ios',custom_fields_filter_id='6009d417241bc16eb8e07e9b',limit=10,countries='US',startDate='2023-12-01',endDate='2023-12-31'))

    print(getDownloadAndRevenue(appid='com.topwar.gp',os='android',countries='US',date_granularity='monthly',startDate='2023-12-01',endDate='2023-12-31'))
    # print(getDownloadAndRevenue('1479198816',os='ios',countries='US',startDate='2023-12-01',endDate='2023-12-31',date_granularity='weekly'))
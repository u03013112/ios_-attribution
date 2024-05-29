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
def getTopApp(os='android',custom_fields_filter_id='6009d417241',time_range='year',limit=10,category='all',countries='US',measure='units',startDate='2023-12-01',endDate='2023-12-31'):
    urlTail = ''
    if os == 'ios':
        urlTail = '&device_type=total'
        if category == 'all':
            category = '0'
    # https://api.sensortower.com/v1/ios/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range=week&measure=units&device_type=total&category=6000&date=2021-01-04&end_date=2021-01-10&regions=US,JP&limit=25&custom_tags_mode=include_unified_apps&auth_token=YOUR_AUTHENTICATION_TOKEN
  
    url = 'https://api.sensortower.com/v1/{}/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range={}&measure={}&category={}&date={}&end_date={}&regions={}&limit={}&custom_fields_filter_id={}&custom_tags_mode=exclude_unified_apps&auth_token={}'.format(os,time_range,measure,category,startDate,endDate,countries,limit,custom_fields_filter_id,sensortowerToken)
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
        # appId 装换为str
        if type(appId) != str:
            appId = str(appId)
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

# getDownloadAndRevenue 的第二个版本，appids是一个列表
def getDownloadAndRevenue2(appids = [],os='android',countries='',date_granularity='quarterly',startDate='2023-12-01',endDate='2023-12-31'):
    if countries == '':
        url = 'https://api.sensortower.com/v1/{}/sales_report_estimates?app_ids={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(os,','.join(appids),date_granularity,startDate,endDate,sensortowerToken)
    else:
        url = 'https://api.sensortower.com/v1/{}/sales_report_estimates?app_ids={}&countries={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(os,','.join(appids),countries,date_granularity,startDate,endDate,sensortowerToken)
    
    # print(url)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getDownloadAndRevenue failed, status_code:',r.status_code)
        print(r.text)
        return None
    # print(r.text)
    ret = r.json()
    aids = []
    cs = []
    ds = []
    downloads = []
    revenues = []

    countryKey = 'c' if os == 'android' else 'cc'
    downloadKey = 'u' if os == 'android' else 'iu'
    revenueKey = 'r' if os == 'android' else 'ir'

    for r in ret:
        aid = r['aid']
        # aid 转为str
        if type(aid) != str:
            aid = str(aid)
        aids.append(aid)
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

    df = pd.DataFrame({'app_id':aids,'country':cs,'date':ds,'downloads':downloads,'revenues':revenues})

    # 其中date 是类似 2023-12-01T00:00:00Z 的字符串，改为 2023-12 的格式
    # df['date'] = df['date'].apply(lambda x:x[:7])
    # df = df.groupby(['date','country']).agg({'downloads':'sum','revenues':'sum'}).reset_index()

    return df



def getRanking(platform='ios',category='7017',countries='KR',chartTypeIds='topfreeapplications',date='2024-02-27'):
    url = 'https://api.sensortower.com/v1/{}/ranking?category={}&chart_type={}&country={}&date={}&auth_token={}'.format(platform,category,chartTypeIds,countries,date,sensortowerToken)
    
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getRanking failed, status_code:',r.status_code)
        return None
    
    ret = r.json()
    # print(ret)
    return ret['ranking']


def getCreatives(unifiedAppIds=[],countries=[],networks='Admob,Meta Audience Network,Unity,Facebook,Instagram,TikTok,Youtube',ad_types='video',limit=10,page=1,display_breakdown=False,start_date='2024-01-01',end_date='2024-02-29',debug=False):
    # https://api.sensortower.com/v1/unified/ad_intel/creatives?app_ids=56cbbce9d48401b048003405&start_date=2023-01-01&end_date=2023-01-31&countries=US%2CCA&networks=Adcolony&ad_types=video&limit=10&page=1&display_breakdown=true&auth_token=YOUR_AUTHENTICATION_TOKEN
    url = 'https://api.sensortower.com/v1/unified/ad_intel/creatives?app_ids={}&start_date={}&end_date={}&countries={}&networks={}&ad_types={}&limit={}&page={}&display_breakdown={}&auth_token={}'.format(','.join(unifiedAppIds),start_date,end_date,','.join(countries),networks,ad_types,limit,page,display_breakdown,sensortowerToken)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getCreatives failed, status_code:',r.status_code)
        print(r.text)
        return None
    if debug:
        print(r.text)
    creatives = []
    ret = r.json()
    ad_units = ret['ad_units']
    for ad_unit in ad_units:
        creative = {
            'app_id':ad_unit['app_id'],
            'network':ad_unit['network'],
            'first_seen_at':ad_unit['first_seen_at'],
            'creative_url':ad_unit['creatives'][0]['creative_url']
        }
        creatives.append(creative)
    return creatives

# getCreatives 的第二个版本，返回的数据更多，拿到结果再做筛选
def getCreatives2(os,appIds=[],countries=[],networks='Admob,Meta Audience Network,Unity,Facebook,Instagram,TikTok,Youtube',ad_types='video',limit=10,page=1,display_breakdown=False,start_date='2024-01-01',end_date='2024-02-29'):
    url = 'https://api.sensortower.com/v1/{}/ad_intel/creatives?app_ids={}&start_date={}&end_date={}&countries={}&networks={}&ad_types={}&limit={}&page={}&display_breakdown={}&auth_token={}'.format(os,','.join(appIds),start_date,end_date,','.join(countries),networks,ad_types,limit,page,display_breakdown,sensortowerToken)
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getCreatives failed, status_code:',r.status_code)
        print(r.text)
        return None
    
    return r.json()

def getUnifiedAppIds(app_id_type='android',app_ids=[]):
    if app_id_type == 'ios':
        app_id_type = 'itunes'
    # https://api.sensortower.com/v1/unified/apps?app_id_type=android&app_ids=com.topwar.gp,com.fun.lastwar.gp&auth_token=YOUR_AUTHENTICATION_TOKEN
    url = 'https://api.sensortower.com/v1/unified/apps?app_id_type={}&app_ids={}&auth_token={}'.format(app_id_type,','.join(app_ids),sensortowerToken)
    # print(url)

    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getUnified failed, status_code:',r.status_code)
        print(r.text)
        return None
    
    ret = r.json()
    apps = ret['apps']
    
    unifiedAppIds = []
    for app_id in app_ids:
        for app in apps:
            unified_app_id = app['unified_app_id']
            name = app['name']
            if app_id_type == 'android':
                android_apps = app['android_apps']
                for android_app in android_apps:
                    if android_app['app_id'] == app_id:
                        # print(android_app)
                        unifiedAppIds.append({
                            'unified_app_id':unified_app_id,
                            'name':name,
                            'app_id':android_app
                        })
                        break
            else:
                itunes_apps = app['itunes_apps']
                for itunes_app in itunes_apps:
                    if itunes_app['app_id'] == int(app_id):
                        # print(itunes_app)
                        unifiedAppIds.append({
                            'unified_app_id':unified_app_id,
                            'name':name,
                            'app_id':itunes_app
                        })
                        break
    return unifiedAppIds

# 获得指定app的留存
def getRetention(app_ids=[],platform='ios',date_granularity='quarterly',start_date='2021-01-01',end_date='2021-04-01',country=''):
    # https://api.sensortower.com/v1/ios/usage/retention?app_ids=5cc98b703ea98357b8ed3ce0&date_granularity=quarterly&start_date=2021-01-01&end_date=2021-04-01&country=US&auth_token=YOUR_AUTHENTICATION_TOKEN
    url = 'https://api.sensortower.com/v1/{}/usage/retention?app_ids={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(platform,','.join(app_ids),date_granularity,start_date,end_date,sensortowerToken)
    if country != '':
        url += '&country='+country
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getRetention failed, status_code:',r.status_code)
        print(r.text)
        return None
    
    # print(r.text)
    ret = r.json()

    app_data = ret['app_data']


    retentions = []
    for data in app_data:
        app_id = data['app_id']
        # app_id 转为str
        if type(app_id) != str:
            app_id = str(app_id)
        date = data['date']
        country = data['country']
        retention = data['corrected_retention']
        
        retentions.append({
            'app_id':app_id,
            'date':date,
            'country':country,
            'retention':retention
        })

    return retentions

def getDemographics(app_ids=[],platform='ios',date_granularity='quarterly',start_date='2021-01-01',end_date='2021-04-01',country=''):
    # https://api.sensortower.com/v1/ios/usage/demographics?app_ids=284882215,310633997&date_granularity=all_time&start_date=2021-01-01&end_date=2021-04-01&auth_token=YOUR_AUTHENTICATION_TOKEN
    url = 'https://api.sensortower.com/v1/{}/usage/demographics?app_ids={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(platform,','.join(app_ids),date_granularity,start_date,end_date,sensortowerToken)
    if country != '':
        url += '&country='+country
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getDemographics failed, status_code:',r.status_code)
        print(r.text)
        return None
    
    # print(r.text)
    demographics = []
    app_data = r.json()['app_data']

    l = ["female_18","female_25","female_35","female_45","female_55","male_18","male_25","male_35","male_45","male_55"]
    for data in app_data:
        app_id = data['app_id']
        # app_id 转为str
        if type(app_id) != str:
            app_id = str(app_id)
        # app_id 去除换行符
        app_id = app_id.replace('\n','')
        date = data['date']
        country = data['country']
        demographic = {
            'app_id':app_id,
            'date':date,
            'country':country,
        }
        for key in l:
            demographic[key] = data['normalized_demographics'][key]

        demographics.append(demographic)

    df = pd.DataFrame(demographics)
    return df


def getActiveUsers(app_ids=[],platform='ios',countries='',time_period='month',start_date='2021-01-01',end_date='2021-01-03'):
    needSum = False
    if time_period == 'quarter':
        # 这个接口只支持month，之后再汇总
        time_period = 'month'
        needSum = True

    # https://api.sensortower.com/v1/ios/usage/active_users?app_ids=284882215&time_period=month&start_date=2021-01-01&end_date=2021-01-03&auth_token=YOUR_AUTHENTICATION_TOKEN
    url = 'https://api.sensortower.com/v1/{}/usage/active_users?app_ids={}&time_period={}&start_date={}&end_date={}&auth_token={}'.format(platform,','.join(app_ids),time_period,start_date,end_date,sensortowerToken)
    if countries != '':
        url += '&countries='+countries
    r = requests.get(url)
    if r.status_code != 200:
        print('Error: getActiveUsers failed, status_code:',r.status_code)
        print(r.text)
        return None
    
    # print(r.text)
    ret = r.json()

    df = pd.DataFrame()

    for data in ret:
        appId = data['app_id']
        # app_id 转为str
        if type(appId) != str:
            appId = str(appId)
        country = data['country']
        date = data['date']
        if platform == 'ios':
            users = data['ipad_users'] + data['iphone_users']
        else:
            users = data['users']
        # users 转为int
        if type(users) != int:
            users = int(users)

        df = df.append({
            'app_id':appId,
            'country':country,
            'date':date,
            'users':users
        },ignore_index=True)


    if needSum:
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].apply(lambda x: x.to_period('Q').to_timestamp())
        df['date'] = df['date'].dt.strftime('%Y-%m')
        df = df.groupby(['app_id','country','date']).agg({'users':'sum'}).reset_index()
    
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

    # print(getDownloadAndRevenue2(appids=['com.topwar.gp','com.fun.lastwar.gp'],os='android',countries='US',date_granularity='monthly',startDate='2023-12-01',endDate='2023-12-31'))
    # print(getDownloadAndRevenue2(['1479198816'],os='ios',countries='US',startDate='2023-12-01',endDate='2023-12-31',date_granularity='weekly'))

    # getRanking()
    # print(getUnifiedAppIds(app_id_type='android',app_ids=['com.topwar.gp','com.fun.lastwar.gp']))
    print(getCreatives(['5cc98b703ea98357b8ed3ce0','64075e77537c41636a8e1c58'],['US'],networks='Admob',start_date='2024-01-01',end_date='2024-01-31',debug=True))
    # print(getRetention(app_ids=['1479198816'],platform='ios',date_granularity='quarterly',start_date='2021-01-01',end_date='2021-04-01'))
    # print(getDemographics(app_ids=['1479198816'],platform='ios',date_granularity='quarterly',start_date='2021-01-01',end_date='2021-04-01'))
    # print(getActiveUsers(platform='ios',app_ids=['1479198816'],time_period='quarter',start_date='2021-01-01',end_date='2021-12-31',countries='US'))

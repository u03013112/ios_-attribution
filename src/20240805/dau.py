# 获得国家分组的dau
# 统计lw和top3的dau或mau的均值的比较
# top1 dau 和 top3 dau 的比值
# top3 dau 和 top10 dau 的比值
# top10 dau 和 top100 dau 的比值
# 用来说明US是竞争更加激烈的地区
# 用来说明T2、T3 竞争不激烈，甚至出现垄断？
# 用来 协助 指定 LW的 策略，比如US可能难以进步，因为竞争激烈，而T2、T3可能有机会，从寡头市场中获得更多的份额即可
import os
import json
import requests
import pandas as pd

import sys
sys.path.append('/src')

from market import getCountryGroupList,getDataFromSt,getRevenueTop3,lwAppId
from src.config import sensortowerToken

def getDauData(app_ids=[],time_period='day',countries=[],start_date='2024-01-01',end_date='2024-06-30'):
    filename = f'/src/data/stDau20240815_{start_date}_{end_date}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # https://api.sensortower-china.com/v1/unified/usage/active_users?app_ids=64075e77537c41636a8e1c58&time_period=day&start_date=2024-01-01&end_date=2024-06-30&countries=US%2CKR%2CJP&auth_token=YOUR_AUTHENTICATION_TOKEN
        url = f'https://api.sensortower.com/v1/unified/usage/active_users?app_ids={"%2C".join(app_ids)}&time_period={time_period}&start_date={start_date}&end_date={end_date}&countries={"%2C".join(countries)}&auth_token={sensortowerToken}'
        print(url)
        response = requests.get(url)
        if response.status_code != 200:
            print('请求失败')
            print(response)
            return
        
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv(filename,index=False)
    return df

def getAllDauData():
    countries = ['AE','AO','AR','AT','AU','AZ','BB','BE','BG','BM','BR','BY','CA','CH','CL','CN','CO','CR','CZ','DE','DK','DO','DZ','EC','EG','ES','FI','FR','GB','GH','GR','GT','HK','HR','HU','ID','IE','IL','IN','IT','JP','KE','KR','KW','KZ','LB','LK','LT','LU','MG','MO','MX','MY','NG','NL','NO','NZ','OM','PA','PE','PH','PK','PL','PT','QA','RO','RU','SA','SE','SG','SI','SK','SV','TH','TN','TR','TW','UA','US','UY','UZ','VE','VN','ZA']
    
    # Q1 & Q2
    filename = 'response_1723448192124.json'

    with open(filename) as f:
        slgJsonStr = f.read()

    slgAppIdList = []
    slgJson = json.loads(slgJsonStr)
    for j in slgJson:
        app_id = j['app_id']
        slgAppIdList.append(app_id)

    df = getDauData(app_ids=slgAppIdList,countries=countries,time_period='month',start_date='2024-01-01',end_date='2024-06-30')
    df['au'] = df['android_users'] + df['ipad_users'] + df['iphone_users']

    df['countryGroup'] = 'Others'
    countryGroupList = getCountryGroupList()
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            df.loc[df['country'] == country, 'countryGroup'] = countryGroup['name']

    return df
        
# 暂时不分平台
def a1():
    sdData = getDataFromSt()
    df = getAllDauData()
    df['ios_users'] = df['iphone_users'] + df['ipad_users']
    df = df.groupby(['country','app_id']).agg({
        'ios_users':'mean',
        'android_users':'mean'    
    }).reset_index()
    df = df.merge(sdData, on=['country','app_id'], how='left')
    # print(df)
    df['countryGroup'] = 'Others'
    countryGroupList = getCountryGroupList()
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            df.loc[df['country'] == country, 'countryGroup'] = countryGroup['name']

    df = df.groupby(['countryGroup','app_id']).agg({
        'ios_users':'sum',
        'android_users':'sum',
        'revenue':'sum'
    }).reset_index()
    # 按照countryGroup分组，然后在分组中按照revenue列进行排序（降序）
    df = df.sort_values(by=['countryGroup', 'revenue'], ascending=[True, False])
    
    # 计算每个分组中的top1，和top3的au sum
    result = df.groupby('countryGroup').apply(lambda x: pd.Series({
        'top1_ios_users_sum': x.head(1)['ios_users'].sum(),
        'top3_ios_users_sum': x.head(3)['ios_users'].sum(),
        'top10_ios_users_sum': x.head(10)['ios_users'].sum(),
        'top1_android_users_sum': x.head(1)['android_users'].sum(),
        'top3_android_users_sum': x.head(3)['android_users'].sum(),
        'top10_android_users_sum': x.head(10)['android_users'].sum(),
    })).reset_index()
    
    result['ios top1/top3'] = result['top1_ios_users_sum'] / result['top3_ios_users_sum']
    result['ios top3/top10'] = result['top3_ios_users_sum'] / result['top10_ios_users_sum']

    result['android top1/top3'] = result['top1_android_users_sum'] / result['top3_android_users_sum']
    result['android top3/top10'] = result['top3_android_users_sum'] / result['top10_android_users_sum']

    # 改为百分比
    result['ios top1/top3'] = result['ios top1/top3'].apply(lambda x: f'{x:.2%}')
    result['ios top3/top10'] = result['ios top3/top10'].apply(lambda x: f'{x:.2%}')
    result['android top1/top3'] = result['android top1/top3'].apply(lambda x: f'{x:.2%}')
    result['android top3/top10'] = result['android top3/top10'].apply(lambda x: f'{x:.2%}')
    
    print(result)
    return result
    
# 获得留存数据
def getRetention(app_ids=[],platform='ios',date_granularity='all_time',start_date='2021-01-01',end_date='2021-04-01',country=''):
    filename = f'/src/data/stRetention_{platform}_{start_date}_{end_date}_{country}.csv'
    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # https://api.sensortower.com/v1/ios/usage/retention?app_ids=5cc98b703ea98357b8ed3ce0&date_granularity=quarterly&start_date=2021-01-01&end_date=2021-04-01&country=US&auth_token=YOUR_AUTHENTICATION_TOKEN
        url = 'https://api.sensortower.com/v1/{}/usage/retention?app_ids={}&date_granularity={}&start_date={}&end_date={}&auth_token={}'.format(platform,','.join(app_ids),date_granularity,start_date,end_date,sensortowerToken)
        print(url)

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
                'retention0':retention[0],
                'retention1':retention[1],
                'retention6':retention[6],
                'retention29':retention[29],
            })

        df = pd.DataFrame(retentions)
        df.to_csv(filename,index=False)

    return df

def getAllRetentionData():
    countries = ["AU","BR","CA","DE","ES","FR","GB","IN","IT","JP","KR","US"]
    # Q1 & Q2
    filename = 'response_1723448192124.json'

    with open(filename) as f:
        slgJsonStr = f.read()

    iosAppIds = []
    androidAppIds = []
    iosUnifiedAppIds = [] 
    androidUnifiedAppIds = [] 

    def is_number(value):
        # 如果值本身是整数或浮点数，直接返回 True
        if isinstance(value, (int, float)):
            return True
        
        # 如果值是字符串，尝试转换为浮点数
        if isinstance(value, str):
            try:
                float(value)
                return True
            except ValueError:
                return False
        
        # 其他类型返回 False
        return False

    slgJson = json.loads(slgJsonStr)
    for j in slgJson:
        entities = j['entities']
        for entity in entities:
            app_id = entity['app_id']
            # 如果app_id是数字，那么就是ios的，如果是字符串，那么就是android的
            if is_number(app_id):
                platform = 'ios'
            else:
                platform = 'android'

            unified_app_id = entity['unified_app_id']
            if platform == 'ios':
                iosAppIds.append(str(app_id))
                iosUnifiedAppIds.append(unified_app_id)
            elif platform == 'android':
                androidAppIds.append(app_id)
                androidUnifiedAppIds.append(unified_app_id)
            
    iosAppIdsDf = pd.DataFrame({
        'iosAppIds':iosAppIds,
        'unifiedAppIds':iosUnifiedAppIds,
    })
    androidAppIdsDf = pd.DataFrame({
        'androidAppIds':androidAppIds,
        'unifiedAppIds':androidUnifiedAppIds,
    })

    retentionDf = pd.DataFrame()

    for country in countries:
        iosRetentions = getRetention(app_ids=iosAppIds,platform='ios',date_granularity='all_time',start_date='2024-01-01',end_date='2024-06-30',country=country)
        iosRetentions['app_id'] = iosRetentions['app_id'].astype(str)
        iosRetentions = iosRetentions.merge(iosAppIdsDf,left_on='app_id',right_on='iosAppIds',how='left')
        iosRetentions.rename(columns={
            'retention0':'ios retention0',
            'retention1':'ios retention1',
            'retention6':'ios retention6',
            'retention29':'ios retention29',
        },inplace=True)
        iosRetentions = iosRetentions[['unifiedAppIds','country','ios retention0','ios retention1','ios retention6','ios retention29']]

        androidRetentions = getRetention(app_ids=androidAppIds,platform='android',date_granularity='all_time',start_date='2024-01-01',end_date='2024-06-30',country=country)
        androidRetentions = androidRetentions.merge(androidAppIdsDf,left_on='app_id',right_on='androidAppIds',how='left')
        androidRetentions.rename(columns={
            'retention0':'android retention0',
            'retention1':'android retention1',
            'retention6':'android retention6',
            'retention29':'android retention29',
        },inplace=True)
        androidRetentions = androidRetentions[['unifiedAppIds','country','android retention0','android retention1','android retention6','android retention29']]

        tmpDf = pd.merge(iosRetentions,androidRetentions,on=['unifiedAppIds','country'],how='outer')

        retentionDf = pd.concat([retentionDf,tmpDf],ignore_index=True)

    # print(retentionDf)
    return retentionDf

def a2():
    sdData = getDataFromSt()
    df = getAllDauData()
    df['ios_users'] = df['iphone_users'] + df['ipad_users']
    df = df[['country','app_id','ios_users','android_users']]
    df = df.groupby(['country','app_id']).agg({
        'ios_users':'mean',
        'android_users':'mean'
    }).reset_index()
    df = df.merge(sdData, on=['country','app_id'], how='left')
    # print(df[df['app_id'].isna()])

    retentionDf = getAllRetentionData()
    retentionDf.rename(columns={
        'unifiedAppIds':'app_id'
    },inplace=True)
    # print(retentionDf[retentionDf['unifiedAppIds'].isna()])

    # 拥有留存的国家不多，所以用right join
    df = df.merge(retentionDf, on=['app_id','country'], how='right')
    df['ios_users0'] = (df['ios_users'] * df['ios retention0']).round(0)
    df['ios_users1'] = (df['ios_users0'] * df['ios retention1']).round(0)
    df['ios_users6'] = (df['ios_users1'] * df['ios retention6']).round(0)
    df['ios_users29'] = (df['ios_users6'] * df['ios retention29']).round(0)

    df['android_users0'] = (df['android_users'] * df['android retention0']).round(0)
    df['android_users1'] = (df['android_users0'] * df['android retention1']).round(0)
    df['android_users6'] = (df['android_users1'] * df['android retention6']).round(0)
    df['android_users29'] = (df['android_users6'] * df['android retention29']).round(0)

    df['countryGroup'] = 'Others'
    countryGroupList = getCountryGroupList()
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            df.loc[df['country'] == country, 'countryGroup'] = countryGroup['name']

    df = df.groupby(['countryGroup','app_id']).agg({
        'ios_users':'sum',
        'ios_users0':'sum',
        'ios_users1':'sum',
        'ios_users6':'sum',
        'ios_users29':'sum',
        'android_users':'sum',
        'android_users0':'sum',
        'android_users1':'sum',
        'android_users6':'sum',
        'android_users29':'sum',
        'revenue':'sum'
    }).reset_index()
    # 按照countryGroup分组，然后在分组中按照revenue列进行排序（降序）
    df = df.sort_values(by=['countryGroup', 'revenue'], ascending=[True, False])
    
    # 计算每个分组中的top1，和top3的au sum
    result = df.groupby('countryGroup').apply(lambda x: pd.Series({
        'top3_ios_users_sum': x.head(3)['ios_users'].sum(),
        'top3_ios_users0_sum': x.head(3)['ios_users0'].sum(),
        'top3_ios_users1_sum': x.head(3)['ios_users1'].sum(),
        'top3_ios_users6_sum': x.head(3)['ios_users6'].sum(),
        'top3_ios_users29_sum': x.head(3)['ios_users29'].sum(),
        'top3_android_users_sum': x.head(3)['android_users'].sum(),
        'top3_android_users0_sum': x.head(3)['android_users0'].sum(),
        'top3_android_users1_sum': x.head(3)['android_users1'].sum(),
        'top3_android_users6_sum': x.head(3)['android_users6'].sum(),
        'top3_android_users29_sum': x.head(3)['android_users29'].sum(),
    })).reset_index()


    result['ios retention0'] = result['top3_ios_users0_sum'] / result['top3_ios_users_sum']
    result['ios retention1'] = result['top3_ios_users1_sum'] / result['top3_ios_users0_sum']
    result['ios retention6'] = result['top3_ios_users6_sum'] / result['top3_ios_users1_sum']
    result['ios retention29'] = result['top3_ios_users29_sum'] / result['top3_ios_users6_sum']

    result['android retention0'] = result['top3_android_users0_sum'] / result['top3_android_users_sum']
    result['android retention1'] = result['top3_android_users1_sum'] / result['top3_android_users0_sum']
    result['android retention6'] = result['top3_android_users6_sum'] / result['top3_android_users1_sum']
    result['android retention29'] = result['top3_android_users29_sum'] / result['top3_android_users6_sum']

    result['ios retention0'] = result['ios retention0'].apply(lambda x: f'{x:.2%}')
    result['ios retention1'] = result['ios retention1'].apply(lambda x: f'{x:.2%}')
    result['ios retention6'] = result['ios retention6'].apply(lambda x: f'{x:.2%}')
    result['ios retention29'] = result['ios retention29'].apply(lambda x: f'{x:.2%}')

    result['android retention0'] = result['android retention0'].apply(lambda x: f'{x:.2%}')
    result['android retention1'] = result['android retention1'].apply(lambda x: f'{x:.2%}')
    result['android retention6'] = result['android retention6'].apply(lambda x: f'{x:.2%}')
    result['android retention29'] = result['android retention29'].apply(lambda x: f'{x:.2%}')

    print(result)
    return result

def a2Lastwar():
    sdData = getDataFromSt()
    df = getAllDauData()
    df = df[df['app_id'] == lwAppId]
    df['ios_users'] = df['iphone_users'] + df['ipad_users']
    df = df[['country','app_id','ios_users','android_users']]
    df = df.groupby(['country','app_id']).agg({
        'ios_users':'mean',
        'android_users':'mean'
    }).reset_index()
    df = df.merge(sdData, on=['country','app_id'], how='left')
    # print(df[df['app_id'].isna()])

    retentionDf = getAllRetentionData()
    retentionDf.rename(columns={
        'unifiedAppIds':'app_id'
    },inplace=True)
    # print(retentionDf[retentionDf['unifiedAppIds'].isna()])

    # 拥有留存的国家不多，所以用right join
    df = df.merge(retentionDf, on=['app_id','country'], how='right')
    df['ios_users0'] = (df['ios_users'] * df['ios retention0']).round(0)
    df['ios_users1'] = (df['ios_users0'] * df['ios retention1']).round(0)
    df['ios_users6'] = (df['ios_users1'] * df['ios retention6']).round(0)
    df['ios_users29'] = (df['ios_users6'] * df['ios retention29']).round(0)

    df['android_users0'] = (df['android_users'] * df['android retention0']).round(0)
    df['android_users1'] = (df['android_users0'] * df['android retention1']).round(0)
    df['android_users6'] = (df['android_users1'] * df['android retention6']).round(0)
    df['android_users29'] = (df['android_users6'] * df['android retention29']).round(0)

    df['countryGroup'] = 'Others'
    countryGroupList = getCountryGroupList()
    for countryGroup in countryGroupList:
        for country in countryGroup['countries']:
            df.loc[df['country'] == country, 'countryGroup'] = countryGroup['name']

    df = df.groupby(['countryGroup','app_id']).agg({
        'ios_users':'sum',
        'ios_users0':'sum',
        'ios_users1':'sum',
        'ios_users6':'sum',
        'ios_users29':'sum',
        'android_users':'sum',
        'android_users0':'sum',
        'android_users1':'sum',
        'android_users6':'sum',
        'android_users29':'sum',
        'revenue':'sum'
    }).reset_index()
    # 按照countryGroup分组，然后在分组中按照revenue列进行排序（降序）
    df = df.sort_values(by=['countryGroup', 'revenue'], ascending=[True, False])
    
    # 计算每个分组中的top1，和top3的au sum
    result = df.groupby('countryGroup').apply(lambda x: pd.Series({
        'top3_ios_users_sum': x.head(3)['ios_users'].sum(),
        'top3_ios_users0_sum': x.head(3)['ios_users0'].sum(),
        'top3_ios_users1_sum': x.head(3)['ios_users1'].sum(),
        'top3_ios_users6_sum': x.head(3)['ios_users6'].sum(),
        'top3_ios_users29_sum': x.head(3)['ios_users29'].sum(),
        'top3_android_users_sum': x.head(3)['android_users'].sum(),
        'top3_android_users0_sum': x.head(3)['android_users0'].sum(),
        'top3_android_users1_sum': x.head(3)['android_users1'].sum(),
        'top3_android_users6_sum': x.head(3)['android_users6'].sum(),
        'top3_android_users29_sum': x.head(3)['android_users29'].sum(),
    })).reset_index()


    result['ios retention0'] = result['top3_ios_users0_sum'] / result['top3_ios_users_sum']
    result['ios retention1'] = result['top3_ios_users1_sum'] / result['top3_ios_users0_sum']
    result['ios retention6'] = result['top3_ios_users6_sum'] / result['top3_ios_users1_sum']
    result['ios retention29'] = result['top3_ios_users29_sum'] / result['top3_ios_users6_sum']

    result['android retention0'] = result['top3_android_users0_sum'] / result['top3_android_users_sum']
    result['android retention1'] = result['top3_android_users1_sum'] / result['top3_android_users0_sum']
    result['android retention6'] = result['top3_android_users6_sum'] / result['top3_android_users1_sum']
    result['android retention29'] = result['top3_android_users29_sum'] / result['top3_android_users6_sum']

    result['ios retention0'] = result['ios retention0'].apply(lambda x: f'{x:.2%}')
    result['ios retention1'] = result['ios retention1'].apply(lambda x: f'{x:.2%}')
    result['ios retention6'] = result['ios retention6'].apply(lambda x: f'{x:.2%}')
    result['ios retention29'] = result['ios retention29'].apply(lambda x: f'{x:.2%}')

    result['android retention0'] = result['android retention0'].apply(lambda x: f'{x:.2%}')
    result['android retention1'] = result['android retention1'].apply(lambda x: f'{x:.2%}')
    result['android retention6'] = result['android retention6'].apply(lambda x: f'{x:.2%}')
    result['android retention29'] = result['android retention29'].apply(lambda x: f'{x:.2%}')

    print(result)
    return result



if __name__ == '__main__':
    # a1Df = a1()
    # a1Df.to_csv('/src/data/a1.csv',index=False)
    # a2Df = a2()
    # a2Df.to_csv('/src/data/a2.csv',index=False)
    a2LastwarDf = a2Lastwar()
    a2LastwarDf.to_csv('/src/data/a2Lastwar.csv',index=False)
    

        
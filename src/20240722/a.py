import os
import numpy as np
import pandas as pd
from datetime import datetime

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getVideoDataFromMC(installTimeStart = '20240601',installTimeEnd = '20240630'):
    filename = f'/src/data/zk2/lwVideoData_{installTimeStart}_{installTimeEnd}.csv'

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
select
    install_day,
    app_package,
    country,
    sum(cost_value_usd) as cost,
    sum(revenue_d7) as r7usd,
    max(video_url) as video_url,
    language,
    earliest_day,
    original_name,
    campaign_name,
    mediasource
from
    rg_bi.dws_material_overseas_data_public
where
    app = '502'
    and material_type = '视频'
    and install_day between {installTimeStart} and {installTimeEnd}
group by
    install_day,
    app_package,
    country,
    language,
    earliest_day,
    original_name,
    campaign_name,
    mediasource;
        '''
        df = execSql(sql)
        df.to_csv(filename,index=False)
        
    return df

def getMediaCostDataFromMC(installTimeStart = '20240601',installTimeEnd = '20240630'):
    filename = f'/src/data/zk2/lwMediaCostData_{installTimeStart}_{installTimeEnd}.csv'

    if os.path.exists(filename):
        print('已存在%s'%filename)
        return pd.read_csv(filename)
    else:
        # 获得用户信息，这里要额外获得归因信息，精确到campaign
        sql = f'''
select
    install_day,
    country,
    mediasource,
    app_package,
    sum(cost_value_usd) as cost,
    sum(revenue_d7) as r7usd
from
    rg_bi.dws_overseas_public_roi
where
    app = '502'
    and zone = 0
    and facebook_segment in ('N/A', 'country')
    and install_day between {installTimeStart} and {installTimeEnd}
group by
    install_day,
    country,
    mediasource,
    app_package
having
    sum(cost_value_usd) > 0;
        '''
        df = execSql(sql)
        df.to_csv(filename,index=False)

    return df

# 对数1
def check1():
    installTimeStart = '20240601'
    installTimeEnd = '20240630'

    videoDf = getVideoDataFromMC(installTimeStart,installTimeEnd)
    mediaCostDf = getMediaCostDataFromMC(installTimeStart,installTimeEnd)

    # 统一app_package
    # print('videoDf app_package:',videoDf['app_package'].unique())
    # # videoDf app_package: ['海外IOS' '海外安卓']
    # print('mediaCostDf app_package:',mediaCostDf['app_package'].unique())
    # # mediaCostDf app_package: ['com.fun.lastwar.gp' 'id6448786147']
    mediaCostDf.replace('com.fun.lastwar.gp','海外安卓',inplace=True)
    mediaCostDf.replace('id6448786147','海外IOS',inplace=True)

    # 统一mediasource
    # print('videoDf mediasource:',videoDf['mediasource'].unique())
    # # videoDf mediasource: ['Facebook' 'Applovin' 'Snapchat' 'tiktok' 'Twitter' 'Google']
    # print('mediaCostDf mediasource:',mediaCostDf['mediasource'].unique())
    # # mediaCostDf mediasource: ['Facebook Ads' 'applovin_int' 'bytedanceglobal_int' 'googleadwords_int' 'snapchat_int' 'unityads_int' 'Apple Search Ads' 'Twitter' 'smartnewsads_int']
    mediaCostDf.replace('Facebook Ads','Facebook',inplace=True)
    mediaCostDf.replace('applovin_int','Applovin',inplace=True)
    mediaCostDf.replace('snapchat_int','Snapchat',inplace=True)
    mediaCostDf.replace('bytedanceglobal_int','tiktok',inplace=True)
    mediaCostDf.replace('googleadwords_int','Google',inplace=True)
    mediaCostDf.replace('unityads_int','Other',inplace=True)
    mediaCostDf.replace('Apple Search Ads','Other',inplace=True)
    mediaCostDf.replace('smartnewsads_int','Other',inplace=True)
    
    # 统一country
    # 将 US KR JP TW 保留，剩下的归为Other
    mediaCostDf['country'] = mediaCostDf['country'].apply(lambda x: x if x in ['US','KR','JP','TW'] else 'Other')
    videoDf['country'] = videoDf['country'].apply(lambda x: x if x in ['US','KR','JP','TW'] else 'Other')

    mediaCostDf = mediaCostDf.groupby(['install_day','country','mediasource','app_package']).sum().reset_index()
    videoDf = videoDf.groupby(['install_day','country','mediasource','app_package']).sum().reset_index()

    # 合并
    df = pd.merge(mediaCostDf,videoDf,on=['install_day','country','mediasource','app_package'],how='outer',suffixes=('_media','_video'))
    df0 = df.loc[df['mediasource'] != 'Other']
    df0.to_csv(f'/src/data/zk2/lwMediaCostVsVideoData_{installTimeStart}_{installTimeEnd}.csv',index=False)

    # 大盘汇总
    
    df1 = df0.groupby(['install_day']).sum().reset_index()
    df1['cost mape'] = np.abs(df1['cost_media'] - df1['cost_video']) / df1['cost_media']
    df1['r7usd mape'] = np.abs(df1['r7usd_media'] - df1['r7usd_video']) / df1['r7usd_media']
    print('大盘汇总')
    print('cost mape:',df1['cost mape'].mean())
    print('r7usd mape:',df1['r7usd mape'].mean())

    # 按媒体汇总

    df2 = df0.groupby(['mediasource','app_package']).sum().reset_index()
    df2['cost mape'] = np.abs(df2['cost_media'] - df2['cost_video']) / df2['cost_media']
    df2['r7usd mape'] = np.abs(df2['r7usd_media'] - df2['r7usd_video']) / df2['r7usd_media']
    print('按媒体汇总')
    print('cost mape:',df2)

    # 找到Google差异
    googleDf = df0.loc[df0['mediasource'] == 'Google']
    googleDf = googleDf.groupby(['install_day','app_package']).sum().reset_index()
    googleDf = googleDf[['install_day','app_package','cost_media','cost_video']]
    googleDf['cost mape'] = np.abs(googleDf['cost_media'] - googleDf['cost_video']) / googleDf['cost_media']
    print('Google差异')
    print(googleDf)
    

if __name__ == '__main__':
    check1()
# 针对iOS的长期报告，长期指30,60,90,120日的报告
# 目的：为了说明不同媒体的长期ROI表现，和长期的付费增长能力的差异
# 进一步目的：是否可以通过长期ROI，适度的减低短期ROI的KPI
# 或者有个猜测，目前的国家分组，分的比较粗，是否可以通过更细的国家分组，来降低部分国家的短期ROI的KPI

import os
import datetime
import pandas as pd

import sys
sys.path.append('/src')

from src.report.geo import getIOSGeoGroup01
from src.report.data.ad import getAdDataIOSGroupByCampaignAndGeoAndMedia,getAdDataIOSGroupByCampaignAndGeoAndMedia2
from src.report.data.revenue import getRevenueDataIOSGroupByCampaignAndGeoAndMedia2,getRevenueDataIOSGroupByGeo

directory = '/src/data/'
def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)

def main(startDayStr,endDayStr):
    print('查询日期：',startDayStr,'~',endDayStr)

    global directory
    directory = f'/src/data/report/iOS2_{startDayStr}_{endDayStr}'

    if not os.path.exists(directory):
        os.makedirs(directory)

    adCostDf = getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStr,endDayStr,directory)
    revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr,directory)

    df = pd.merge(adCostDf,revenueDf,on=[
        'install_date','campaign_id','campaign_name','media','geoGroup'
        ],how='outer',suffixes=('_ad','_revenue'))

    df = df.fillna(0)

    df.to_csv(getFilename('merge'),index=False)

    df = pd.read_csv(getFilename('merge'))
    df = df.groupby(['install_date','media','geoGroup']).sum().reset_index()
    df.to_csv(getFilename('merge_groupby'),index=False)
        
    df = pd.read_csv(getFilename('merge_groupby'),dtype={'install_date':str})
    # print(df.columns)
    df = df[['install_date','media','geoGroup','cost','revenue_7d','revenue_30d','revenue_60d','revenue_90d','revenue_120d']]
    # print(df.head(10))

    # 将安装日期从类似20231022的字符串转换为精确到月份的字符串，如202310
    df['install_date'] = df['install_date'].apply(lambda x:x[:6])
    df2 = df.groupby(['install_date','media','geoGroup']).sum().reset_index()
    # print(df2.head(10))
    df2['roi7d'] = df2['revenue_7d']/df2['cost']
    df2['roi30d'] = df2['revenue_30d']/df2['cost']
    df2['roi60d'] = df2['revenue_60d']/df2['cost']
    df2['roi90d'] = df2['revenue_90d']/df2['cost']
    df2['roi120d'] = df2['revenue_120d']/df2['cost']

    # 猜测，不同媒体的长期表现差异不大，越往后，差异越小
    df2['r30/r7'] = df2['revenue_30d']/df2['revenue_7d']
    df2['r60/r30'] = df2['revenue_60d']/df2['revenue_30d']
    df2['r90/r60'] = df2['revenue_90d']/df2['revenue_60d']
    df2['r120/r90'] = df2['revenue_120d']/df2['revenue_90d']

    df2['r60/r7'] = df2['revenue_60d']/df2['revenue_7d']
    df2['r90/r7'] = df2['revenue_90d']/df2['revenue_7d']
    df2['r120/r7'] = df2['revenue_120d']/df2['revenue_7d']

    df2.to_csv(getFilename('roi'),index=False)


def debug():
    df = pd.read_csv('/src/data/report/iOS2_20230401_20230731/roi.csv')
    df.groupby(['media']).sum().reset_index().to_csv('/src/data/report/iOS2_20230401_20230731/roi_groupby_media.csv',index=False)

def debug2(startDayStr,endDayStr):
    global directory
    directory = f'/src/data/report/iOS2_{startDayStr}_{endDayStr}'
    df = getRevenueDataIOSGroupByGeo(startDayStr,endDayStr,directory)

    df = df.groupby(['country_code']).sum().reset_index()

    adDf = getAdDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr,directory)
    adDf = adDf.groupby(['country_code']).sum().reset_index()

    df = pd.merge(df,adDf,on='country_code',how='outer',suffixes=('_revenue','_ad'))
    df = df.fillna(0)

    # print(df.head(10))
    # 将US作为基准，要求获得roi120高于US的，并且revenue_120d/revenue_7d 也高于US的国家
    df['roi7'] = df['revenue_7d']/df['cost']
    df['roi120'] = df['revenue_120d']/df['cost']
    df['r120/r7'] = df['revenue_120d']/df['revenue_7d']
    roi120US = df.loc[df['country_code']=='US','roi120'].values[0]
    r120r7US = df.loc[df['country_code']=='US','r120/r7'].values[0]

    df = df.loc[
        (df['roi120'] > roi120US)
        & (df['r120/r7'] > r120r7US)
        & (df['cost'] > 1000)
    ][['country_code','cost','roi7','roi120','r120/r7']]

    df.to_csv(getFilename('roi120'),index=False)


    

if __name__ == '__main__':
    main('20230401','20230930')
    # debug()
    # debug2('20230401','20230731')
# 市场相关
# 主要是找到与制定APP相关性强的APP，包括正相关与负相关
# 然后针对这个APP，制定APP相关指数，即与此APP相关的（筛选一批）APP的表现，用来侧面佐证此APP近期的表现
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

import sys
sys.path.append('/src')

from src.sensortower.intel import getTopApp,getDownloadAndRevenue
from src.sensortower.iosIdToName import iOSIdToName
from src.sensortower.androidIdToName import androidIdToName

class Market:
    def __init__(self, appId,platform = 'ios',units = 'day',country = 'US',limit = 1000):
        self.appId = appId
        self.platform = platform
        self.units = units
        self.country = country
        self.limit = limit

        if units == 'day':
            self.dateGranularity = 'daily'
        elif units == 'week':
            self.dateGranularity = 'weekly'
        elif units == 'month':
            self.dateGranularity = 'monthly'
        else:
            self.dateGranularity = 'quarterly'

    # 获取与指定APP相关性强的APP，这里是和免费榜前1000名的APP做比较
    # 要求是在时间范围内在榜单上的所有APP，所有unit中都要有下载数据
    # 返回APPID，相关性 列表，按相关性排序
    # filterAllFreeId = '6009d417241bc16eb8e07e9b'
    # filter4XStrategyId = '600a22c0241bc16eb899fd71'
    def getAPPsCorrelation(self,startDate,endDate,filterId = '6009d417241bc16eb8e07e9b',limit = 0):
        if limit == 0:
            limit = self.limit

        topAppDf = getTopApp(os=self.platform,custom_fields_filter_id=filterId,time_range='year',limit=limit,category='all',countries=self.country,startDate=startDate,endDate=endDate)

        retDf = pd.DataFrame(columns=['appId','downloads','revenues','downloads correlation','revenues correlation'])

        downloadDf0 = getDownloadAndRevenue(self.appId,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate,endDate=endDate)
        downloadDf0 = downloadDf0[['date','downloads','revenues']]
        downloadSum0 = downloadDf0['downloads'].sum()
        revenueSum0 = downloadDf0['revenues'].sum()
        retDf = pd.concat([retDf,pd.DataFrame({'appId':[self.appId],'downloads':[downloadSum0],'revenues':[revenueSum0],'downloads correlation':[1],'revenues correlation':[1]})])

        for appid in topAppDf['appId']:
            if appid == self.appId:
                continue
            downloadDf = getDownloadAndRevenue(appid,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate,endDate=endDate)
            downloadDf = downloadDf[['date','downloads','revenues']]

            df = pd.merge(downloadDf0,downloadDf,on='date',how='left',suffixes=('_0','_1'))
            downloadsCorr = df['downloads_0'].corr(df['downloads_1'])
            revenuesCorr = df['revenues_0'].corr(df['revenues_1'])
            downloadsSum = downloadDf['downloads'].sum()
            revenuesSum = downloadDf['revenues'].sum()

            retDf = pd.concat([retDf,pd.DataFrame({'appId':[appid],'downloads':[downloadsSum],'revenues':[revenuesSum],'downloads correlation':[downloadsCorr],'revenues correlation':[revenuesCorr]})])
            print(f'appid:{appid},downloads:{downloadsSum},revenues:{revenuesSum},downloadsCorr:{downloadsCorr},revenuesCorr:{revenuesCorr}')

        retDf = retDf.sort_values(by='downloads',ascending=False)
        return retDf
    
    # 计算所有相关APP的下载量的和，和的走势就是需要的市场走势
    def getDownloadAndRevenueSum(self,appIdList,startDate,endDate):
        sumDf = pd.DataFrame(columns=['date','downloads','revenues'])
        for appid in appIdList:
            downloadDf = getDownloadAndRevenue(appid,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate,endDate=endDate)
            sumDf = pd.concat([sumDf,downloadDf[['date','downloads','revenues']]])
        sumDf = sumDf.groupby('date').sum().reset_index()
        return sumDf
    

    # self.appId 与 appIdList下载量汇总后的相关系数
    # 分两个周期，startDate1-endDate1，startDate2-endDate2
    # TODO: 收入数据好像在最后一周（不完整周）数据不准，需要确认，如果属实，舍弃最后一周数据收入
    def report(self,appIdList,startDate,midDate,endDate,colName = 'downloads',name = 'downloads'):
        # 获得app名字
        appNameList = []
        for appId in appIdList:
            if self.platform == 'android':
                appName = androidIdToName(appId)
            else:
                appName = iOSIdToName(appId)
            appNameList.append(appName)
        print('appNameList:',appNameList)

        # 简单处理，中间middate多算了一遍， 但是不影响结果
        startDate1 = startDate
        endDate1 = midDate
        startDate2 = midDate
        endDate2 = endDate

        # 获取自己的下载量（colName）
        downloadDf1 = getDownloadAndRevenue(self.appId,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate1,endDate=endDate1)
        downloadDf1 = downloadDf1[['date',colName]]
        downloadSumDf1 = self.getDownloadAndRevenueSum(appIdList,startDate1,endDate1)
        downloadSumDf1 = downloadSumDf1[['date',colName]]
        df1 = pd.merge(downloadDf1,downloadSumDf1,on='date',how='left',suffixes=('_0','_1'))
    
        corr1 = df1[f'{colName}_0'].corr(df1[f'{colName}_1'])
        print(f'{startDate1}~{endDate1}的相关系数：{corr1}')

        downloadDf2 = getDownloadAndRevenue(self.appId,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate2,endDate=endDate2)
        downloadDf2 = downloadDf2[['date',colName]]
        downloadSumDf2 = self.getDownloadAndRevenueSum(appIdList,startDate2,endDate2)
        downloadSumDf2 = downloadSumDf2[['date',colName]]
        df2 = pd.merge(downloadDf2,downloadSumDf2,on='date',how='left',suffixes=('_0','_1'))
        corr2 = df2[f'{colName}_0'].corr(df2[f'{colName}_1'])
        print(f'{startDate2}~{endDate2}的相关系数：{corr2}')

        # 画
        fig, ax1 = plt.subplots(figsize=(16, 5))

        # 将日期字符串转换为日期对象
        df1['date'] = df1['date'].apply(lambda x:x[:10])
        df2['date'] = df2['date'].apply(lambda x:x[:10])
        df1['date'] = df1['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        df2['date'] = df2['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))

        # 数据修正，如果downloads_0和downloads_1差距较大，画在图上会很难看
        # 所以进行标准化
        min1,max1 = df1[f'{colName}_0'].min(),df1[f'{colName}_0'].max()
        min2,max2 = df1[f'{colName}_1'].min(),df1[f'{colName}_1'].max()
        df1[f'{colName}_0n'] = (df1[f'{colName}_0'] - min1) / (max1 - min1)
        df2[f'{colName}_0n'] = (df2[f'{colName}_0'] - min1) / (max1 - min1)
        df1[f'{colName}1n'] = (df1[f'{colName}_1'] - min2) / (max2 - min2)
        df2[f'{colName}1n'] = (df2[f'{colName}_1'] - min2) / (max2 - min2)

        ax1.plot(df1['date'], df1[f'{colName}_0n'], label=f'{self.appId} {colName}')
        ax1.plot(df2['date'], df2[f'{colName}_0n'], label=f'{self.appId} {colName}')
        ax1.plot(df1['date'], df1[f'{colName}1n'], label=f'OtherApps {colName}')
        ax1.plot(df2['date'], df2[f'{colName}1n'], label=f'OtherApps {colName}')

        ax1.set_xlabel('Date')
        ax1.set_ylabel(f'{colName} Normalized')
        ax1.legend(loc='upper left')

        # 添加中间竖线
        midDate_datetime = datetime.strptime(midDate, '%Y-%m-%d')
        plt.axvline(x=midDate_datetime, color='r', linestyle='-', label='Mid Date')

        date_fmt = mdates.DateFormatter('%Y-%m-%d')
        ax1.xaxis.set_major_formatter(date_fmt)
        plt.xticks(rotation=45)
        plt.tight_layout()

        plt.savefig(f"/src/data/{name}{self.appId}_{startDate}_{midDate}_{endDate}.png")
        print(f"图片保存在/src/data/{name}{self.appId}_{startDate}_{midDate}_{endDate}.png")

        return appNameList,corr1,corr2


    def weeklyReport(self):
        N = 12
        M = 4
        # 按周进行报告
        today = datetime.now().strftime('%Y-%m-%d')
        # 获取上周日作为endDate
        endDate = (pd.to_datetime(today) - pd.Timedelta(days=pd.to_datetime(today).weekday() + 1)).strftime('%Y-%m-%d')
        # 获取endDate前N+M周的周日作为startDate
        startDate = (pd.to_datetime(endDate) - pd.Timedelta(days=7 * (N + M - 1)-1)).strftime('%Y-%m-%d')
        # 获取endDate前M周的周日作为midDate
        midDate = (pd.to_datetime(endDate) - pd.Timedelta(days=7 * M)).strftime('%Y-%m-%d')
        print(f'startDate:{startDate},midDate:{midDate},endDate:{endDate}')

        # 获取download相关的APP
        appsDownloadsCorrDf = self.getAPPsCorrelation(startDate,midDate)
        appsDownloadsCorrDf.to_csv('/src/data/appsDownloadsCorrDf.csv',index=False)
        appIddownloadList = appsDownloadsCorrDf[
            (appsDownloadsCorrDf['downloads correlation'] > 0.7) &
            (appsDownloadsCorrDf['downloads'] > 10000)
        ]['appId'].tolist()
        appNameDownloadsCorrList,downloadsCorr1,downloadsCorr2 = self.report(appIddownloadList,startDate,midDate,endDate,colName = 'downloads',name = 'downloadsCorr')

        # 获取revenue相关的APP
        appsRevenuesCorrDf = self.getAPPsCorrelation(startDate,midDate)
        appsRevenuesCorrDf.to_csv('/src/data/appsRevenuesCorrDf.csv',index=False)
        appIdrevenueList = appsRevenuesCorrDf[
            (appsRevenuesCorrDf['revenues correlation'] > 0.7) &
            (appsRevenuesCorrDf['revenues'] > 1000)
        ]['appId'].tolist()
        appNameRevenuesCorrList,revenuesCorr1,revenuesCorr2 = self.report(appIdrevenueList,startDate,midDate,endDate,colName = 'revenues',name = 'revenuesCorr')

        # 获取slg相关的APP
        slgAppsDf = self.getAPPsCorrelation(startDate,midDate,filterId = '600a22c0241bc16eb899fd71',limit=50)
        slgAppsDf.to_csv('/src/data/slgAppsDf.csv',index=False)
        slgAppsList = slgAppsDf['appId'].tolist()
        appNameSlgList,slgCorr1,slgCorr2 = self.report(slgAppsList,startDate,midDate,endDate,colName = 'downloads',name = 'slgDownloads')

if __name__ == '__main__':
    # topwar android US 按周
    market = Market('com.topwar.gp','android','week','US')
    # 2023年4季度走势，与topwar相关性强的APP
    # appsCorrDf = market.getAPPsCorrelation('2023-10-01','2024-01-01')
    # appsCorrDf.to_csv('/src/data/appsCorrDf0.csv',index=False)
    # appsCorrDf = pd.read_csv('/src/data/appsCorrDf0.csv')
    # # appsCorrDf = pd.read_csv('/src/data/appsCorrDf1.csv')
    
    # downloadAppsList = appsCorrDf[
    #     (appsCorrDf['downloads correlation'] > 0.7) &
    #     (appsCorrDf['downloads'] > 10000)
    # ]['appId'].tolist()
    # appNameList,corr1,corr2 = market.report(downloadAppsList,'2023-10-01','2024-01-01','2024-02-05',colName = 'downloads')

    # revenueAppsList = appsCorrDf[
    #     (appsCorrDf['revenues correlation'] > 0.7) &
    #     (appsCorrDf['revenues'] > 1000)
    # ]['appId'].tolist()
    # appNameList,corr1,corr2 = market.report(revenueAppsList,'2023-10-01','2024-01-01','2024-02-05',colName = 'revenues')

    # # 找到slg游戏
    # slgAppsDf = market.getAPPsCorrelation('2023-10-01','2024-01-01','600a22c0241bc16eb899fd71',limit=50)
    # slgAppsDf.to_csv('/src/data/slgAppsDf.csv',index=False)

    # # slg部分不做过滤，所有APP都要
    # slgAppsList = slgAppsDf['appId'].tolist()
    # appNameList,corr1,corr2 = market.report(slgAppsList,'2023-10-01','2024-01-01','2024-02-05',colName = 'downloads',name = 'slgDownloads')
    # appNameList,corr1,corr2 = market.report(slgAppsList,'2023-10-01','2024-01-01','2024-02-05',colName = 'revenues',name = 'slgRevenues')

    market.weeklyReport()
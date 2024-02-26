# 市场相关
# 主要是找到与制定APP相关性强的APP，包括正相关与负相关
# 然后针对这个APP，制定APP相关指数，即与此APP相关的（筛选一批）APP的表现，用来侧面佐证此APP近期的表现
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from tqdm import tqdm

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
    
    # 与getAPPsCorrelation功能类似，只是不再计算相关性，只是获取数据
    # 方便后面使用不同的方法计算相关性
    def getAPPsDownloadAndRevenue(self,startDate,endDate,filterId = '6009d417241bc16eb8e07e9b',limit = 0):
        if limit == 0:
            limit = self.limit

        topAppDf = getTopApp(os=self.platform,custom_fields_filter_id=filterId,time_range='year',limit=limit,category='all',countries=self.country,startDate=startDate,endDate=endDate)

        downloadsRetDf = pd.DataFrame(columns=['appId','date','downloads'])
        revenuesRetDf = pd.DataFrame(columns=['appId','date','revenues'])

        downloadDf0 = getDownloadAndRevenue(self.appId,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate,endDate=endDate)
        downloadDf0 = downloadDf0[['date','downloads','revenues']]
        
        downloadsRetDf = pd.concat([downloadsRetDf,pd.DataFrame({'appId':[self.appId]*len(downloadDf0),'date':downloadDf0['date'],'downloads':downloadDf0['downloads']})])
        revenuesRetDf = pd.concat([revenuesRetDf,pd.DataFrame({'appId':[self.appId]*len(downloadDf0),'date':downloadDf0['date'],'revenues':downloadDf0['revenues']})])

        for appid in tqdm(topAppDf['appId']):
            if appid == self.appId:
                continue
            downloadDf = getDownloadAndRevenue(appid,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate,endDate=endDate)
            downloadDf = downloadDf[['date','downloads','revenues']]

            downloadsRetDf = pd.concat([downloadsRetDf,pd.DataFrame({'appId':[appid]*len(downloadDf),'date':downloadDf['date'],'downloads':downloadDf['downloads']})])
            revenuesRetDf = pd.concat([revenuesRetDf,pd.DataFrame({'appId':[appid]*len(downloadDf),'date':downloadDf['date'],'revenues':downloadDf['revenues']})])

        downloadsRetDf = downloadsRetDf.sort_values(by=['appId','date'],ascending=False)
        revenuesRetDf = revenuesRetDf.sort_values(by=['appId','date'],ascending=False)

        return downloadsRetDf,revenuesRetDf
    
    # 从getAPPsDownloadAndRevenue的结果中，获取相关性
    def getAPPsCorrelationFromDF(self,df,colName = 'downloads',corr = ''):
        appIdList = df['appId'].unique().tolist()
        if self.appId in appIdList:
            appIdList.remove(self.appId)

        df0 = df[df['appId'] == self.appId][['date',colName]]
        sum0 = df[df['appId'] == self.appId][colName].sum()

        retDf = pd.DataFrame(columns=['appId',colName,'correlation'])
        retDf = pd.concat([retDf,pd.DataFrame({'appId':[self.appId],colName:[sum0],'correlation':[1]})])
        for appid in appIdList:
            df1 = df[df['appId'] == appid][['date',colName]]
            dfMerge = pd.merge(df0,df1,on='date',how='left',suffixes=('_0','_1'))
            try:
                if corr == 'spearmanr':
                    from scipy.stats import spearmanr
                    correlation,_ = spearmanr(dfMerge[f'{colName}_0'],dfMerge[f'{colName}_1'])
                elif corr == 'kendalltau':
                    from scipy.stats import kendalltau
                    correlation,_ = kendalltau(dfMerge[f'{colName}_0'],dfMerge[f'{colName}_1'])
                elif corr == 'r2':
                    from sklearn.preprocessing import MinMaxScaler
                    from sklearn.metrics import r2_score
                    scaler = MinMaxScaler()
                    dfMerge[f'{colName}_0'] = scaler.fit_transform(dfMerge[f'{colName}_0'].values.reshape(-1,1))
                    dfMerge[f'{colName}_1'] = scaler.fit_transform(dfMerge[f'{colName}_1'].values.reshape(-1,1))
                    # 检查数据中是否包含NaN值或无穷大值，并替换为0
                    dfMerge = dfMerge.replace([np.inf, -np.inf], np.nan).fillna(0)
                    correlation = r2_score(dfMerge[f'{colName}_0'],dfMerge[f'{colName}_1'])
                else:
                    # 默认是pearson相关系数
                    correlation = dfMerge[f'{colName}_0'].corr(dfMerge[f'{colName}_1'])
            except AttributeError:
                correlation = 0

            sum1 = df[df['appId'] == appid][colName].sum()
            retDf = pd.concat([retDf,pd.DataFrame({'appId':[appid],colName:[sum1],'correlation':[correlation]})])

        retDf = retDf.sort_values(by=colName,ascending=False)
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
    def report(self,appIdList,startDate,midDate,endDate,colName = 'downloads',name = 'downloads',corr = ''):
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
    
        if corr == 'spearmanr':
            from scipy.stats import spearmanr
            corr1,_ = spearmanr(df1[f'{colName}_0'],df1[f'{colName}_1'])
        elif corr == 'kendalltau':
            from scipy.stats import kendalltau
            corr1,_ = kendalltau(df1[f'{colName}_0'],df1[f'{colName}_1'])
        elif corr == 'r2':
            from sklearn.preprocessing import MinMaxScaler
            from sklearn.metrics import r2_score
            scaler = MinMaxScaler()
            df1[f'{colName}_0'] = scaler.fit_transform(df1[f'{colName}_0'].values.reshape(-1,1))
            df1[f'{colName}_1'] = scaler.fit_transform(df1[f'{colName}_1'].values.reshape(-1,1))
            df1.replace([np.inf, -np.inf], np.nan).fillna(0)
            corr1 = r2_score(df1[f'{colName}_0'],df1[f'{colName}_1'])
        else:
            corr1 = df1[f'{colName}_0'].corr(df1[f'{colName}_1'])
        
        print(f'{startDate1}~{endDate1}的相关系数：{corr1}')

        downloadDf2 = getDownloadAndRevenue(self.appId,os=self.platform,countries=self.country,date_granularity=self.dateGranularity,startDate=startDate2,endDate=endDate2)
        downloadDf2 = downloadDf2[['date',colName]]
        downloadSumDf2 = self.getDownloadAndRevenueSum(appIdList,startDate2,endDate2)
        downloadSumDf2 = downloadSumDf2[['date',colName]]
        df2 = pd.merge(downloadDf2,downloadSumDf2,on='date',how='left',suffixes=('_0','_1'))

        if corr == 'spearmanr':
            from scipy.stats import spearmanr
            corr2,_ = spearmanr(df2[f'{colName}_0'],df2[f'{colName}_1'])
        elif corr == 'kendalltau':
            from scipy.stats import kendalltau
            corr2,_ = kendalltau(df2[f'{colName}_0'],df2[f'{colName}_1'])
        elif corr == 'r2':
            from sklearn.preprocessing import MinMaxScaler
            from sklearn.metrics import r2_score
            scaler = MinMaxScaler()
            df2[f'{colName}_0'] = scaler.fit_transform(df2[f'{colName}_0'].values.reshape(-1,1))
            df2[f'{colName}_1'] = scaler.fit_transform(df2[f'{colName}_1'].values.reshape(-1,1))
            df2.replace([np.inf, -np.inf], np.nan).fillna(0)
            corr2 = r2_score(df2[f'{colName}_0'],df2[f'{colName}_1'])
        else:
            corr2 = df2[f'{colName}_0'].corr(df2[f'{colName}_1'])
        
        print(f'{startDate2}~{endDate2}的相关系数：{corr2}')

        # 画
        fig, ax1 = plt.subplots(figsize=(16, 5))

        # 将日期字符串转换为日期对象
        df1['date'] = df1['date'].apply(lambda x:x[:10])
        df2['date'] = df2['date'].apply(lambda x:x[:10])
        df1['date'] = df1['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        df2['date'] = df2['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))

        df3 = pd.concat([df1,df2])

        # 数据修正，如果downloads_0和downloads_1差距较大，画在图上会很难看
        # 所以进行标准化
        min1,max1 = df3[f'{colName}_0'].min(),df3[f'{colName}_0'].max()
        min2,max2 = df3[f'{colName}_1'].min(),df3[f'{colName}_1'].max()
        df3[f'{colName}_0n'] = (df3[f'{colName}_0'] - min1) / (max1 - min1)
        df3[f'{colName}1n'] = (df3[f'{colName}_1'] - min2) / (max2 - min2)
        

        ax1.plot(df3['date'], df3[f'{colName}_0n'], label=f'{self.appId} {colName}')
        ax1.plot(df3['date'], df3[f'{colName}1n'], label=f'OtherApps {colName}')

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

        # downloadsRetDf,revenuesRetDf = self.getAPPsDownloadAndRevenue(startDate,midDate)
        # downloadsRetDf.to_csv('/src/data/downloadsRetDf_2.csv',index=False)
        # revenuesRetDf.to_csv('/src/data/revenuesRetDf_2.csv',index=False)
        
        # downloadsRetDf = pd.read_csv('/src/data/downloadsRetDf_2.csv')
        # revenuesRetDf = pd.read_csv('/src/data/revenuesRetDf_2.csv')

        downloadsRetDf = pd.read_csv('/src/data/downloadsRetDf.csv')
        # revenuesRetDf = pd.read_csv('/src/data/revenuesRetDf.csv')
        
        corrList = [
            'spearmanr','kendalltau','r2','pearson'
        ]

        # 获取download相关的APP
        for corr in corrList:
            appsDownloadsCorrDf = self.getAPPsCorrelationFromDF(downloadsRetDf,colName = 'downloads',corr = corr)
            appIddownloadList = appsDownloadsCorrDf[
                (appsDownloadsCorrDf['correlation'] > 0.7) &
                (appsDownloadsCorrDf['downloads'] > 10000)
            ]['appId'].tolist()
            appNameDownloadsCorrList,downloadsCorr1,downloadsCorr2 = self.report(appIddownloadList,startDate,midDate,endDate,colName = 'downloads',name = 'downloads_'+corr,corr = corr)

        # # 获取revenue相关的APP
        # for corr in corrList:
        #     appsRevenuesCorrDf = self.getAPPsCorrelationFromDF(revenuesRetDf,colName = 'revenues',corr = corr)
        #     appIdrevenueList = appsRevenuesCorrDf[
        #         (appsRevenuesCorrDf['correlation'] > 0.7) &
        #         (appsRevenuesCorrDf['revenues'] > 1000)
        #     ]['appId'].tolist()
        #     appNameRevenuesCorrList,revenuesCorr1,revenuesCorr2 = self.report(appIdrevenueList,startDate,midDate,endDate,colName = 'revenues',name = 'revenues_'+corr,corr = corr)

        # 获取slg相关的APP
        downloadsRetDf,revenuesRetDf = self.getAPPsDownloadAndRevenue(startDate,midDate,filterId = '600a22c0241bc16eb899fd71',limit=50)
        for corr in corrList:
            slgAppsDf = self.getAPPsCorrelationFromDF(downloadsRetDf,colName = 'downloads',corr = corr)

            slgAppsList = slgAppsDf['appId'].tolist()
            appNameSlgList,slgCorr1,slgCorr2 = self.report(slgAppsList,startDate,midDate,endDate,colName = 'downloads',name = 'slgDownloads_'+corr,corr = corr)

if __name__ == '__main__':
    # topwar android US 按周
    market = Market('com.topwar.gp','android','week','US')
    # market = Market('com.topwar.gp','android','week','KR')
    market.weeklyReport()
# 指数
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import sys
sys.path.append('/src')

from src.sensortower.intel import getDownloadAndRevenue,getRetention,getTopApp,getDownloadAndRevenue2,getUnifiedAppIds,getDemographics,getActiveUsers


# 抽象指数相关方法，输入数据，并获得指数，再将指数绘图

# 计算指数，其中df要求包括列 date，格式 2024-01-02 str
# dataColumn 指定数据列
# stdDateStartStr 和 stdDateEndStr 指定标准时间段，格式 2024-01-02 str。
# 标准时段必须在df中存在，用标准时段的数据作为基准，即1000点，计算其他时间段的指数（df中所有数据）。
# 返回df，增加一列，列名为 index，值为指数。
# removeMin 和 removeMax 为True时，去掉标准时段中数据的最小值和最大值，再计算指数。
def addIndexColumn(df,dataColumn,dateColumn = 'date',stdDateStartStr='2024-01-01',stdDateEndStr='2024-01-31',removeMin = True,removeMax = True):
    stdDf = df[(df[dateColumn] >= stdDateStartStr) & (df[dateColumn] <= stdDateEndStr)]
    if removeMin:
        stdDf = stdDf[stdDf[dataColumn] != stdDf[dataColumn].min()]
    if removeMax:
        stdDf = stdDf[stdDf[dataColumn] != stdDf[dataColumn].max()]
    stdValue = stdDf[dataColumn].mean()
    df['index'] = 1000 * df[dataColumn] / stdValue

    return df

def draw(df, dateColumn='date', stdDateStartStr='', stdDateEndStr='', dataColumnList=['index'], labelList=['index'], filename='index.png'):
    df[dateColumn] = pd.to_datetime(df[dateColumn])
    date_fmt = mdates.DateFormatter('%Y-%m-%d')

    fig, ax = plt.subplots(figsize=(16, 6))

    for i in range(len(dataColumnList)):
        # 判断如果dataColumnList[i]列真的存在
        if dataColumnList[i] in df.columns:
            ax.plot(df[dateColumn], df[dataColumnList[i]], label=labelList[i])

    ax.set_xlabel('date')
    ax.set_ylabel('index')

    # stdDateStartStr 和 stdDateEndStr 画两条竖线
    if stdDateStartStr != '' and stdDateEndStr != '':
        ax.axvline(x=pd.to_datetime(stdDateStartStr), c='r', linestyle='--', label='index std date')
        ax.axvline(x=pd.to_datetime(stdDateEndStr), c='r', linestyle='--')

    ax.xaxis.set_major_formatter(date_fmt)
    plt.tight_layout()
    plt.legend()

    plt.savefig(filename)
    print(f'save to {filename}')


def test():
    topwarDfFilename = '/src/data/topwarDf.csv'
    if os.path.exists(topwarDfFilename):
        topwarDf = pd.read_csv(topwarDfFilename)
    else:
        topwarDf = getDownloadAndRevenue('com.topwar.gp',os='android',countries='US',date_granularity='weekly',startDate='2023-12-01',endDate='2024-02-29')
        topwarDf.to_csv(topwarDfFilename,index=False)

    topwarDf = topwarDf[['date','revenues']]
    # topwarDf['date'] = topwarDf['date'].apply(lambda x:x[:10])
    topwarDf = addIndexColumn(topwarDf,'revenues',stdDateStartStr='2024-01-01',stdDateEndStr='2024-01-31')


    draw(topwarDf,stdDateStartStr='2024-01-01',stdDateEndStr='2024-01-31',filename='/src/data/topwarIndex.png')


# SLG指数增加三个观测目标值，并跟topwar，lastwar，topheroes比较
# 30日留存
# ARPU
# 美国男性用户25岁-44岁活跃用户数

# 30日留存
def retention30Day():
    startDate='2021-01-01'
    endDate='2023-12-31'

    # 单独关注的APPs
    apps = [
        {'name':'topwar','android':'com.topwar.gp','ios':'1479198816'},
        {'name':'lastwar','android':'com.fun.lastwar.gp','ios':'6448786147'},
        {'name':'topheroes','android':'com.greenmushroom.boomblitz.gp','ios':'6450953550'},
        {'name':'Lords Mobile: Kingdom Wars','android':'com.igg.android.lordsmobile','ios':'1071976327'},
        {'name':'Evony','android':'com.topgamesinc.evony','ios':'1094930835'},
        {'name':'State of Survival','android':'com.kingsgroup.sos','ios':'1452471765'}
    ]


    for platform in ['android','ios']:

        app_ids = [app[platform] for app in apps]        

        retention = getRetention(app_ids = app_ids,platform=platform,start_date=startDate,end_date=endDate)

        df = pd.DataFrame()
        for r in retention:
            appName = ''
            for app in apps:
                if r['app_id'] == app[platform]:
                    appName = app['name']
                    break

            # 2021-01-01T00:00:00Z 截取前10位
            date = r['date'][:10]
            country = r['country']
            retention30 = r['retention'][29]

            df = df.append({'date':date,'country':country,'appName':appName,'retention30':retention30},ignore_index=True)

        # 将表格转换为所需格式
        pivot_df = df.pivot_table(index=['country', 'date'], columns='appName', values='retention30').reset_index()

        # 重命名列名
        pivot_df.columns.name = ''
        columns = {}
        for app in apps:
            columns[app['name']] = app['name'] + ' retention30'
        pivot_df.rename(columns=columns, inplace=True)
    
        # 2021 年 slg top 10
        topAppDf = getTopApp(os=platform,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=10,category='all',countries='WW',startDate='2021-01-01',endDate='2021-12-31')
        appIdList = topAppDf['appId'].tolist()

        print(appIdList)
        appNames = []
        unifiedAppIds = getUnifiedAppIds(app_id_type=platform,app_ids=appIdList)
        for unifiedAppId in unifiedAppIds:
            appNames.append(unifiedAppId['name'])
        print(appNames)

        slgTopNDf = getDownloadAndRevenue2(appIdList,platform,'WW','quarterly',startDate,endDate)

        slgRetentionDf = pd.DataFrame()
        retention = getRetention(app_ids = appIdList,platform=platform,start_date=startDate,end_date=endDate)
        for r in retention:
            slgRetentionDf = slgRetentionDf.append({
                'date':r['date'],
                'country':r['country'],
                'app_id':r['app_id'],
                'retention30':r['retention'][29]
            },ignore_index=True)

        # app_id 转为str，并去除换行符
        slgTopNDf['app_id'] = slgTopNDf['app_id'].apply(lambda x:str(x).replace('\n',''))
        slgRetentionDf['app_id'] = slgRetentionDf['app_id'].apply(lambda x:str(x).replace('\n',''))

        slgMergeDf = pd.merge(slgTopNDf,slgRetentionDf,on=['date','app_id','country'],how='left')
        
        slgMergeDf['user30'] = slgMergeDf['downloads'] * slgMergeDf['retention30']
        slgMergeDf = slgMergeDf.groupby(['date','country']).agg(
            {'user30':'sum','downloads':'sum'}).reset_index()
        slgMergeDf['retention30'] = slgMergeDf['user30'] / slgMergeDf['downloads']
        # 2021-01-01T00:00:00Z -> 2021-01-01
        slgMergeDf['date'] = slgMergeDf['date'].apply(lambda x:x[:10])

        slgMergeDf = slgMergeDf[['date','country','retention30']]
        slgMergeDf.rename(columns={'retention30':'slg retention30'},inplace=True)

        df = pd.merge(pivot_df,slgMergeDf,on=['date','country'],how='left')

        df.to_csv(f'/src/data/retention30_{platform}.csv',index=False)

        dataColumnList = ['slg retention30']
        for app in apps:
            dataColumnList.append(app['name'] + ' retention30')

        draw(df,dataColumnList=dataColumnList, labelList=dataColumnList,filename=f'/src/data/retention30_{platform}_index.png')

# 美国男性用户25岁-44岁活跃用户数
def demography():
    startDate='2021-01-01'
    endDate='2023-12-31'

    # 单独关注的APPs
    apps = [
        {'name':'topwar','android':'com.topwar.gp','ios':'1479198816'},
        {'name':'lastwar','android':'com.fun.lastwar.gp','ios':'6448786147'},
        {'name':'topheroes','android':'com.greenmushroom.boomblitz.gp','ios':'6450953550'},
        {'name':'Lords Mobile: Kingdom Wars','android':'com.igg.android.lordsmobile','ios':'1071976327'},
        {'name':'Evony','android':'com.topgamesinc.evony','ios':'1094930835'},
        {'name':'State of Survival','android':'com.kingsgroup.sos','ios':'1452471765'}
    ]

    for platform in ['android','ios']:
        app_ids = [app[platform] for app in apps]

        df = getDemographics(app_ids = app_ids,platform = platform,start_date = startDate,end_date = endDate,date_granularity='quarterly')
        df['appName'] = df['app_id']
        for app in apps:
            appName = app['name']
            df.loc[df['app_id'] == app[platform],'appName'] = appName
            
        df['male_25_45'] = df['male_25'] + df['male_35'] + df['male_45']
        df = df[['appName','date','country','male_25_45']]
        # print(df)

        pivot_df = df.pivot_table(index=['country', 'date'], columns='appName', values='male_25_45').reset_index()

        # 重命名列名
        pivot_df.columns.name = ''
        columns = {}
        for app in apps:
            columns[app['name']] = app['name'] + ' male_25_45'
        pivot_df.rename(columns=columns, inplace=True)

        # print(pivot_df)
        # 2021 年 slg top 10
        topAppDf = getTopApp(os=platform,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=10,category='all',countries='WW',startDate='2021-01-01',endDate='2021-12-31')
        appIdList = topAppDf['appId'].tolist()

        print(appIdList)
        appNames = []
        unifiedAppIds = getUnifiedAppIds(app_id_type=platform,app_ids=appIdList)
        for unifiedAppId in unifiedAppIds:
            appNames.append(unifiedAppId['name'])
        print(appNames)

        slgTopNDf = getDownloadAndRevenue2(appIdList,platform,'WW','quarterly',startDate,endDate)
        
        demographics = getDemographics(app_ids = appIdList,platform = platform,start_date = startDate,end_date = endDate,date_granularity='quarterly')
        demographics['male_25_45'] = demographics['male_25'] + demographics['male_35'] + demographics['male_45']
        demographics = demographics[['app_id','date','country','male_25_45']]

        # app_id 转为str，并去除换行符
        slgTopNDf['app_id'] = slgTopNDf['app_id'].apply(lambda x:str(x).replace('\n',''))
        demographics['app_id'] = demographics['app_id'].apply(lambda x:str(x).replace('\n',''))

        slgMergeDf = pd.merge(slgTopNDf,demographics,on=['date','app_id','country'],how='left')

        slgMergeDf['user_male_25_45'] = slgMergeDf['downloads'] * slgMergeDf['male_25_45']
        slgMergeDf = slgMergeDf.groupby(['date','country']).agg(
            {'user_male_25_45':'sum','downloads':'sum'}).reset_index()
        slgMergeDf['male_25_45'] = slgMergeDf['user_male_25_45'] / slgMergeDf['downloads']

        slgMergeDf = slgMergeDf[['date','country','male_25_45']]
        slgMergeDf.rename(columns={'male_25_45':'slg male_25_45'},inplace=True)

        df = pd.merge(pivot_df,slgMergeDf,on=['date','country'],how='left')

        df.to_csv(f'/src/data/male_25_45_{platform}.csv',index=False)

        dataColumnList = ['slg male_25_45']
        for app in apps:
            dataColumnList.append(app['name'] + ' male_25_45')

        draw(df,dataColumnList=dataColumnList, labelList=dataColumnList,filename=f'/src/data/male_25_45_{platform}_index.png')


def arpu():
    startDate='2021-01-01'
    endDate='2023-12-31'

    # 单独关注的APPs
    apps = [
        {'name':'topwar','android':'com.topwar.gp','ios':'1479198816'},
        {'name':'lastwar','android':'com.fun.lastwar.gp','ios':'6448786147'},
        {'name':'topheroes','android':'com.greenmushroom.boomblitz.gp','ios':'6450953550'},
        {'name':'Lords Mobile: Kingdom Wars','android':'com.igg.android.lordsmobile','ios':'1071976327'},
        {'name':'Evony','android':'com.topgamesinc.evony','ios':'1094930835'},
        {'name':'State of Survival','android':'com.kingsgroup.sos','ios':'1452471765'}
    ]

    for platform in ['android','ios']:
        app_ids = [app[platform] for app in apps]

        df = getDownloadAndRevenue2(app_ids,platform,'WW','quarterly',startDate,endDate)

        df['date'] = df['date'].apply(lambda x:x[:7])

        activeUsersDf = getActiveUsers(app_ids,platform,'WW','quarter',startDate,endDate)
        
        df = pd.merge(df,activeUsersDf,on=['date','country','app_id'],how='left')

        df['appName'] = df['app_id']
        for app in apps:
            appName = app['name']
            df.loc[df['app_id'] == app[platform],'appName'] = appName
        
        print(df)
        df = df[['appName','date','country','users','revenues']]

        df['arpu'] = df['revenues'] / df['users']

        pivot_df = df.pivot_table(index=['country', 'date'], columns='appName', values='arpu').reset_index()
        pivot_df.columns.name = ''
        columns = {}
        for app in apps:
            columns[app['name']] = app['name'] + ' arpu'
        pivot_df.rename(columns=columns, inplace=True)

        # 2021 年 slg top 10
        topAppDf = getTopApp(os=platform,custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='year',limit=10,category='all',countries='WW',startDate='2021-01-01',endDate='2021-12-31')
        appIdList = topAppDf['appId'].tolist()

        print(appIdList)
        appNames = []
        unifiedAppIds = getUnifiedAppIds(app_id_type=platform,app_ids=appIdList)
        for unifiedAppId in unifiedAppIds:
            appNames.append(unifiedAppId['name'])

        slgTopNDf = getDownloadAndRevenue2(appIdList,platform,'WW','quarterly',startDate,endDate)

        slgTopNDf['date'] = slgTopNDf['date'].apply(lambda x:x[:7])

        slgTopActiveUsersDf = getActiveUsers(appIdList,platform,'WW','quarter',startDate,endDate)
        slgTopNDf = pd.merge(slgTopNDf,slgTopActiveUsersDf,on=['date','country','app_id'],how='left')
        print(slgTopNDf)

        slgTopNDf = slgTopNDf.groupby(['date','country']).agg(
            {'downloads':'sum','revenues':'sum','users':'sum'}).reset_index()
        slgTopNDf['arpu'] = slgTopNDf['revenues'] / slgTopNDf['users']
        slgTopNDf.rename(columns={'arpu':'slg arpu'},inplace=True)
        slgTopNDf['date'] = slgTopNDf['date'].apply(lambda x:x[:7])

        df = pd.merge(slgTopNDf,pivot_df,on=['date','country'],how='left') 
        df.to_csv(f'/src/data/arpu_{platform}.csv',index=False)
        print(f'save to /src/data/arpu_{platform}.csv')

        dataColumnList = ['slg arpu']
        for app in apps:
            dataColumnList.append(app['name'] + ' arpu')

        draw(df,dataColumnList=dataColumnList, labelList=dataColumnList,filename=f'/src/data/arpu_{platform}_index.png')

if __name__ == '__main__':
    # test()
    # retention30Day()
    # demography()
    arpu()
    
# 
import numpy as np
import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getFilename(filename,ext='csv'):
    return '/src/data/zk2/%s.%s'%(filename,ext)

def mind1():
    df = pd.read_csv(getFilename('attCampaign24_attribution24RetCheck'))
    # df 中有列：campaign_id,install_date,r7usd,user_count,r1usd,r7usdp,MAPE

    # 为了有效的计算MAPE，需要过滤掉r7usd为0的数据
    # df = df.loc[df['r7usd'] > 0]

    df2 = df.groupby(by = ['install_date']).agg({
        'r7usd':'sum',
        'r7usdp':'sum',
    })
    df2['MAPE'] = abs(df2['r7usd'] - df2['r7usdp']) / df2['r7usd']
    print('原本MAPE:',df2['MAPE'].mean())

    df = df.sort_values(by='user_count',ascending=True)
    userCountList = df['user_count'].quantile([0.1,0.2,0.3,0.4,0.5]).tolist()
    print('用户数 10%分位数:',userCountList[0])
    print('用户数 20%分位数:',userCountList[1])
    print('用户数 30%分位数:',userCountList[2])
    print('用户数 40%分位数:',userCountList[3])
    print('用户数 50%分位数:',userCountList[4])

    for userCount in userCountList:
        print(f'过滤掉用户数小于{userCount}的数据后，影响用户比例：',(df.loc[df['user_count'] < userCount]['user_count'].sum() / df['user_count'].sum()))
        print(f'过滤掉用户数小于{userCount}的数据后，影响首日收入比例：',(df.loc[df['user_count'] < userCount]['r1usd'].sum() / df['r1usd'].sum()))

        df2 = df.loc[df['user_count'] > userCount].copy()
        df2 = df2.groupby(by = ['install_date']).agg({
            'r7usd':'sum',
            'r7usdp':'sum',
        })
        df2['MAPE'] = abs(df2['r7usd'] - df2['r7usdp']) / df2['r7usd']
        print('过滤后MAPE:',df2['MAPE'].mean())

    # 找到r1usd列中的5%，10%，15%分位数
    df = df.sort_values(by='r1usd',ascending=True)
    r1usdList = df['r1usd'].quantile([0.1,0.2,0.3,0.4,0.5]).tolist()
    print('r1usd 10%分位数:',r1usdList[0])
    print('r1usd 20%分位数:',r1usdList[1])
    print('r1usd 30%分位数:',r1usdList[2])
    print('r1usd 40%分位数:',r1usdList[3])
    print('r1usd 50%分位数:',r1usdList[4])

    for r1usd in r1usdList:
        print(f'过滤掉r1usd小于{r1usd}的数据后，影响用户比例：',(df.loc[df['r1usd'] < r1usd]['user_count'].sum() / df['user_count'].sum()))
        print(f'过滤掉r1usd小于{r1usd}的数据后，影响首日收入比例：',(df.loc[df['r1usd'] < r1usd]['r1usd'].sum() / df['r1usd'].sum()))

        df2 = df.loc[df['r1usd'] > r1usd].copy()
        df2 = df2.groupby(by = ['install_date']).agg({
            'r7usd':'sum',
            'r7usdp':'sum',
        })
        df2['MAPE'] = abs(df2['r7usd'] - df2['r7usdp']) / df2['r7usd']
        print('过滤后MAPE:',df2['MAPE'].mean())

    campaignList = df['campaign_id'].unique()

    campaignIdList = []
    mapeList = []
    userCountMeanList = []
    r1usdMeanList = []
    userCountListList = []
    for userCount in userCountList:
        userCountListList.append([])
    r1usdListList = []
    for r1usd in r1usdList:
        r1usdListList.append([])

    for campaign in campaignList:
        campaignDf = df.loc[df['campaign_id'] == campaign]
        mape = campaignDf['MAPE'].mean()
        campaignIdList.append(campaign)
        mapeList.append(mape)
        userCountMeanList.append(campaignDf['user_count'].mean())
        r1usdMeanList.append(campaignDf['r1usd'].mean())
        
        for i in range(len(userCountList)):
            userCount = userCountList[i]
            userCountDf = campaignDf.loc[campaignDf['user_count'] > userCount]
            mape = userCountDf['MAPE'].mean()
            # print(f'过滤掉用户数小于{userCount}的数据后，影响MAPE：',mape)
            userCountListList[i].append(mape)
        
        for i in range(len(r1usdList)):
            r1usd = r1usdList[i]
            r1usdDf = campaignDf.loc[campaignDf['r1usd'] > r1usd]
            mape = r1usdDf['MAPE'].mean()
            # print(f'过滤掉r1usd小于{r1usd}的数据后，影响MAPE：',mape)
            r1usdListList[i].append(mape)
    
    retDf = pd.DataFrame({
        'campaign_id':campaignIdList,
        'mape':mapeList,
        'user_count_mean':userCountMeanList,
        'r1usd_mean':r1usdMeanList,
    })
    for i in range(len(userCountList)):
        userCount = userCountList[i]
        retDf[f'user_count_{userCount}'] = userCountListList[i]
    for i in range(len(r1usdList)):
        r1usd = r1usdList[i]
        retDf[f'r1usd_{r1usd}'] = r1usdListList[i]

    retDf = retDf.sort_values(by='mape',ascending=False)
    retDf.to_csv(getFilename('campaignMind1'))

def mind2(csvFile):
    df = pd.read_csv(csvFile)

    # 为了有效的计算MAPE，需要过滤掉r7usd为0的数据
    df = df.loc[df['r7usd'] > 0]

    df2 = df.groupby(by = ['install_date']).agg({
        'r7usd':'sum',
        'r7usdp':'sum',
    })
    df2['MAPE'] = abs(df2['r7usd'] - df2['r7usdp']) / df2['r7usd']
    print('原本MAPE:',df2['MAPE'].mean())

    df = df.sort_values(by='user_count',ascending=True)
    userCountList = df['user_count'].quantile([0.1,0.2,0.3,0.4,0.5]).tolist()
    print('用户数 10%分位数:',userCountList[0])
    print('用户数 20%分位数:',userCountList[1])
    print('用户数 30%分位数:',userCountList[2])
    print('用户数 40%分位数:',userCountList[3])
    print('用户数 50%分位数:',userCountList[4])

    df = df.sort_values(by='r1usd',ascending=True)
    r1usdList = df['r1usd'].quantile([0.1,0.2,0.3,0.4,0.5]).tolist()
    print('r1usd 10%分位数:',r1usdList[0])
    print('r1usd 20%分位数:',r1usdList[1])
    print('r1usd 30%分位数:',r1usdList[2])
    print('r1usd 40%分位数:',r1usdList[3])
    print('r1usd 50%分位数:',r1usdList[4])


    for userCount in userCountList:
        for r1usd in r1usdList:
            condition = (df['user_count'] > userCount) & (df['r1usd'] > r1usd)
            print(f'过滤掉用户数小于{userCount} 并且 首日付费金额小于{r1usd} 的数据后，影响用户比例：',(df.loc[~condition]['user_count'].sum() / df['user_count'].sum()))
            print(f'过滤掉用户数小于{userCount} 并且 首日付费金额小于{r1usd} 的数据后，影响首日收入比例：',(df.loc[~condition]['r1usd'].sum() / df['r1usd'].sum()))

            df2 = df.loc[condition].copy()
            df2 = df2.groupby(by = ['install_date']).agg({
                'r7usd':'sum',
                'r7usdp':'sum',
            })
            df2['MAPE'] = abs(df2['r7usd'] - df2['r7usdp']) / df2['r7usd']
            print('过滤后MAPE:',df2['MAPE'].mean())


    campaignList = df['campaign_id'].unique()

    campaignIdList = []
    mapeList = []
    userCountMeanList = []
    r1usdMeanList = []
    
    mapeList2 = []
    for userCount in userCountList:
        for r1usd in r1usdList:
            mapeList2.append([])
    print('len(mapeList2):',len(mapeList2))

    # 索引
    a = np.arange(len(userCountList)*len(r1usdList)).reshape(len(userCountList),len(r1usdList))
    aList = a.tolist()
    print('aList:',aList)

    for campaign in campaignList:
        campaignDf = df.loc[df['campaign_id'] == campaign]
        mape = campaignDf['MAPE'].mean()
        campaignIdList.append(campaign)
        mapeList.append(mape)
        userCountMeanList.append(campaignDf['user_count'].mean())
        r1usdMeanList.append(campaignDf['r1usd'].mean())
        
        for i in range(len(userCountList)):
            for j in range(len(r1usdList)):
                userCount = userCountList[i]
                r1usd = r1usdList[j]
                filtedDf = campaignDf.loc[(campaignDf['user_count'] > userCount) & (campaignDf['r1usd'] > r1usd)]
                mape = filtedDf['MAPE'].mean()
                mapeList2[aList[i][j]].append(mape)

    print('len(campaignIdList):',len(campaignIdList))
    retDf = pd.DataFrame({
        'campaign_id':campaignIdList,
        'mape':mapeList,
        'user_count_mean':userCountMeanList,
        'r1usd_mean':r1usdMeanList,
    })
    for i in range(len(userCountList)):
        for j in range(len(r1usdList)):
            retDf[f'mape_{userCountList[i]}_{r1usdList[j]}'] = mapeList2[aList[i][j]]    

    retDf = retDf.sort_values(by='mape',ascending=False)
    retDf.to_csv(getFilename('campaignMind2'))

# 不再计算MAPE，而是计算相关系数
def mind3(csvFile):
    df = pd.read_csv(csvFile)
    df = df.sort_values(by=['campaign_id','install_date'],ascending=True).reset_index(drop=True)

    campaignList = df['campaign_id'].unique()
    campaignIdList = []
    corrList = []
    corr7List = []
    for campaign in campaignList:
        campaignDf = df.loc[df['campaign_id'] == campaign].copy()
        corr = campaignDf.corr()['r7usd']['r7usdp']
        campaignIdList.append(campaign)
        corrList.append(corr)
        campaignDf['r7usd rolling7'] = campaignDf['r7usd'].rolling(7).mean()
        campaignDf['r7usdp rolling7'] = campaignDf['r7usdp'].rolling(7).mean()
        corr7 = campaignDf.corr()['r7usd rolling7']['r7usdp rolling7']
        corr7List.append(corr7)
    retDf = pd.DataFrame({
        'campaign_id':campaignIdList,
        'pearson':corrList,
        'pearson7':corr7List,
    })
    retDf['pearson_square'] = retDf['pearson'] ** 2
    retDf['pearson7_square'] = retDf['pearson7'] ** 2

    retDf.to_csv(getFilename('campaignMind3'))

def mind4(csvFile):
    df = pd.read_csv(csvFile)

    df = df.sort_values(by='user_count',ascending=True)
    userCountList = df['user_count'].quantile([0.1,0.2,0.3,0.4,0.5]).tolist()
    print('用户数 10%分位数:',userCountList[0])
    print('用户数 20%分位数:',userCountList[1])
    print('用户数 30%分位数:',userCountList[2])
    print('用户数 40%分位数:',userCountList[3])
    print('用户数 50%分位数:',userCountList[4])

    df = df.sort_values(by='r1usd',ascending=True)
    r1usdList = df['r1usd'].quantile([0.1,0.2,0.3,0.4,0.5]).tolist()
    print('r1usd 10%分位数:',r1usdList[0])
    print('r1usd 20%分位数:',r1usdList[1])
    print('r1usd 30%分位数:',r1usdList[2])
    print('r1usd 40%分位数:',r1usdList[3])
    print('r1usd 50%分位数:',r1usdList[4])

    campaignList = df['campaign_id'].unique()
    campaignIdList = []
    corrList = []

    # 索引
    a = np.arange(len(userCountList)*len(r1usdList)).reshape(len(userCountList),len(r1usdList))
    aList = a.tolist()
    print('aList:',aList)

    corrList2 = []
    for userCount in userCountList:
        for r1usd in r1usdList:
            corrList2.append([])

    for campaign in campaignList:
        campaignDf = df.loc[df['campaign_id'] == campaign]
        corr = campaignDf.corr()['r7usd']['r7usdp']
        campaignIdList.append(campaign)
        corrList.append(corr)
        for i in range(len(userCountList)):
            for j in range(len(r1usdList)):
                userCount = userCountList[i]
                r1usd = r1usdList[j]
                filtedDf = campaignDf.loc[(campaignDf['user_count'] > userCount) & (campaignDf['r1usd'] > r1usd)]
                corr = filtedDf.corr()['r7usd']['r7usdp']
                corrList2[aList[i][j]].append(corr)
               
    retDf = pd.DataFrame({
        'campaign_id':campaignIdList,
        'pearson':corrList,
    })
    retDf['pearson_square'] = retDf['pearson'] ** 2

    for i in range(len(userCountList)):
        for j in range(len(r1usdList)):
            retDf[f'corr_%d_%.0f'%(userCountList[i],r1usdList[j])] = corrList2[aList[i][j]]  


    retDf.to_csv(getFilename('campaignMind4'))


def debug():
    df = pd.read_csv(getFilename('campaignMind1'))
    # 

if __name__ == '__main__':
    # mind1()
    # mind2(getFilename('attCampaign24_attribution24RetCheck'))
    # mind2(getFilename('attCampaign48_attribution24RetCheck'))
    mind3(getFilename('attCampaign24_attribution24RetCheck'))
    # mind4(getFilename('attCampaign24_attribution24RetCheck'))
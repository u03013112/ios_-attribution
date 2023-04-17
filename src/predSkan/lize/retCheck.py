# 计算按照预测方案+分组之后的首日和7日收入美元金额的MAPE

# 暂时参考数据来自rg_ai_bj.ads_userfeature_v2_predictvalue_wx_cn_day1to7，rg_ai_bj.ads_train_needpredictusers_cnwx 两个表，应该是国内微信版本
# 大体思路如下：
# 1、针对此部分用户，计算64个档位的分档方案，以及首日与7日的大盘MAPE
# 2、针对此部分用户，计算预测结果的MAPE
# 3、针对此部分用户，计算预测结果+分档后的7日大盘MAPE

import pandas as pd

import sys
sys.path.append('/src')
from src.maxCompute import execSqlBj
from src.tools import getFilename


# 获得数据 写sql
# 数据来源表1 rg_ai_bj.ads_userfeature_v2_predictvalue_wx_cn_day1to7 as a，拥有3列：uid，predict_value，dt
# 数据来源表2 rg_ai_bj.ads_train_needpredictusers_cnwx as b，列：uid	platform	country	idfa	gaid	firebaseid	afid	model	sys	hour24price	hour48price	hour72price	hour96price	hour120price	hour144price	hour168price	ip_country	ip_city	regtime	ad_mediasource	ad_advertiser_id	ad_campaign_id	ad_adset_id	ad_creative_id	ad_inventory	ad_link_version	d0	d1	d2	d3	d4	d5	d6	hour1price	hour2price	hour3price	hour4price	hour5price	hour6price	hour7price	hour8price	hour9price	hour10price	hour11price	hour12price	hour13price	hour14price	hour15price	hour16price	hour17price	hour18price	hour19price	hour20price	hour21price	hour22price	hour23price	ua	device_type	asset	param1	param2	param3	param4	param5	dt。
# 使用`INNER JOIN`连接两个表，连接条件是`a.uid = b.uid`
# 需要列a.uid,a.predict_value,b.hour24price,b.hour168price,b.device_type,b.dt
# dt过滤 20230101-20230401

def getDataFromMC():
    sql = '''
        SELECT
            a.uid,
            a.predict_value,
            b.hour24price,
            b.hour168price,
            b.device_type,
            b.dt
        FROM
            rg_ai_bj.ads_userfeature_v2_predictvalue_wx_cn_day1to7 AS a
            INNER JOIN rg_ai_bj.ads_train_needpredictusers_cnwx AS b ON a.uid = b.uid
        WHERE
            b.dt >= '20230101'
            AND b.dt <= '20230401'
            AND a.dt >= '20230101'
            AND a.dt <= '20230401'
    '''
    df = execSqlBj(sql)
    return df

def saveData():
    df = getDataFromMC()
    df.to_csv(getFilename('retCheckData_20230101_20230401'))

def loadData():
    df = pd.read_csv(getFilename('retCheckData_20230101_20230401'))
    return df

def makeLevels1(userDf, usd='hour24price', N=8):
    # `makeLevels1`函数接受一个包含用户数据的DataFrame（`userDf`），一个表示用户收入的列名（`usd`，默认为'r1usd'），以及分组的数量（`N`，默认为8）。
    # 其中第0组特殊处理，第0组是收入等于0的用户。
    # 过滤收入大于0的用户进行后续分组，分为N-1组，每组的总收入大致相等。
    # 根据收入列（`usd`）对用户DataFrame（`userDf`）进行排序。
    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值。
    # 计算所有这些用户的总收入。
    # 计算每组的目标收入（总收入除以分组数量）。
    # 初始化当前收入（`current_usd`）和组索引（`group_index`）。
    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入。然后，将该用户的收入值存储到`levels`数组中，并将当前收入重置为0，组索引加1。当组索引达到N-1时，停止遍历。
    # 返回`levels`数组。
    
    # 过滤收入大于0的用户
    filtered_df = userDf[userDf[usd] > 0]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df[usd].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N)

    # 初始化当前收入（`current_usd`）和组索引（`group_index`）
    current_usd = 0
    group_index = 0

    # 遍历过滤后的用户DataFrame，将用户的收入累加到当前收入，直到达到目标收入
    for index, row in df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            # 将该用户的收入值存储到`levels`数组中
            levels[group_index] = row[usd]
            # 将当前收入重置为0，组索引加1
            current_usd = 0
            group_index += 1
            # 当组索引达到N-1时，停止遍历
            if group_index == N - 1:
                break

    return levels

# 用level生成cvMap，包含cv，min_event_revenue，max_event_revenue和avg字段
def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0],
        'avg':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        min = mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1]
        max = levels[i]
        mapData['min_event_revenue'].append(min)
        mapData['max_event_revenue'].append(max)
        mapData['avg'].append((min+max)/2)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def cvMapFixAvg1(userDf,cvMapDf,usd='hour24price'):
    min = cvMapDf['min_event_revenue'][1]
    max = cvMapDf['max_event_revenue'][1]
    cv1UserDf = userDf[(userDf[usd]>min) & (userDf[usd]<=max)]
    cvMapDf.at[1, 'avg'] = cv1UserDf[usd].mean()
    return cvMapDf

def addCv(userDf,cvMapDf,usd='hour24price',cv='cv1',usdp='hour24priceP'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf['cv'].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        avg = cvMapDf['avg'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = cv1
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),usdp
        ] = avg
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = cv1
    userDfCopy.loc[userDfCopy[usd]>max,usdp] = avg

    return userDfCopy[['uid',cv,usdp]]

from sklearn.metrics import r2_score,mean_absolute_percentage_error
def check(userDf,usd='hour24price',usdp='hour24priceP'):
    # 按dt进行分组
    groupDf = userDf.groupby('dt').agg({usd:'sum',usdp:'sum'})
    # groupDf.to_csv(getFilename('group1'))
    # 计算usd与usdp的MAPE与R2
    mape = mean_absolute_percentage_error(groupDf[usd],groupDf[usdp])
    r2 = r2_score(groupDf[usd],groupDf[usdp])
    return mape,r2

if __name__ == '__main__':
    # saveData()
    # df = loadData()
    # # 将重复的uid行去掉
    # df = df.drop_duplicates(subset=['uid'])

    # levels = makeLevels1(df)
    # print(levels)
    # cvMapDf = makeCvMap(levels)
    # print(cvMapDf)
    # cvMapDf = cvMapFixAvg1(df,cvMapDf)
    # print(cvMapDf)
    # cvMapDf.to_csv(getFilename('cvMapRetCheckData_20230101_20230401'))
    # tmpDf = addCv(df,cvMapDf)
    # df = df.merge(tmpDf,how='left',on='uid')
    # df.to_csv(getFilename('retCheckData_cv1_20230101_20230401'),index=False)

    # df = pd.read_csv(getFilename('retCheckData_cv1_20230101_20230401'))

    # tmpDf = pd.DataFrame()
    # for cv1 in df['cv1'].unique():
    #     print('cv1:',cv1)
    #     N = 8
    #     if cv1 > 0:
    #         N = 9
    #     cv1Df = df[df['cv1']==cv1]
    #     levelsCv1 = makeLevels1(cv1Df,usd='hour168price',N=N)
    #     print(levelsCv1)
    #     cv1MapDf = makeCvMap(levelsCv1)
    #     print(cv1MapDf)
    #     cv1MapDf = cvMapFixAvg1(cv1Df,cv1MapDf,usd='hour168price')
    #     print(cv1MapDf)
    #     cv1MapDf.to_csv(getFilename('cvMapRetCheckData_cv1_'+str(int(cv1))+'_20230101_20230401'))

    #     tmpDf = tmpDf.append(addCv(cv1Df,cv1MapDf,cv='cv7',usd='hour168price',usdp='hour168priceP'))

    # df = df.merge(tmpDf,how='left',on='uid') 
    # df.to_csv(getFilename('retCheckData_cv1_cv7_20230101_20230401'),index=False)


    df = pd.read_csv(getFilename('retCheckData_cv1_cv7_20230101_20230401'))
    df['predict_value'] += df['hour24price']

    # # 计算只分64组的MAPE与R2
    # mape,r2 = check(df,usd='hour24price',usdp='hour24priceP')
    # print('hour24price mape:',mape,'r2:',r2)
    # mape,r2 = check(df,usd='hour168price',usdp='hour168priceP')
    # print('hour168price mape:',mape,'r2:',r2)
    
    # # 结论
    # # hour24price mape: 0.039739974824987395 r2: 0.9787564834097484
    # # hour168price mape: 0.04222958037112309 r2: 0.9689412863091535

    # # 计算预测结果的MAPE
    # mape,r2 = check(df,usd='hour168price',usdp='predict_value')
    # print('predict_value mape:',mape,'r2:',r2)
    # # predict_value mape: 0.23106358607064295 r2: 0.48760671290925406

    

    # tmpDf = pd.DataFrame()
    # for cv1 in df['cv1'].unique():
    #     print('cv1:',cv1)
    #     N = 8
    #     if cv1 > 0:
    #         N = 9
    #     cv1Df = df[df['cv1']==cv1]
        
    #     cv1MapDf = pd.read_csv(getFilename('cvMapRetCheckData_cv1_'+str(int(cv1))+'_20230101_20230401'))
    #     tmpDf = pd.concat([tmpDf, addCv(cv1Df,cv1MapDf,cv='cv7p',usd='predict_value',usdp='predict_value2')], ignore_index=True)

    # tmpDf.to_csv(getFilename('tmpDf'),index=False)

    tmpDf = pd.read_csv(getFilename('tmpDf'))
    

    print('start merge')
    df = df.merge(tmpDf,how='left',on='uid')
    df.to_csv(getFilename('111'),index=False)
    mape,r2 = check(df,usd='hour168price',usdp='predict_value2')
    print('predict_value mape:',mape,'r2:',r2)
    # predict_value mape: 0.2144434505349273 r2: 0.5499838227394538

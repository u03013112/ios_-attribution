# 付费档位计算
import datetime
import pandas as pd
import numpy as np

import sys
sys.path.append('/src')
from src.tools import getFilename
from src.maxCompute import execSql

# 获得指定时间范围内
# 具体的付费信息，应该是groupby uid的，每一行有付费次数、付费总金额 还有安装日期
# 暂时先这样统计就好
# 过滤了媒体为FB，并且国家限定
def getDataFromMaxCompute(sinceTimeStr,unitlTimeStr):
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    
    sinceTimeStr2 = sinceTime.strftime("%Y-%m-%d")
    unitlTimeStr2 = unitlTime.strftime("%Y-%m-%d") + ' 23:59:59'
    # 为了获得完整的7日回收，需要往后延长7天
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=7)).strftime('%Y%m%d')

    sql='''
        select
            customer_user_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                    when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (1 as double)
                    else 0
                end
            ) as r7count,
            sum(
                case
                    when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r7usd
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= % s
            and day <= % s
            and install_time >= "%s"
            and install_time <= "%s"
            and country_code in ("US","CA","AU","GB","UK","NZ","DE","FR","KR")
        group by
            install_date,
            customer_user_id
    '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    print(sql)
    # return
    pd_df = execSql(sql)
    return pd_df

# 输入原本的数据，用新的步长来做新的r7count
def getNewEventCount(dataFrame,stepDetaUsd):
    r7usd = dataFrame['r7usd']
    r7count = np.ceil(r7usd/stepDetaUsd)
    return pd.DataFrame({
        'r7usd':r7usd,
        'r7count':r7count
    })

# 获得每个事件的回收
# 返回数据中，不同转化数量的人，平均每个转化的回收价值
# 比如只转化1次的人，平均转化价值是多少
# maxLevel 是最高采用什么档位，这个和ads有关，fb暂时未找到文档，暂时默认40吧
def getRevenuePerEvent(dataFrame,maxLevel=40):
    user_count = []
    avg = []
    count_sum = []
    usd_sum = []

    dataFrame.loc[dataFrame.r7count > maxLevel,'r7count'] = maxLevel

    # 总体
    user_count.append(len(dataFrame))
    countSum = dataFrame['r7count'].sum()
    usdSum = dataFrame['r7usd'].sum()
    count_sum.append(countSum)
    usd_sum.append(usdSum)
    avg.append(usdSum/countSum)

    for count in range(1,maxLevel+1):
        df = dataFrame.loc[dataFrame.r7count == count]
        user_count.append(len(df))
        countSum = df['r7count'].sum()
        usdSum = df['r7usd'].sum()
        count_sum.append(countSum)
        usd_sum.append(usdSum)
        if countSum > 0:
            avg.append(usdSum/countSum)
        else:
            avg.append(0)
        

    return pd.DataFrame(data = {
        'event_count':range(41),
        'user_count':user_count,
        'count_sum':count_sum,
        'usd_sum':usd_sum,
        'avg':avg,
    })

# 整体分析，输入一个df，要求里面有r7count和r7usd两列，一行代表一个用户
# stepDetaUsd 是金额步长
def analyze(dataFrame,stepDetaUsd = 0,maxLevel=40,maxUsd=400,minUsd=0):

    dataFrameCopy = dataFrame.copy()
    minLossDf = dataFrameCopy.loc[dataFrameCopy.r7usd <= minUsd]
    dataFrameCopy = dataFrameCopy.loc[dataFrameCopy.r7usd > minUsd]
    dataFrameCopy.loc[dataFrameCopy.r7usd > maxUsd,'r7usd'] = maxUsd

    if stepDetaUsd > 0:
        r7usd = dataFrameCopy['r7usd']
        r7count = np.ceil(r7usd/stepDetaUsd)
        dataFrameCopy = pd.DataFrame({
            'r7usd':r7usd,
            'r7count':r7count
        })

    user_count = []
    avg = []
    count_sum = []
    usd_sum = []

    # 将超过的抹平
    dataFrameCopy.loc[dataFrameCopy.r7count > maxLevel,'r7count'] = maxLevel

    # 总体，用这个值做平均值，然后其他值基于这个做计算，比如mae，mse
    countSumTotal = dataFrameCopy['r7count'].sum()
    usdSumTotal = dataFrameCopy['r7usd'].sum()
    userCountTotal = len(dataFrameCopy)
    userMinLossTotal = len(minLossDf)
    # 这是每个转化带来价值
    totalAvg = usdSumTotal/countSumTotal

    lastCount = 0
    lastUsd = 0
    lastUserCount = 0

    for count in range(1,maxLevel+1):
        df = dataFrameCopy.loc[dataFrameCopy.r7count == count]
        user_count.append(len(df))
        countSum = df['r7count'].sum()
        usdSum = df['r7usd'].sum()
        count_sum.append(countSum)
        usd_sum.append(usdSum)
        if countSum > 0:
            avg.append(usdSum/countSum)
        else:
            avg.append(0)
        if count == maxLevel:
            lastCount = countSum
            lastUsd = usdSum
            lastUserCount = len(df)

    filename = getFilename('se%d_%d_%d_%d'%(stepDetaUsd,maxLevel,maxUsd,minUsd))
    pd.DataFrame(data = {
        'event_count':range(1,maxLevel+1),
        'user_count':user_count,
        'count_sum':count_sum,
        'usd_sum':usd_sum,
        'avg':avg,
    }).to_csv(filename)
    print('save ',filename)

    # 要返回所有档位针对总体平均值的差距
    yt = np.ones(maxLevel)*totalAvg
    yp = np.array(avg)
    # print(yt,yp)

    mse = np.mean(np.square(yt - yp))
    rmse = np.sqrt(np.mean(np.square(yt - yp)))
    mae = np.mean(np.abs(yt-yp))
    mape = np.mean(np.abs((yt - yp) / yt)) * 100

    yt = yt[1:]
    yp = yp[1:]
    mse2 = np.mean(np.square(yt - yp))
    rmse2 = np.sqrt(np.mean(np.square(yt - yp)))
    mae2 = np.mean(np.abs(yt-yp))
    mape2 = np.mean(np.abs((yt - yp) / yt)) * 100

    return mse,rmse,mae,mape,mse2,rmse2,mae2,mape2,lastUserCount/userCountTotal,lastUsd/usdSumTotal,userMinLossTotal/(userMinLossTotal+userCountTotal)


def main():
    # df = getDataFromMaxCompute('20221001','20221031')
    # df.to_csv(getFilename('se_20221001_20221031'))
    df = pd.read_csv(getFilename('se_20221001_20221031'))

    stepList = []
    mseList = []
    rmseList = []
    maeList = []
    mapeList = []
    mape2List = []
    maxLevelList = []
    maxUsdList = []
    minUsdList = []
    userLossList = []
    usdLossList = []
    userMinLossList = []
    # for maxLevel in [20,30,40]:
    #     for maxUsd in [200,400,600,800]:
    for maxLevel in [40]:
        for maxUsd in [800]:
            for minUsd in [1,2,3,4,5]:
                for s in range(30):
                    mse,rmse,mae,mape,mse2,rmse2,mae2,mape2,userLoss,usdLoss,userMinLoss = analyze(df,s,maxLevel,maxUsd,minUsd)
                    stepList.append(s)
                    mseList.append(mse)
                    rmseList.append(rmse)
                    maeList.append(mae)
                    mapeList.append(mape)
                    mape2List.append(mape2)
                    maxLevelList.append(maxLevel)
                    maxUsdList.append(maxUsd)
                    minUsdList.append(minUsd)
                    userLossList.append(userLoss)
                    usdLossList.append(usdLoss)
                    userMinLossList.append(userMinLoss)
        
    return pd.DataFrame(data={
        'maxLevel':maxLevelList,
        'maxUsd':maxUsdList,
        'minUsd':minUsdList,
        'step': stepList,
        # 'mse':mseList,
        # 'rmse':rmseList,
        # 'mae':maeList,
        'mape':mapeList,
        # 'mape2':mape2List
        'userLoss':userLossList,
        'usdLoss':usdLossList,
        'userMinLoss':userMinLossList
    })

if __name__ == '__main__':    
    df = main()
    df.to_csv(getFilename('se_ret'))
    df2 = df.loc[df.groupby(['maxLevel','maxUsd']).mape.idxmin()].reset_index(drop=True)
    df2.to_csv(getFilename('se_ret2'))
    

    # 每一种方案都应该有一个损失金额，金额是超出最大档位的金额，但是未到金额限定上限的金额
    # 对应的还应该有一个受影响大R人数
    
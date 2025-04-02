# 为了FB做64档CV
import sys
sys.path.append('/src')

from src.maxCompute import execSql
from src.tools.cvTools import makeLevels,makeLevelsByJenkspy,makeLevelsByKMeans

import datetime
import pandas as pd

def getPayDataFromMC():
    todayStr = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    oneMonthAgoStr = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')
    print('获得%s~%s的付费数据'%(oneMonthAgoStr,todayStr))

    sql = f'''
        select
            install_day AS install_date,
            game_uid as uid,
            sum(
                case
                    when event_time - install_timestamp <= 24 * 3600 then revenue_value_usd
                    else 0
                end
            ) as revenue
        from
            dwd_overseas_revenue_allproject
        where
            app = 502
            and zone = 0
            and day between {oneMonthAgoStr} and {todayStr}
            and app_package = 'id6448786147'
        group by
            install_day,
            game_uid
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

# 这部分代码算是通用代码，直接抄过来

def makeLevels1(userDf, usd='r1usd', N=32):    
    # 过滤收入大于0的用户
    filtered_df = userDf[userDf[usd] > 0]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df[usd].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N - 1)

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

    # levels 排重
    levels = list(set(levels))
    # levels 去掉0
    levels.remove(0)
    # levels 排序
    levels.sort()
    max = levels[len(levels)-1]
    # levels[N-2] = 1000
    return levels

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

    # 最后再加一档无上限
    # mapData['cv'].append(len(mapData['cv']))
    # mapData['min_event_revenue'].append(max)
    # mapData['max_event_revenue'].append(max)
    # mapData['avg'].append(max)

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def addCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    userDfCopy = userDf.copy(deep=True).reset_index(drop=True)
    for cv1 in cvMapDf[cv].values:
        min = cvMapDf['min_event_revenue'][cv1]
        max = cvMapDf['max_event_revenue'][cv1]
        userDfCopy.loc[
            (userDfCopy[usd]>min) & (userDfCopy[usd]<=max),cv
        ] = int(cv1)
    # 将userDfCopy[usd]>max的用户的cv1和max设置为最后一档
    userDfCopy.loc[userDfCopy[usd]>max,cv] = int(cv1)
    return userDfCopy

def checkCv(userDf,cvMapDf,usd='r1usd',cv='cv'):
    addCvDf = addCv(userDf,cvMapDf,usd,cv)
    df = addCvDf.merge(cvMapDf,on=[cv],how='left')
    
    # tmpDf = df.groupby([cv]).agg({usd:'sum','avg':'sum'}).reset_index()
    # tmpDf['usd/usdSum'] = tmpDf[usd]/tmpDf[usd].sum()
    # tmpDf['avg/avgSum'] = tmpDf['avg']/tmpDf['avg'].sum()
    # print(tmpDf)

    df = df.groupby(['install_date']).agg({usd:'sum','avg':'sum'}).reset_index()
    df['mape'] = abs(df[usd] - df['avg']) / df[usd]
    df = df.dropna()
    # print('mape:',df['mape'].mean())
    return df['mape'].mean(),df[['install_date','mape']]

def checkLevels(df,levels,usd='payUsd',cv='cv'):
    cvMapDf = makeCvMap(levels)

    return checkCv(df,cvMapDf,usd=usd,cv=cv)


from src.report.feishu.feishu import sendMessageDebug
def main():
    df = getPayDataFromMC()
    df.to_csv('/src/data/payData.csv',index=False)
    df = pd.read_csv('/src/data/payData.csv')


    for N in (16,32,64):
        message = f'N={N}\n\n'
        levels = makeLevels(df,usd='revenue',N=N)
        levels = [round(x,2) for x in levels]

        cvMapDf = makeCvMap(levels)
        cvMapDf.to_csv(f'/src/data/cvMapDf{N}_1.csv',index=False)

        mape,mapeDf = checkLevels(df,levels,usd='revenue',cv='cv')
        mapeDf.to_csv(f'/src/data/mapeDf{N}_1.csv',index=False)
        message += 'makeLevels\n'
        message += f'{levels}\n'
        message += f'{mape*100:.2f}%\n\n'

        levels = makeLevelsByKMeans(df,usd='revenue',N=N)
        levels = [round(x,2) for x in levels]

        cvMapDf = makeCvMap(levels)
        cvMapDf.to_csv(f'/src/data/cvMapDf{N}_2.csv',index=False)

        mape,mapeDf = checkLevels(df,levels,usd='revenue',cv='cv')
        mapeDf.to_csv(f'/src/data/mapeDf{N}_2.csv',index=False)
        message += 'makeLevelsByKMeans\n'
        message += f'{levels}\n'
        message += f'{mape*100:.2f}%\n\n'
        
        print(message)
    # sendMessageDebug(message)

if __name__ == '__main__':
    main()

    # levels = [1.01, 2.09, 3.05, 3.96, 5.98, 8.09, 10.68, 13.1, 15.74, 17.94, 20.82, 23.5, 27.28, 31.79, 35.59, 39.57, 45.29, 51.6, 57.91, 64.04, 70.56, 77.54, 85.34, 93.78, 102.85, 110.55, 120.56, 133.16, 146.3, 159.41, 175.59, 190.67, 207.42, 221.2, 235.69, 251.68, 267.3, 286.39, 314.37, 343.45, 372.85, 401.52, 427.41, 456.57, 500.49, 544.45, 576.01, 615.13, 658.09, 698.14, 726.09, 794.38, 851.99, 896.61, 938.36, 1007.96, 1080.38, 1201.19, 1260.38, 1491.48, 1601.03, 2155.46, 7698.85]
    # cvMapDf = makeCvMap(levels)
    # cvMapDf.to_csv('/src/data/cvMapDf.csv',index=False)
    

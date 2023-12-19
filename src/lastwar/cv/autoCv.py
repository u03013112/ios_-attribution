# 定期的计算，最近一个月的cv值，是否比之前的版本有长足进步，如果有，就保存当前版本，并通知管理员
# 每周一早上10点，执行一次
# 10 10 * * 1 docker exec -t ios_attribution python /src/src/lastwar/cv/autoCv.py
import sys
sys.path.append('/src')

from src.maxCompute import execSql

import datetime
import pandas as pd

def getPayDataFromMC():
    todayStr = datetime.datetime.now().strftime('%Y%m%d')
    oneMonthAgoStr = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')
    print('获得%s~%s的付费数据'%(oneMonthAgoStr,todayStr))

    sql = f'''
        select
            SUBSTRING(install_time, 1, 10) AS install_date,
            appsflyer_id,
            SUM(event_revenue_usd) as revenue
        from
            ods_platform_appsflyer_events
        where
            app = '502'
            AND zone = '0'
            AND event_name IN ('af_purchase', 'af_purchase_oldusers')
            AND event_timestamp - install_timestamp BETWEEN 0 AND 86400
            AND day BETWEEN {oneMonthAgoStr} AND {todayStr}
        group by
            install_date,
            appsflyer_id
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
    print('mape:',df['mape'].mean())
    return df['mape'].mean()

from src.report.feishu.feishu import sendMessageDebug
def main():
    # df = getPayDataFromMC()
    # df.to_csv('/src/data/payData.csv',index=False)
    df = pd.read_csv('/src/data/payData.csv')
    levels = makeLevels1(df,usd='revenue',N=33)
    cvMapDf = makeCvMap(levels)
    print(cvMapDf)
    mape = checkCv(df,cvMapDf,usd='revenue')

    # 计算旧版本的Mape
    cvMapDfOld = pd.read_csv('/src/src/lastwar/cv/cvMap20231205.csv')
    cvMapDfOld = cvMapDfOld.loc[cvMapDfOld['conversion_value']<32][['conversion_value','min_event_revenue','max_event_revenue']].fillna(0)
    cvMapDfOld['avg'] = (cvMapDfOld['min_event_revenue'] + cvMapDfOld['max_event_revenue'])/2
    print(cvMapDfOld)
    mapeOld = checkCv(df,cvMapDfOld,usd='revenue',cv='conversion_value')

    # 当前版本的Mape比旧版本的Mape小超过1%，就通知管理员
    if mape < mapeOld - 0.01:
        message = '当前版本的Mape比旧版本的Mape超过1%，请检查\n'
        message += '当前版本的Mape为%f\n'%mape
        message += '旧版本的Mape为%f\n'%mapeOld
        message += cvMapDf.to_string()

        sendMessageDebug(message)
    else:
        message = 'cv 自动计算任务完成\n'
        message += '当前版本的Mape为%f\n'%mape
        message += '旧版本的Mape为%f\n'%mapeOld
        message += cvMapDf.to_string()
        
        sendMessageDebug(message)




if __name__ == '__main__':
    main()
    

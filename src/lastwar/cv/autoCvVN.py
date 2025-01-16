# 越南版本，由于目前没有数据，所以用T3国家的数据代替
# 越南版本也没有SSOT，所以是64档

import sys
sys.path.append('/src')

from src.maxCompute import execSql
from src.tools.cvTools import makeLevels,makeLevelsByJenkspy,makeLevelsByKMeans,checkLevels

import datetime
import pandas as pd

def getPayDataFromMC():
    todayStr = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    oneMonthAgoStr = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')
    print('获得%s~%s的付费数据'%(oneMonthAgoStr,todayStr))

    t2CountryList = ['BG','BY','EE','ES','GL','GR','HU','ID','KZ','LT','LV','MA','MY','PH','PL','RO','RS','SI','SK','TH','TM','TR','UZ']
    t3CountryList = ['BO','GT','IQ','UY','CZ','IN','BR','IM','MD','HR','PA','CL','MX','ME','GG','FO','PE','JE','PY','SR','UA','CO','AL','SM','AR','EG','DZ','MT','CR','EC','MK','BA','GI','XK','VN']

    t3CountryList += t2CountryList
    
    sql = f'''
        select
            install_day AS install_date,
            game_uid as uid,
            sum(
                case
                    when (event_time - install_timestamp) between 0 and 24 * 3600 then revenue_value_usd
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
            and country in {tuple(t3CountryList)}
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
    print('mape:',df['mape'].mean())
    return df['mape'].mean()

from src.report.feishu.feishu import sendMessageDebug
def main():

    N = 64

    df = getPayDataFromMC()
    df.to_csv('/src/data/payData.csv',index=False)
    df = pd.read_csv('/src/data/payData.csv')

    # # 将收入3000以上的用户的收入设置为3000
    # df.loc[df['revenue']>3000,'revenue'] = 3000

    message = 'Lastwar NV CV档位自动测试\n\n'

    # # 计算旧版本的Mape
    # cvMapDf = pd.read_csv('/src/src/lastwar/cv/cvMap20240708.csv')
    # # cvMapDf = cvMapDf.loc[
    # #     (cvMapDf['event_name'] == 'af_skad_revenue')
    # #     & (cvMapDf['conversion_value'] < 32)
    # # ]
    # cvMapDf = cvMapDf.loc[cvMapDf['event_name'] == 'af_purchase_update_skan_on']
    
    # levels = cvMapDf['max_event_revenue'].tolist()
    # mape = checkLevels(df,levels,usd='revenue',cv='cv')
    # message += '旧版本\n'
    # message += f'{levels}\n'
    # message += f'{mape*100:.2f}%\n\n'

    # levels = makeLevels1(df,usd='revenue',N=33)
    # levels = [round(x,2) for x in levels]
    # mape = checkLevels(df,levels,usd='revenue',cv='cv')
    # message += 'makeLevels1\n'
    # message += f'{levels}\n'
    # message += f'{mape*100:.2f}%\n\n'

    levels = makeLevels(df,usd='revenue',N=N)
    levels = [round(x,2) for x in levels]
    cvMapDf = makeCvMap(levels)
    cvMapDf.to_csv('/src/data/lastwarNV_CV1.csv',index=False)
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    message += 'makeLevels\n'
    message += f'{levels}\n'
    message += f'{mape*100:.2f}%\n\n'

    # levels = makeLevelsByJenkspy(df,usd='revenue',N=32)
    # levels = [round(x,2) for x in levels]
    # mape = checkLevels(df,levels,usd='revenue',cv='cv')
    # message += 'makeLevelsByJenkspy\n'
    # message += f'{levels}\n'
    # message += f'{mape*100:.2f}%\n\n'

    levels = makeLevelsByKMeans(df,usd='revenue',N=N)
    levels = [round(x,2) for x in levels]
    cvMapDf = makeCvMap(levels)
    cvMapDf.to_csv('/src/data/lastwarNV_CV2.csv',index=False)
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    message += 'makeLevelsByKMeans\n'
    message += f'{levels}\n'
    message += f'{mape*100:.2f}%\n\n'
    
    print(message)
    sendMessageDebug(message)


def debug():
    df = pd.read_csv('/src/data/payData.csv')
    df = df.groupby(['install_date']).agg({'revenue':'sum'}).reset_index()
    print(df)

if __name__ == '__main__':
    main()
    # debug()
    

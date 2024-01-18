import pandas as pd
import numpy as np
# makeLevels1
# 计算档位的核心算法，推荐使用makeLevels
# 传入参数：
# userDf iOS平台一段时间的用户收入数据，要求至少有一列，是指定时间内（比如24小时内）的收入金额，必须全部是美金，且是float类型
# 注意：userDf是按照用户汇总的，每个用户一行。
# usd 指定时间内的收入金额列名
# N 档位数量
# 返回值
# levels 档位数组，是一个长度为N-1的数组，每个元素是一个档位的最大收入值
# 注意：levels是有可能比N-1短的，即一个小范围的付费金额过高了，无法有效的分开，导致档位数量不足N-1
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
    
    return levels

# makeLevels
# 计算档位的核心算法
# 基本与makeLevels1改进版
def makeLevels(userDf, usd='r1usd', N=32):
    # 如果userDf没有sumUsd列，就添加sumUsd列，值为usd列的值
    if 'sumUsd' not in userDf.columns:
        userDf['sumUsd'] = userDf[usd]
    
    # 进行汇总，如果输入的数据已经汇总过，那就再做一次，效率影响不大
    userDf = userDf.groupby([usd]).agg({'sumUsd':'sum'}).reset_index()

    filtered_df = userDf[(userDf[usd] > 0) & (userDf['sumUsd'] > 0)]

    # 根据收入列（`usd`）对过滤后的用户DataFrame（`filtered_df`）进行排序
    df = filtered_df.sort_values([usd])

    # 初始化一个长度为N-1的数组（`levels`），用于存储每个分组的最大收入值
    levels = [0] * (N - 1)

    # 计算所有这些用户的总收入
    total_usd = df['sumUsd'].sum()

    # 计算每组的目标收入（总收入除以分组数量）
    target_usd = total_usd / (N)
    df['sum'] = df['sumUsd'].cumsum()
    
    
    for i in range(1,N):
        target = target_usd*(i)
        # 找到第一个大于target的行
        rows = df[df['sum']>=target]
        if len(rows) > 0:
            row = rows.iloc[0]
            levels[i-1] = row[usd]

    # levels 排重
    levels = list(set(levels))
    # levels 中如果有0，去掉0
    if 0 in levels:
        levels.remove(0)
    # levels 排序
    levels.sort()

    return levels

import jenkspy
def makeLevelsByJenkspy(userDf, usd='r1usd', N=32):
    filtered_df = userDf[userDf[usd] > 0]
    df = filtered_df.sort_values([usd])
    data = df[usd]
    levels = jenkspy.jenks_breaks(data, n_classes=N-2)

    return levels

from sklearn.cluster import KMeans
def makeLevelsByKMeans(userDf, usd='r1usd', N=32):
    filtered_df = userDf[userDf[usd] > 0]
    df = filtered_df.sort_values([usd])
    data = df[usd].values.reshape(-1, 1)
    kmeans = KMeans(n_clusters=N-1, random_state=0).fit(data)
    levels = sorted(np.unique(kmeans.cluster_centers_).tolist())

    return levels

# makeCvMap
# 根据档位数组，生成cvMap
# 输入参数：
# levels 是makeLevels1或者makeLevels2的返回值
# 返回值：
# cvMapDf 是一个DataFrame，有4列，分别是
# cv 档位
# min_event_revenue 档位的最小收入
# max_event_revenue 档位的最大收入
# avg 档位的平均收入
# 与AF的cvMap下载文件格式类似，可以兼容AF格式
def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-np.inf],
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

# addCv
# 为用户DataFrame添加cv列，验算的环节使用
# 输入参数：
# userDf 是用户DataFrame，要求至少有一列，是指定时间内（比如24小时内）的收入金额，必须全部是美金，且是float类型
# cvMapDf 是makeCvMap的返回值
# usd 指定时间内的收入金额列名
# cv cv列名
# install_date 安装日期列名
# 返回值：
# userDfCopy 是一个新的DataFrame，是userDf的深拷贝，但是多了一列cv
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

def checkCv(userDf,cvMapDf,usd='r1usd',count='count',cv='cv',install_date='install_date'):
    # 如果userDf没有count列，就添加count列，值为1
    if count not in userDf.columns:
        userDf[count] = 1
    
    # 进行汇总，如果输入的数据已经汇总过，那就再做一次，效率影响不大
    userDf = userDf.groupby([install_date,usd]).agg({count:'sum'}).reset_index()

    addCvDf = addCv(userDf,cvMapDf,usd,cv)
    df = addCvDf.merge(cvMapDf,on=[cv],how='left')
    df['sumUsd'] = df[usd] * df[count]
    df['sumAvg'] = df['avg'] * df[count]

    # 每个档位的真实花费占比，和cv均值花费占比，只打印到终端，看看就好
    tmpDf = df.groupby([cv]).agg({'sumUsd':'sum','sumAvg':'sum'}).reset_index()
    tmpDf['真实花费占比'] = tmpDf['sumUsd']/tmpDf['sumUsd'].sum()
    tmpDf['cv均值花费占比'] = tmpDf['sumAvg']/tmpDf['sumAvg'].sum()    
    print(tmpDf)

    df = df.groupby(['install_date']).agg({'sumUsd':'sum','sumAvg':'sum'}).reset_index()
    df['mape'] = abs(df['sumUsd'] - df['sumAvg']) / df['sumUsd']
    # print('mape:',df['mape'].mean())
    return df['mape'].mean()


def checkLevels(df,levels,usd='payUsd',cv='cv'):
    cvMapDf = makeCvMap(levels)
    return checkCv(df,cvMapDf,usd=usd,cv=cv)

def main1():
    # df = pd.read_csv('/src/data/zk2/lastwar20230920_20231019_allPay.csv')

    df = pd.read_csv('/src/data/lastwar_pay2_20230901_20231123.csv')
    N = 32

    levels = makeLevels1(df,usd='payUsd',N=N)
    print(len(levels))
    print(levels)
    print('makeLevels1')
    checkLevels(df,levels)

    levels = makeLevels(df,usd='payUsd',N=N)
    print(len(levels))
    print(levels)
    print('makeLevels')
    checkLevels(df,levels)

    levels = makeLevelsByJenkspy(df,usd='payUsd',N=N)
    print(len(levels))
    print(levels)
    print('makeLevelsByJenkspy')
    checkLevels(df,levels)

    levels = makeLevelsByKMeans(df,usd='payUsd',N=N)
    print(len(levels))
    print(levels)
    print('makeLevelsByKMeans')
    checkLevels(df,levels)

    # jenkspy
    # levels = [0.66, 2.13, 5.15, 9.53, 14.2, 19.77, 27.29, 36.1, 46.22, 59.36, 75.1, 93.18, 110.91, 129.65, 147.88, 175.86, 212.92, 247.59, 301.81, 390.58, 457.82, 515.11, 582.49, 638.44, 755.45, 814.48, 969.65, 1096.14, 1545.59, 1804.0, 4493.93]

    # k means
    # levels = [1.08460037730661, 3.161479396984968, 6.764175872735309, 10.063610223642176, 14.52798336798336, 21.528318739054267, 29.73640522875814, 39.9996638655462, 52.26362385321103, 67.03745945945944, 83.88968421052633, 103.012987012987, 122.11568627450978, 138.05823529411765, 159.94470588235293, 196.54270270270277, 232.364, 268.6894117647058, 314.0183333333333, 373.85900000000004, 451.40400000000005, 516.1366666666667, 574.3466666666667, 631.0966666666666, 741.8050000000001, 805.0699999999999, 969.65, 1089.555, 1545.59, 1804.0, 4493.93]

    # for i in range (N,100):
    #     levels = makeLevels1(df,usd='payUsd',N=i)
    #     if len(levels) >= N-1:
    #         print('N:',i,'levels:',len(levels))
    #         break
    
    # cvMapDf = makeCvMap(levels)
    # print(cvMapDf)
    # # # cvMapDf.to_csv('/src/data/zk2/lastwar20230901_20231123_allPay_cvMap.csv',index=False)
    # checkCv(df,cvMapDf,usd='payUsd',cv='cv')

if __name__ == '__main__':
    main1()

    
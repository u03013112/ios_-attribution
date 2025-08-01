import io
import os
import math
import datetime
import numpy as np
import pandas as pd

def makeLevels(userDf, usd='r1usd', N=32):
    userDf = userDf.copy()
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

# 针对越南等数据不足的地区使用的一套方法
def makeLevelsFix(userDf, usd='revenue', N=32):
    userDf = userDf.copy()

    # 如果userDf没有sumUsd列，就添加sumUsd列，值为usd列的值
    if 'sumUsd' not in userDf.columns:
        userDf['sumUsd'] = userDf[usd]

    userDf = userDf.groupby([usd]).agg({'sumUsd':'sum'}).reset_index()
    filtered_df = userDf[(userDf[usd] > 0) & (userDf['sumUsd'] > 0)]
    df = filtered_df.sort_values([usd])
    df['shifted'] = df['revenue'].shift(1, fill_value=0)

    # 计算总收入和目标每组收入
    total_usd = df['sumUsd'].sum()
    target_usd = total_usd / N
    print('total_usd:', total_usd, 'target_usd:', target_usd)

    levels = []
    cumulative_sum = 0

    for _, row in df.iterrows():
        cumulative_sum += row['sumUsd']
        # print('debug: cumulative_sum:', cumulative_sum, 'row[usd]:', row[usd])
        if cumulative_sum >= target_usd:
            # 超过目标，回退一行，前一行为边界
            levels.append(row['shifted'])
            # print('debug: 添加边界值:', row['shifted'])
            cumulative_sum = row['sumUsd']  # 当前行作为下一组的起始值
            
    # levels 中如果有0，去掉0
    if 0 in levels:
        levels.remove(0)

    # 去重、排序
    levels = sorted(set(levels))

    return levels

# 尝试将档位的diffUsdRatio最小化
def makeLevels2(userDf, usd='r1usd', N=32):
    userDf = userDf.copy()
    if 'sumUsd' not in userDf.columns:
        userDf['sumUsd'] = userDf[usd]

    userDf = userDf.groupby([usd]).agg({'uid': 'count', 'sumUsd': 'sum'}).reset_index()
    filtered_df = userDf[(userDf[usd] > 0)]
    df = filtered_df.sort_values([usd])

    # 手动做一个初始版本的levels
    revenueList = df[usd].tolist()
    revenueList = sorted(revenueList)
    revenueList = [math.ceil(revenue * 100) / 100 for revenue in revenueList]
    print('初始revenueList:', revenueList)
    levels = []
    for i in range(len(revenueList)-2):
        r1 = revenueList[i]
        r2 = revenueList[i+1]
        r3 = revenueList[i+2]
        r2_r1 = r2 - r1
        r3_r2 = r3 - r2
        if r2_r1 > r3_r2:
            levels.append(r2 - r3_r2/2)
            levels.append(r2 + r3_r2/2)
        else:
            levels.append(r2 - r2_r1/2)
            levels.append(r2 + r2_r1/2)
        
    levels = sorted(set(levels))  # 去重并排序
    d0 = levels[0] - revenueList[0]
    levels.insert(0, revenueList[0] - d0)  # 在最前面插入一个小于第一个档位的值
    d1 = revenueList[-1] - levels[-1]
    levels.append(revenueList[-1] + d1)  # 在最后面插入一个大于最后一个档位的值
    levels = [math.ceil(level * 100) / 100 for level in levels]
    # print('初始levels:', levels)
    
    # 计算按照目前levels的临近组合并后的diffUsdRatio
    def func1(df,levels):
        df0 = df.copy()
        cvMapDf = makeCvMap(levels)
        cvMapDf = cvMapDf[cvMapDf['cv'] > 0]
        cvMapDf['next_max_event_revenue'] = cvMapDf['max_event_revenue'].shift(-1, fill_value=0)
        cvMapDf = cvMapDf[cvMapDf['next_max_event_revenue'] > 0]
        # print('debug: cvMapDf:')
        # print(cvMapDf.head(10))
        for i in range(len(cvMapDf)):
            cv = cvMapDf.iloc[i]['cv']
            min_revenue = cvMapDf.iloc[i]['min_event_revenue']
            max_revenue = cvMapDf.iloc[i]['next_max_event_revenue']
            avg = (min_revenue + max_revenue) / 2
            df0.loc[(df0[usd] > min_revenue) & (df0[usd] <= max_revenue), 'cv'] = cv
            df0.loc[(df0[usd] > min_revenue) & (df0[usd] <= max_revenue), 'avg'] = avg

        df0.loc[(df0[usd] > cvMapDf['max_event_revenue'].max()), 'cv'] = cvMapDf['cv'].max()
        df0.loc[(df0[usd] > cvMapDf['max_event_revenue'].max()), 'avg'] = df0['avg'].max()
        df0['cv'] = df0['cv'].fillna(0)
        df0.loc[df0['cv'] == 0, 'avg'] = 0

        df0 = df0.groupby(['cv']).agg({'uid': 'sum', 'sumUsd': 'sum', 'avg': 'mean'}).reset_index()
        df0['diffUsd'] = np.abs(df0['sumUsd'] - df0['avg'] * df0['uid'])

        df0 = df0.merge(cvMapDf[['cv','min_event_revenue','max_event_revenue','next_max_event_revenue']], on='cv', how='left')

        # print('debug: df0:')
        # print(df0.head(10))

        # 找到最小的diffUsd对应的level
        min_diffUsd = df0['diffUsd'].min()
        min_diffUsd_index = df0[df0['diffUsd'] == min_diffUsd].index[0]
        min_diffUsd_cv = df0[df0['diffUsd'] == min_diffUsd]['cv'].values[0].astype(int)
        
        # print('min_diffUsd:', min_diffUsd, 'min_diffUsd_index:', min_diffUsd_index)
        # print(df0.iloc[min_diffUsd_index-2:min_diffUsd_index+2])

        return min_diffUsd_cv - 2

    while len(levels) > N-1:
        levelsIndex = func1(df,levels)
        levels.pop(levelsIndex)
        # print(f'去除第{levelsIndex}个档位：',levels[levelsIndex])
        # print('当前剩余档位数:', len(levels))
        # print('当前levels:')
        # print(levels)
        

    # print('最终档位数:', len(levels))
    # print('最终档位:', levels)
    return levels
        
from sklearn.cluster import KMeans
def makeLevelsByKMeans(userDf, usd='r1usd', N=32):
    filtered_df = userDf[userDf[usd] > 0]
    df = filtered_df.sort_values([usd])
    data = df[usd].values.reshape(-1, 1)
    kmeans = KMeans(n_clusters=N-1, random_state=0).fit(data)
    levels = sorted(np.unique(kmeans.cluster_centers_).tolist())

    return levels

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

    # # 每个档位的真实花费占比，和cv均值花费占比，只打印到终端，看看就好
    # tmpDf = df.groupby([cv]).agg({'sumUsd':'sum','sumAvg':'sum'}).reset_index()
    # tmpDf['真实花费占比'] = tmpDf['sumUsd']/tmpDf['sumUsd'].sum()
    # tmpDf['cv均值花费占比'] = tmpDf['sumAvg']/tmpDf['sumAvg'].sum()    
    # print(tmpDf)

    df = df.groupby(['install_date']).agg({'sumUsd':'sum','sumAvg':'sum'}).reset_index()
    df['mape'] = abs(df['sumUsd'] - df['sumAvg']) / df['sumUsd']
    return df['mape'].mean()

def checkLevels(df,levels,usd='payUsd',cv='cv'):
    cvMapDf = makeCvMap(levels)

    return checkCv(df,cvMapDf,usd=usd,cv=cv)

def init():
    global execSql
    global dayStr

    if 'o' in globals():
        print('this is online version')

        from odps import options
        # UTC+0
        options.sql.settings = {
            'odps.sql.timezone':'Africa/Accra',
            "odps.sql.submit.mode" : "script"
        }

        def execSql_online(sql):
            with o.execute_sql(sql).open_reader(tunnel=True, limit=False) as reader:
                pd_df = reader.to_pandas()
                print('获得%d行数据' % len(pd_df))
                return pd_df

        execSql = execSql_online

        # 线上版本是有args这个全局变量的，无需再判断
        dayStr = args['dayStr']
    else:
        print('this is local version')
        import sys
        sys.path.append('/src')
        from src.maxCompute import execSql as execSql_local

        execSql = execSql_local
        dayStr = '20250608'

    print('dayStr:', dayStr)

def createCvMapTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            # Column(name='app_package', type='string', comment=''),
            # Column(name='name', type='string', comment=''),
            Column(name='conversion_value', type='double', comment=''),
            Column(name='min_event_revenue', type='double', comment=''),
            Column(name='max_event_revenue', type='double', comment=''),
        ]
        partitions = [
            Partition(name='app_package', type='string', comment=''),
            Partition(name='name', type='string', comment=''),
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'ios_conversion_value_map'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deleteCvMapPartition(app_package,name):
    if 'o' in globals():
        table_name = 'ios_conversion_value_map'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('app_package=%s,name=%s'%(app_package,name), if_exists=True)
        print(f"Deleted partition: app_package={app_package},name={name}")
    else:
        print('No partition deletion in local version')

def writeCvMapToMC(cvMapDf,app_package,name):
    print('写入数据到MC',app_package,name)
    print(cvMapDf)
    if 'o' in globals():
        table_name = 'ios_conversion_value_map'
        t = o.get_table(table_name)
        with t.open_writer(partition='app_package=%s,name=%s'%(app_package,name), create_partition=True, arrow=True) as writer:
            writer.write(cvMapDf)
        print('写入MC成功')
    else:
        print('No data writing in local version')

def createMapeTable():
    if 'o' in globals():
        from odps.models import Schema, Column, Partition
        # 创建表格（如果不存在）
        columns = [
            Column(name='mape', type='double', comment=''),
        ]
        partitions = [
            Partition(name='day', type='string', comment='prediction date, like 20221018'),
            Partition(name='app_package', type='string', comment=''),
            Partition(name='name', type='string', comment=''),
        ]
        schema = Schema(columns=columns, partitions=partitions)
        table_name = 'ios_conversion_value_mape'
        o.create_table(table_name, schema, if_not_exists=True)
    else:
        print('No table creation in local version')

def deleteMapePartition(day,app_package,name):
    if 'o' in globals():
        table_name = 'ios_conversion_value_mape'
        t = o.get_table(table_name)
        # 删除分区（如果存在）
        t.delete_partition('day=%s,app_package=%s,name=%s'%(day,app_package,name), if_exists=True)
        print(f"Deleted partition: day={day},app_package={app_package},name={name}")
    else:
        print('No partition deletion in local version')

def writeMapeToMC(mape,day,app_package,name):
    print('写入数据到MC',day,app_package,name)
    print(mape)
    if 'o' in globals():
        table_name = 'ios_conversion_value_mape'
        t = o.get_table(table_name)
        with t.open_writer(partition='day=%s,app_package=%s,name=%s'%(day,app_package,name), create_partition=True, arrow=True) as writer:
            writer.write(pd.DataFrame({'mape':[mape]}))
        print('写入MC成功')
    else:
        print('No data writing in local version')

def levelsToCvMap(levels):
    mapData = {
        'conversion_value': [0],
        'min_event_revenue': [-1],
        'max_event_revenue': [0]
    }
    
    for i in range(len(levels)):
        min_revenue = mapData['max_event_revenue'][-1]
        max_revenue = levels[i]
        mapData['conversion_value'].append(i + 1)
        mapData['min_event_revenue'].append(min_revenue)
        mapData['max_event_revenue'].append(max_revenue)
    
    # # 最后一档的 max_event_revenue 设置为无穷大
    # mapData['max_event_revenue'][-1] = np.inf
    
    cvMapDf = pd.DataFrame(data=mapData)

    cvMapDf['conversion_value'] = cvMapDf['conversion_value'].astype('double')
    cvMapDf['min_event_revenue'] = cvMapDf['min_event_revenue'].astype('double')
    cvMapDf['max_event_revenue'] = cvMapDf['max_event_revenue'].astype('double')
    return cvMapDf

def getTopherosPayDataFromMC(todayStr):
    # todayStr = datetime.datetime.now().strftime('%Y%m%d')
    today = datetime.datetime.strptime(todayStr, '%Y%m%d')
    oneMonthAgoStr = (today - datetime.timedelta(days=30)).strftime('%Y%m%d')
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
            app = 116
            and zone = 0
            and day between {oneMonthAgoStr} and {todayStr}
            and app_package = 'id6450953550'
        group by
            install_day,
            game_uid
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

topheros20240201CvMapStr = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6450953550,0,,,,,,0,24,2023-12-29 09:39:55,0,,,
id6450953550,1,af_skad_revenue,0,1,0,1.94,0,24,2023-12-29 09:39:55,0,,,
id6450953550,2,af_skad_revenue,0,1,1.94,1.99,0,24,2023-12-29 09:39:55,0,,,
id6450953550,3,af_skad_revenue,0,1,1.99,2.03,0,24,2023-12-29 09:39:55,0,,,
id6450953550,4,af_skad_revenue,0,1,2.03,2.07,0,24,2023-12-29 09:39:55,0,,,
id6450953550,5,af_skad_revenue,0,1,2.07,2.17,0,24,2023-12-29 09:39:55,0,,,
id6450953550,6,af_skad_revenue,0,1,2.17,2.19,0,24,2023-12-29 09:39:55,0,,,
id6450953550,7,af_skad_revenue,0,1,2.19,2.39,0,24,2023-12-29 09:39:55,0,,,
id6450953550,8,af_skad_revenue,0,1,2.39,3.37,0,24,2023-12-29 09:39:55,0,,,
id6450953550,9,af_skad_revenue,0,1,3.37,5.97,0,24,2023-12-29 09:39:55,0,,,
id6450953550,10,af_skad_revenue,0,1,5.97,7.7,0,24,2023-12-29 09:39:55,0,,,
id6450953550,11,af_skad_revenue,0,1,7.7,9.89,0,24,2023-12-29 09:39:55,0,,,
id6450953550,12,af_skad_revenue,0,1,9.89,11.98,0,24,2023-12-29 09:39:55,0,,,
id6450953550,13,af_skad_revenue,0,1,11.98,12.37,0,24,2023-12-29 09:39:55,0,,,
id6450953550,14,af_skad_revenue,0,1,12.37,13.07,0,24,2023-12-29 09:39:55,0,,,
id6450953550,15,af_skad_revenue,0,1,13.07,13.21,0,24,2023-12-29 09:39:55,0,,,
id6450953550,16,af_skad_revenue,0,1,13.21,14.95,0,24,2023-12-29 09:39:55,0,,,
id6450953550,17,af_skad_revenue,0,1,14.95,16.98,0,24,2023-12-29 09:39:55,0,,,
id6450953550,18,af_skad_revenue,0,1,16.98,18.51,0,24,2023-12-29 09:39:55,0,,,
id6450953550,19,af_skad_revenue,0,1,18.51,19.96,0,24,2023-12-29 09:39:55,0,,,
id6450953550,20,af_skad_revenue,0,1,19.96,22.06,0,24,2023-12-29 09:39:55,0,,,
id6450953550,21,af_skad_revenue,0,1,22.06,23.69,0,24,2023-12-29 09:39:55,0,,,
id6450953550,22,af_skad_revenue,0,1,23.69,25.29,0,24,2023-12-29 09:39:55,0,,,
id6450953550,23,af_skad_revenue,0,1,25.29,27.42,0,24,2023-12-29 09:39:55,0,,,
id6450953550,24,af_skad_revenue,0,1,27.42,29.63,0,24,2023-12-29 09:39:55,0,,,
id6450953550,25,af_skad_revenue,0,1,29.63,31.32,0,24,2023-12-29 09:39:55,0,,,
id6450953550,26,af_skad_revenue,0,1,31.32,33.93,0,24,2023-12-29 09:39:55,0,,,
id6450953550,27,af_skad_revenue,0,1,33.93,36.33,0,24,2023-12-29 09:39:55,0,,,
id6450953550,28,af_skad_revenue,0,1,36.33,38.61,0,24,2023-12-29 09:39:55,0,,,
id6450953550,29,af_skad_revenue,0,1,38.61,41.95,0,24,2023-12-29 09:39:55,0,,,
id6450953550,30,af_skad_revenue,0,1,41.95,44.93,0,24,2023-12-29 09:39:55,0,,,
id6450953550,31,af_skad_revenue,0,1,44.93,47.88,0,24,2023-12-29 09:39:55,0,,,
id6450953550,32,af_skad_revenue,0,1,47.88,52.3,0,24,2023-12-29 09:39:55,0,,,
id6450953550,33,af_skad_revenue,0,1,52.3,56.66,0,24,2023-12-29 09:39:55,0,,,
id6450953550,34,af_skad_revenue,0,1,56.66,62.14,0,24,2023-12-29 09:39:55,0,,,
id6450953550,35,af_skad_revenue,0,1,62.14,67.09,0,24,2023-12-29 09:39:55,0,,,
id6450953550,36,af_skad_revenue,0,1,67.09,71.75,0,24,2023-12-29 09:39:55,0,,,
id6450953550,37,af_skad_revenue,0,1,71.75,77.93,0,24,2023-12-29 09:39:55,0,,,
id6450953550,38,af_skad_revenue,0,1,77.93,85.94,0,24,2023-12-29 09:39:55,0,,,
id6450953550,39,af_skad_revenue,0,1,85.94,92.47,0,24,2023-12-29 09:39:55,0,,,
id6450953550,40,af_skad_revenue,0,1,92.47,103.79,0,24,2023-12-29 09:39:55,0,,,
id6450953550,41,af_skad_revenue,0,1,103.79,107.89,0,24,2023-12-29 09:39:55,0,,,
id6450953550,42,af_skad_revenue,0,1,107.89,114.47,0,24,2023-12-29 09:39:55,0,,,
id6450953550,43,af_skad_revenue,0,1,114.47,123.71,0,24,2023-12-29 09:39:55,0,,,
id6450953550,44,af_skad_revenue,0,1,123.71,131.89,0,24,2023-12-29 09:39:55,0,,,
id6450953550,45,af_skad_revenue,0,1,131.89,143.1,0,24,2023-12-29 09:39:55,0,,,
id6450953550,46,af_skad_revenue,0,1,143.1,155.86,0,24,2023-12-29 09:39:55,0,,,
id6450953550,47,af_skad_revenue,0,1,155.86,170.61,0,24,2023-12-29 09:39:55,0,,,
id6450953550,48,af_skad_revenue,0,1,170.61,184.13,0,24,2023-12-29 09:39:55,0,,,
id6450953550,49,af_skad_revenue,0,1,184.13,203.16,0,24,2023-12-29 09:39:55,0,,,
id6450953550,50,af_skad_revenue,0,1,203.16,226.78,0,24,2023-12-29 09:39:55,0,,,
id6450953550,51,af_skad_revenue,0,1,226.78,244.75,0,24,2023-12-29 09:39:55,0,,,
id6450953550,52,af_skad_revenue,0,1,244.75,265.39,0,24,2023-12-29 09:39:55,0,,,
id6450953550,53,af_skad_revenue,0,1,265.39,282.05,0,24,2023-12-29 09:39:55,0,,,
id6450953550,54,af_skad_revenue,0,1,282.05,310.77,0,24,2023-12-29 09:39:55,0,,,
id6450953550,55,af_skad_revenue,0,1,310.77,337.73,0,24,2023-12-29 09:39:55,0,,,
id6450953550,56,af_skad_revenue,0,1,337.73,385.3,0,24,2023-12-29 09:39:55,0,,,
id6450953550,57,af_skad_revenue,0,1,385.3,443.66,0,24,2023-12-29 09:39:55,0,,,
id6450953550,58,af_skad_revenue,0,1,443.66,547.3,0,24,2023-12-29 09:39:55,0,,,
id6450953550,59,af_skad_revenue,0,1,547.3,630.49,0,24,2023-12-29 09:39:55,0,,,
id6450953550,60,af_skad_revenue,0,1,630.49,836.92,0,24,2023-12-29 09:39:55,0,,,
id6450953550,61,af_skad_revenue,0,1,836.92,1354.95,0,24,2023-12-29 09:39:55,0,,,
id6450953550,62,af_skad_revenue,0,1,1354.95,1706.24,0,24,2023-12-29 09:39:55,0,,,
id6450953550,63,af_skad_revenue,0,1,1706.24,2000,0,24,2023-12-29 09:39:55,0,,,
'''

def topherosMain():
    df = getTopherosPayDataFromMC(dayStr)
    
    # 进行一定的过滤，将收入超过2000美元的用户收入改为2000美元
    df.loc[df['revenue'] > 2000, 'revenue'] = 2000

    # 计算旧版本的Mape
    csv_file_like_object = io.StringIO(topheros20240201CvMapStr)    
    # 加载CV Map
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value', 'min_event_revenue', 'max_event_revenue']]
    # 将cvMapDf写入MC
    app_package = 'id6450953550'
    name = 'af_cv_map'
    
    levels = cvMapDf['max_event_revenue'].dropna().tolist()
    cvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package, name)
    writeCvMapToMC(cvMapDf, app_package, name)

    mape = checkLevels(df, levels, usd='revenue', cv='cv')
    deleteMapePartition(dayStr, app_package, name)
    writeMapeToMC(mape, dayStr, app_package, name)
    
    # 生成新的 levels
    name = 'new_cv_map'
    levels = makeLevels(df, usd='revenue', N=64)
    levels = [round(x, 2) for x in levels]
    # 将新的 levels 转换为 cvMap 并写入数据库
    newCvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package, name)
    writeCvMapToMC(newCvMapDf, app_package, name)

    # 将mape写入MC
    mape = checkLevels(df, levels, usd='revenue', cv='cv')
    deleteMapePartition(dayStr, app_package, name)
    writeMapeToMC(mape, dayStr, app_package, name)

    # 使用 KMeans 生成新的 levels
    name = 'new_cv_map_kmeans'
    levels = makeLevelsByKMeans(df, usd='revenue', N=64)
    levels = [round(x, 2) for x in levels]
    # 将新的 levels 转换为 cvMap 并写入数据库
    newCvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package, name)
    writeCvMapToMC(newCvMapDf, app_package, name)

    # 将mape写入MC
    mape = checkLevels(df, levels, usd='revenue', cv='cv')
    deleteMapePartition(dayStr, app_package, name)
    writeMapeToMC(mape, dayStr, app_package, name)

def getLastwarPayDataFromMC(todayStr):
    today = datetime.datetime.strptime(todayStr, '%Y%m%d')
    oneMonthAgoStr = (today - datetime.timedelta(days=30)).strftime('%Y%m%d')
    print('获得%s~%s的付费数据'%(oneMonthAgoStr,todayStr))

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
        group by
            install_day,
            game_uid
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

lastwar20240708CvMapStr = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2024-07-05 09:48:05,0,,,
id6448786147,1,af_purchase_update_skan_on,0,1,0,0.97,0,24,2024-07-05 09:48:05,0,,,
id6448786147,2,af_purchase_update_skan_on,0,1,0.97,0.99,0,24,2024-07-05 09:48:05,0,,,
id6448786147,3,af_purchase_update_skan_on,0,1,0.99,1.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,4,af_purchase_update_skan_on,0,1,1.92,2.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,5,af_purchase_update_skan_on,0,1,2.91,3.28,0,24,2024-07-05 09:48:05,0,,,
id6448786147,6,af_purchase_update_skan_on,0,1,3.28,5.85,0,24,2024-07-05 09:48:05,0,,,
id6448786147,7,af_purchase_update_skan_on,0,1,5.85,7.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,8,af_purchase_update_skan_on,0,1,7.67,9.24,0,24,2024-07-05 09:48:05,0,,,
id6448786147,9,af_purchase_update_skan_on,0,1,9.24,12.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,10,af_purchase_update_skan_on,0,1,12.4,14.95,0,24,2024-07-05 09:48:05,0,,,
id6448786147,11,af_purchase_update_skan_on,0,1,14.95,17.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,12,af_purchase_update_skan_on,0,1,17.96,22.37,0,24,2024-07-05 09:48:05,0,,,
id6448786147,13,af_purchase_update_skan_on,0,1,22.37,26.96,0,24,2024-07-05 09:48:05,0,,,
id6448786147,14,af_purchase_update_skan_on,0,1,26.96,31.81,0,24,2024-07-05 09:48:05,0,,,
id6448786147,15,af_purchase_update_skan_on,0,1,31.81,36.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,16,af_purchase_update_skan_on,0,1,36.25,42.53,0,24,2024-07-05 09:48:05,0,,,
id6448786147,17,af_purchase_update_skan_on,0,1,42.53,49.91,0,24,2024-07-05 09:48:05,0,,,
id6448786147,18,af_purchase_update_skan_on,0,1,49.91,57.92,0,24,2024-07-05 09:48:05,0,,,
id6448786147,19,af_purchase_update_skan_on,0,1,57.92,67.93,0,24,2024-07-05 09:48:05,0,,,
id6448786147,20,af_purchase_update_skan_on,0,1,67.93,81.27,0,24,2024-07-05 09:48:05,0,,,
id6448786147,21,af_purchase_update_skan_on,0,1,81.27,98.25,0,24,2024-07-05 09:48:05,0,,,
id6448786147,22,af_purchase_update_skan_on,0,1,98.25,117.86,0,24,2024-07-05 09:48:05,0,,,
id6448786147,23,af_purchase_update_skan_on,0,1,117.86,142.29,0,24,2024-07-05 09:48:05,0,,,
id6448786147,24,af_purchase_update_skan_on,0,1,142.29,180.76,0,24,2024-07-05 09:48:05,0,,,
id6448786147,25,af_purchase_update_skan_on,0,1,180.76,225.43,0,24,2024-07-05 09:48:05,0,,,
id6448786147,26,af_purchase_update_skan_on,0,1,225.43,276.72,0,24,2024-07-05 09:48:05,0,,,
id6448786147,27,af_purchase_update_skan_on,0,1,276.72,347.4,0,24,2024-07-05 09:48:05,0,,,
id6448786147,28,af_purchase_update_skan_on,0,1,347.4,472.67,0,24,2024-07-05 09:48:05,0,,,
id6448786147,29,af_purchase_update_skan_on,0,1,472.67,620.8,0,24,2024-07-05 09:48:05,0,,,
id6448786147,30,af_purchase_update_skan_on,0,1,620.8,972.22,0,24,2024-07-05 09:48:05,0,,,
id6448786147,31,af_purchase_update_skan_on,0,1,972.22,2038.09,0,24,2024-07-05 09:48:05,0,,,
'''

lastwar20250403CvMapStr = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6448786147,0,,,,,,0,24,2025-04-03 05:19:54,0,,,
id6448786147,1,af_purchase_update_skan_on,0,1,0,1.06,0,24,2025-04-03 05:19:54,0,,,
id6448786147,2,af_purchase_update_skan_on,0,1,1.06,2.13,0,24,2025-04-03 05:19:54,0,,,
id6448786147,3,af_purchase_update_skan_on,0,1,2.13,3.2,0,24,2025-04-03 05:19:54,0,,,
id6448786147,4,af_purchase_update_skan_on,0,1,3.2,5.97,0,24,2025-04-03 05:19:54,0,,,
id6448786147,5,af_purchase_update_skan_on,0,1,5.97,8.08,0,24,2025-04-03 05:19:54,0,,,
id6448786147,6,af_purchase_update_skan_on,0,1,8.08,10.91,0,24,2025-04-03 05:19:54,0,,,
id6448786147,7,af_purchase_update_skan_on,0,1,10.91,13.31,0,24,2025-04-03 05:19:54,0,,,
id6448786147,8,af_purchase_update_skan_on,0,1,13.31,17.17,0,24,2025-04-03 05:19:54,0,,,
id6448786147,9,af_purchase_update_skan_on,0,1,17.17,22.15,0,24,2025-04-03 05:19:54,0,,,
id6448786147,10,af_purchase_update_skan_on,0,1,22.15,27.18,0,24,2025-04-03 05:19:54,0,,,
id6448786147,11,af_purchase_update_skan_on,0,1,27.18,32.25,0,24,2025-04-03 05:19:54,0,,,
id6448786147,12,af_purchase_update_skan_on,0,1,32.25,37.27,0,24,2025-04-03 05:19:54,0,,,
id6448786147,13,af_purchase_update_skan_on,0,1,37.27,42.96,0,24,2025-04-03 05:19:54,0,,,
id6448786147,14,af_purchase_update_skan_on,0,1,42.96,49.94,0,24,2025-04-03 05:19:54,0,,,
id6448786147,15,af_purchase_update_skan_on,0,1,49.94,57.19,0,24,2025-04-03 05:19:54,0,,,
id6448786147,16,af_purchase_update_skan_on,0,1,57.19,63.4,0,24,2025-04-03 05:19:54,0,,,
id6448786147,17,af_purchase_update_skan_on,0,1,63.4,70.59,0,24,2025-04-03 05:19:54,0,,,
id6448786147,18,af_purchase_update_skan_on,0,1,70.59,79.52,0,24,2025-04-03 05:19:54,0,,,
id6448786147,19,af_purchase_update_skan_on,0,1,79.52,89.43,0,24,2025-04-03 05:19:54,0,,,
id6448786147,20,af_purchase_update_skan_on,0,1,89.43,98.61,0,24,2025-04-03 05:19:54,0,,,
id6448786147,21,af_purchase_update_skan_on,0,1,98.61,105.7,0,24,2025-04-03 05:19:54,0,,,
id6448786147,22,af_purchase_update_skan_on,0,1,105.7,114.61,0,24,2025-04-03 05:19:54,0,,,
id6448786147,23,af_purchase_update_skan_on,0,1,114.61,124.59,0,24,2025-04-03 05:19:54,0,,,
id6448786147,24,af_purchase_update_skan_on,0,1,124.59,135.71,0,24,2025-04-03 05:19:54,0,,,
id6448786147,25,af_purchase_update_skan_on,0,1,135.71,148.07,0,24,2025-04-03 05:19:54,0,,,
id6448786147,26,af_purchase_update_skan_on,0,1,148.07,160.79,0,24,2025-04-03 05:19:54,0,,,
id6448786147,27,af_purchase_update_skan_on,0,1,160.79,173.8,0,24,2025-04-03 05:19:54,0,,,
id6448786147,28,af_purchase_update_skan_on,0,1,173.8,185.74,0,24,2025-04-03 05:19:54,0,,,
id6448786147,29,af_purchase_update_skan_on,0,1,185.74,199.42,0,24,2025-04-03 05:19:54,0,,,
id6448786147,30,af_purchase_update_skan_on,0,1,199.42,215.69,0,24,2025-04-03 05:19:54,0,,,
id6448786147,31,af_purchase_update_skan_on,0,1,215.69,232.62,0,24,2025-04-03 05:19:54,0,,,
id6448786147,32,af_purchase_update_skan_on,0,1,232.62,247.52,0,24,2025-04-03 05:19:54,0,,,
id6448786147,33,af_purchase_update_skan_on,0,1,247.52,264.32,0,24,2025-04-03 05:19:54,0,,,
id6448786147,34,af_purchase_update_skan_on,0,1,264.32,283.8,0,24,2025-04-03 05:19:54,0,,,
id6448786147,35,af_purchase_update_skan_on,0,1,283.8,299.49,0,24,2025-04-03 05:19:54,0,,,
id6448786147,36,af_purchase_update_skan_on,0,1,299.49,325.72,0,24,2025-04-03 05:19:54,0,,,
id6448786147,37,af_purchase_update_skan_on,0,1,325.72,347.38,0,24,2025-04-03 05:19:54,0,,,
id6448786147,38,af_purchase_update_skan_on,0,1,347.38,374.22,0,24,2025-04-03 05:19:54,0,,,
id6448786147,39,af_purchase_update_skan_on,0,1,374.22,401.19,0,24,2025-04-03 05:19:54,0,,,
id6448786147,40,af_purchase_update_skan_on,0,1,401.19,441.07,0,24,2025-04-03 05:19:54,0,,,
id6448786147,41,af_purchase_update_skan_on,0,1,441.07,492.51,0,24,2025-04-03 05:19:54,0,,,
id6448786147,42,af_purchase_update_skan_on,0,1,492.51,519.21,0,24,2025-04-03 05:19:54,0,,,
id6448786147,43,af_purchase_update_skan_on,0,1,519.21,549.44,0,24,2025-04-03 05:19:54,0,,,
id6448786147,44,af_purchase_update_skan_on,0,1,549.44,580.15,0,24,2025-04-03 05:19:54,0,,,
id6448786147,45,af_purchase_update_skan_on,0,1,580.15,617.15,0,24,2025-04-03 05:19:54,0,,,
id6448786147,46,af_purchase_update_skan_on,0,1,617.15,668.99,0,24,2025-04-03 05:19:54,0,,,
id6448786147,47,af_purchase_update_skan_on,0,1,668.99,711.78,0,24,2025-04-03 05:19:54,0,,,
id6448786147,48,af_purchase_update_skan_on,0,1,711.78,762.73,0,24,2025-04-03 05:19:54,0,,,
id6448786147,49,af_purchase_update_skan_on,0,1,762.73,815.06,0,24,2025-04-03 05:19:54,0,,,
id6448786147,50,af_purchase_update_skan_on,0,1,815.06,915.85,0,24,2025-04-03 05:19:54,0,,,
id6448786147,51,af_purchase_update_skan_on,0,1,915.85,975.68,0,24,2025-04-03 05:19:54,0,,,
id6448786147,52,af_purchase_update_skan_on,0,1,975.68,1057.86,0,24,2025-04-03 05:19:54,0,,,
id6448786147,53,af_purchase_update_skan_on,0,1,1057.86,1239.54,0,24,2025-04-03 05:19:54,0,,,
id6448786147,54,af_purchase_update_skan_on,0,1,1239.54,1282.83,0,24,2025-04-03 05:19:54,0,,,
id6448786147,55,af_purchase_update_skan_on,0,1,1282.83,1416.8,0,24,2025-04-03 05:19:54,0,,,
id6448786147,56,af_purchase_update_skan_on,0,1,1416.8,1570,0,24,2025-04-03 05:19:54,0,,,
id6448786147,57,af_purchase_update_skan_on,0,1,1570,1628.32,0,24,2025-04-03 05:19:54,0,,,
id6448786147,58,af_purchase_update_skan_on,0,1,1628.32,1745.34,0,24,2025-04-03 05:19:54,0,,,
id6448786147,59,af_purchase_update_skan_on,0,1,1745.34,1792.65,0,24,2025-04-03 05:19:54,0,,,
id6448786147,60,af_purchase_update_skan_on,0,1,1792.65,1935.41,0,24,2025-04-03 05:19:54,0,,,
id6448786147,61,af_purchase_update_skan_on,0,1,1935.41,2304.95,0,24,2025-04-03 05:19:54,0,,,
id6448786147,62,af_purchase_update_skan_on,0,1,2304.95,3006.72,0,24,2025-04-03 05:19:54,0,,,
id6448786147,63,af_purchase_update_skan_on,0,1,3006.72,3814.71,0,24,2025-04-03 05:19:54,0,,,
'''

def lastwarMain():

    N = 64

    df = getLastwarPayDataFromMC(dayStr)

    # # 将收入3000以上的用户的收入设置为3000
    # df.loc[df['revenue']>3000,'revenue'] = 3000

    # 计算旧版本的Mape
    csv_file_like_object = io.StringIO(lastwar20250403CvMapStr)    
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value', 'min_event_revenue', 'max_event_revenue']]
    # 将cvMapDf写入MC
    app_package = 'id6448786147'
    name = 'af_cv_map'
    
    levels = cvMapDf['max_event_revenue'].dropna().tolist()
    cvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package, name)
    writeCvMapToMC(cvMapDf, app_package, name)
    
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)
    
    # 生成新的 levels
    name = 'new_cv_map'
    levels = makeLevels(df,usd='revenue',N=N)
    levels = [round(x,2) for x in levels]
    # 将新的 levels 转换为 cvMap 并写入数据库
    newCvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package,name)
    writeCvMapToMC(newCvMapDf,app_package,name)
    
    # 将mape写入MC
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)

    # 使用 KMeans 生成新的 levels
    name = 'new_cv_map_kmeans'
    levels = makeLevelsByKMeans(df,usd='revenue',N=N)
    levels = [round(x,2) for x in levels]
    # 将新的 levels 转换为 cvMap 并写入数据库
    cvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package,name)
    writeCvMapToMC(cvMapDf,app_package,name)

    # 将mape写入MC
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)

def getLastwarVNPayDataFromMC(todayStr):
    today = datetime.datetime.strptime(todayStr, '%Y%m%d')
    oneMonthAgoStr = (today - datetime.timedelta(days=30)).strftime('%Y%m%d')
    print('获得%s~%s的付费数据'%(oneMonthAgoStr,todayStr))

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
            and app_package = 'id6736925794'
        group by
            install_day,
            game_uid
        ;
    '''
    print(sql)
    df = execSql(sql)
    df = df[df['install_date'] >= oneMonthAgoStr]
    return df

lastwarVN20240708CvMapStr = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change,postback_sequence_index,coarse_conversion_value,lock_window_type,lock_window_time
id6736925794,0,,,,,,0,24,2025-01-10 03:37:50,0,,,
id6736925794,1,af_purchase_update_skan_on,0,1,0,0.82,0,24,2025-01-10 03:37:50,0,,,
id6736925794,2,af_purchase_update_skan_on,0,1,0.82,1.04,0,24,2025-01-10 03:37:50,0,,,
id6736925794,3,af_purchase_update_skan_on,0,1,1.04,2.11,0,24,2025-01-10 03:37:50,0,,,
id6736925794,4,af_purchase_update_skan_on,0,1,2.11,3.03,0,24,2025-01-10 03:37:50,0,,,
id6736925794,5,af_purchase_update_skan_on,0,1,3.03,3.69,0,24,2025-01-10 03:37:50,0,,,
id6736925794,6,af_purchase_update_skan_on,0,1,3.69,4.88,0,24,2025-01-10 03:37:50,0,,,
id6736925794,7,af_purchase_update_skan_on,0,1,4.88,6.08,0,24,2025-01-10 03:37:50,0,,,
id6736925794,8,af_purchase_update_skan_on,0,1,6.08,7.14,0,24,2025-01-10 03:37:50,0,,,
id6736925794,9,af_purchase_update_skan_on,0,1,7.14,8.43,0,24,2025-01-10 03:37:50,0,,,
id6736925794,10,af_purchase_update_skan_on,0,1,8.43,10.76,0,24,2025-01-10 03:37:50,0,,,
id6736925794,11,af_purchase_update_skan_on,0,1,10.76,12.91,0,24,2025-01-10 03:37:50,0,,,
id6736925794,12,af_purchase_update_skan_on,0,1,12.91,15.12,0,24,2025-01-10 03:37:50,0,,,
id6736925794,13,af_purchase_update_skan_on,0,1,15.12,17.83,0,24,2025-01-10 03:37:50,0,,,
id6736925794,14,af_purchase_update_skan_on,0,1,17.83,20.76,0,24,2025-01-10 03:37:50,0,,,
id6736925794,15,af_purchase_update_skan_on,0,1,20.76,23.27,0,24,2025-01-10 03:37:50,0,,,
id6736925794,16,af_purchase_update_skan_on,0,1,23.27,26.09,0,24,2025-01-10 03:37:50,0,,,
id6736925794,17,af_purchase_update_skan_on,0,1,26.09,29.18,0,24,2025-01-10 03:37:50,0,,,
id6736925794,18,af_purchase_update_skan_on,0,1,29.18,32.77,0,24,2025-01-10 03:37:50,0,,,
id6736925794,19,af_purchase_update_skan_on,0,1,32.77,35.7,0,24,2025-01-10 03:37:50,0,,,
id6736925794,20,af_purchase_update_skan_on,0,1,35.7,39,0,24,2025-01-10 03:37:50,0,,,
id6736925794,21,af_purchase_update_skan_on,0,1,39,43.27,0,24,2025-01-10 03:37:50,0,,,
id6736925794,22,af_purchase_update_skan_on,0,1,43.27,47.91,0,24,2025-01-10 03:37:50,0,,,
id6736925794,23,af_purchase_update_skan_on,0,1,47.91,53.07,0,24,2025-01-10 03:37:50,0,,,
id6736925794,24,af_purchase_update_skan_on,0,1,53.07,58.25,0,24,2025-01-10 03:37:50,0,,,
id6736925794,25,af_purchase_update_skan_on,0,1,58.25,65.01,0,24,2025-01-10 03:37:50,0,,,
id6736925794,26,af_purchase_update_skan_on,0,1,65.01,71.02,0,24,2025-01-10 03:37:50,0,,,
id6736925794,27,af_purchase_update_skan_on,0,1,71.02,77,0,24,2025-01-10 03:37:50,0,,,
id6736925794,28,af_purchase_update_skan_on,0,1,77,82.33,0,24,2025-01-10 03:37:50,0,,,
id6736925794,29,af_purchase_update_skan_on,0,1,82.33,87.44,0,24,2025-01-10 03:37:50,0,,,
id6736925794,30,af_purchase_update_skan_on,0,1,87.44,97.04,0,24,2025-01-10 03:37:50,0,,,
id6736925794,31,af_purchase_update_skan_on,0,1,97.04,104.05,0,24,2025-01-10 03:37:50,0,,,
id6736925794,32,af_purchase_update_skan_on,0,1,104.05,111.25,0,24,2025-01-10 03:37:50,0,,,
id6736925794,33,af_purchase_update_skan_on,0,1,111.25,118.46,0,24,2025-01-10 03:37:50,0,,,
id6736925794,34,af_purchase_update_skan_on,0,1,118.46,127.5,0,24,2025-01-10 03:37:50,0,,,
id6736925794,35,af_purchase_update_skan_on,0,1,127.5,142.11,0,24,2025-01-10 03:37:50,0,,,
id6736925794,36,af_purchase_update_skan_on,0,1,142.11,158.07,0,24,2025-01-10 03:37:50,0,,,
id6736925794,37,af_purchase_update_skan_on,0,1,158.07,164.73,0,24,2025-01-10 03:37:50,0,,,
id6736925794,38,af_purchase_update_skan_on,0,1,164.73,172.46,0,24,2025-01-10 03:37:50,0,,,
id6736925794,39,af_purchase_update_skan_on,0,1,172.46,182.37,0,24,2025-01-10 03:37:50,0,,,
id6736925794,40,af_purchase_update_skan_on,0,1,182.37,192.84,0,24,2025-01-10 03:37:50,0,,,
id6736925794,41,af_purchase_update_skan_on,0,1,192.84,201.2,0,24,2025-01-10 03:37:50,0,,,
id6736925794,42,af_purchase_update_skan_on,0,1,201.2,222.03,0,24,2025-01-10 03:37:50,0,,,
id6736925794,43,af_purchase_update_skan_on,0,1,222.03,236.68,0,24,2025-01-10 03:37:50,0,,,
id6736925794,44,af_purchase_update_skan_on,0,1,236.68,249.73,0,24,2025-01-10 03:37:50,0,,,
id6736925794,45,af_purchase_update_skan_on,0,1,249.73,262.64,0,24,2025-01-10 03:37:50,0,,,
id6736925794,46,af_purchase_update_skan_on,0,1,262.64,291.51,0,24,2025-01-10 03:37:50,0,,,
id6736925794,47,af_purchase_update_skan_on,0,1,291.51,316.68,0,24,2025-01-10 03:37:50,0,,,
id6736925794,48,af_purchase_update_skan_on,0,1,316.68,351.55,0,24,2025-01-10 03:37:50,0,,,
id6736925794,49,af_purchase_update_skan_on,0,1,351.55,379.26,0,24,2025-01-10 03:37:50,0,,,
id6736925794,50,af_purchase_update_skan_on,0,1,379.26,405.63,0,24,2025-01-10 03:37:50,0,,,
id6736925794,51,af_purchase_update_skan_on,0,1,405.63,449.31,0,24,2025-01-10 03:37:50,0,,,
id6736925794,52,af_purchase_update_skan_on,0,1,449.31,474.95,0,24,2025-01-10 03:37:50,0,,,
id6736925794,53,af_purchase_update_skan_on,0,1,474.95,522.95,0,24,2025-01-10 03:37:50,0,,,
id6736925794,54,af_purchase_update_skan_on,0,1,522.95,544.87,0,24,2025-01-10 03:37:50,0,,,
id6736925794,55,af_purchase_update_skan_on,0,1,544.87,594.73,0,24,2025-01-10 03:37:50,0,,,
id6736925794,56,af_purchase_update_skan_on,0,1,594.73,700.28,0,24,2025-01-10 03:37:50,0,,,
id6736925794,57,af_purchase_update_skan_on,0,1,700.28,784.51,0,24,2025-01-10 03:37:50,0,,,
id6736925794,58,af_purchase_update_skan_on,0,1,784.51,876.11,0,24,2025-01-10 03:37:50,0,,,
id6736925794,59,af_purchase_update_skan_on,0,1,876.11,994.53,0,24,2025-01-10 03:37:50,0,,,
id6736925794,60,af_purchase_update_skan_on,0,1,994.53,1075.91,0,24,2025-01-10 03:37:50,0,,,
id6736925794,61,af_purchase_update_skan_on,0,1,1075.91,1111.48,0,24,2025-01-10 03:37:50,0,,,
id6736925794,62,af_purchase_update_skan_on,0,1,1111.48,1443.86,0,24,2025-01-10 03:37:50,0,,,
id6736925794,63,af_purchase_update_skan_on,0,1,1443.86,2060.08,0,24,2025-01-10 03:37:50,0,,,
'''


def debug(userDf,cvMapDf, usd='r1usd'):
    userDf = userDf.copy()
    cvMapDf = cvMapDf.copy()
    totalUsd = userDf[usd].sum()
    for i in range(len(cvMapDf)):
        cv = cvMapDf.iloc[i]['conversion_value']
        min_revenue = cvMapDf.iloc[i]['min_event_revenue']
        max_revenue = cvMapDf.iloc[i]['max_event_revenue']
        avg = (min_revenue + max_revenue) / 2
        userDf.loc[(userDf[usd] > min_revenue) & (userDf[usd] <= max_revenue), 'cv'] = cv
        userDf.loc[(userDf[usd] > min_revenue) & (userDf[usd] <= max_revenue), 'avg'] = avg

    userDf.loc[(userDf[usd] > cvMapDf['max_event_revenue'].max()), 'cv'] = cvMapDf['conversion_value'].max()
    userDf.loc[(userDf[usd] > cvMapDf['max_event_revenue'].max()), 'avg'] = userDf['avg'].max()
    userDf['cv'] = userDf['cv'].fillna(0)
    userDf.loc[userDf['cv'] == 0, 'avg'] = 0
    
    df = userDf.groupby('cv').agg({'uid': 'count', usd: 'sum', 'avg':'mean'}).reset_index()
    df['realAvg'] = df[usd] / df['uid']
    df['usdRatio'] = df[usd] / totalUsd
    df['diffUsd'] = (df['realAvg'] - df['avg']) * df['uid']
    df['diffUsdRatio'] = df['diffUsd'] / totalUsd

    print(df)
    print('diffUsdRatio mean:', df['diffUsdRatio'].mean())
    print('diffUsdRatio std:', df['diffUsdRatio'].std())
    print('diffUsdRatio max:', df['diffUsdRatio'].max())

def lastwarVNMain():

    N = 64

    df = getLastwarVNPayDataFromMC(dayStr)
    print('获得了%d条数据'%len(df))

    # # 将收入3000以上的用户的收入设置为3000
    # df.loc[df['revenue']>3000,'revenue'] = 3000

    # 计算旧版本的Mape
    csv_file_like_object = io.StringIO(lastwarVN20240708CvMapStr)    
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value', 'min_event_revenue', 'max_event_revenue']]
    # 将cvMapDf写入MC
    app_package = 'id6736925794'
    name = 'af_cv_map'
    
    levels = cvMapDf['max_event_revenue'].dropna().tolist()
    cvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package, name)
    writeCvMapToMC(cvMapDf, app_package, name)
    
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)
    
    # 生成新的 levels
    name = 'new_cv_map'
    levels = makeLevels2(df,usd='revenue',N=N)
    levels = [math.ceil(x * 100) / 100 for x in levels]
    # 将新的 levels 转换为 cvMap 并写入数据库
    newCvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package,name)
    writeCvMapToMC(newCvMapDf,app_package,name)
    
    # 将mape写入MC
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)

    # 使用 KMeans 生成新的 levels
    name = 'new_cv_map_kmeans'
    levels = makeLevelsByKMeans(df,usd='revenue',N=N)
    levels = [round(x,2) for x in levels]
    # 将新的 levels 转换为 cvMap 并写入数据库
    cvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package,name)
    writeCvMapToMC(cvMapDf,app_package,name)

    # 将mape写入MC
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)




def getTopwarPayDataFromMC(todayStr):
    today = datetime.datetime.strptime(todayStr, '%Y%m%d')
    oneMonthAgoStr = (today - datetime.timedelta(days=30)).strftime('%Y%m%d')
    print('获得%s~%s的付费数据'%(oneMonthAgoStr,todayStr))

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
    dwd_overseas_revenue_afattribution_realtime
where
    app = 102
    and zone = 0
    and window_cycle = '9999'
    and day between {oneMonthAgoStr} and {todayStr}
    and app_package = 'id1479198816'
group by
    install_day,
    game_uid
;
    '''
    print(sql)
    df = execSql(sql)
    return df

topwarCvMap20250102Str = '''
app_id,conversion_value,event_name,min_event_counter,max_event_counter,min_event_revenue,max_event_revenue,min_time_post_install,max_time_post_install,last_config_change
id1479198816,0,,,,,,0,24,2023-06-12 09:08:54
id1479198816,1,af_skad_revenue,0,1,0,1.64,0,24,2023-06-12 09:08:54
id1479198816,2,af_skad_revenue,0,1,1.64,3.24,0,24,2023-06-12 09:08:54
id1479198816,3,af_skad_revenue,0,1,3.24,5.35,0,24,2023-06-12 09:08:54
id1479198816,4,af_skad_revenue,0,1,5.35,7.8,0,24,2023-06-12 09:08:54
id1479198816,5,af_skad_revenue,0,1,7.8,10.71,0,24,2023-06-12 09:08:54
id1479198816,6,af_skad_revenue,0,1,10.71,14.47,0,24,2023-06-12 09:08:54
id1479198816,7,af_skad_revenue,0,1,14.47,18.99,0,24,2023-06-12 09:08:54
id1479198816,8,af_skad_revenue,0,1,18.99,24.29,0,24,2023-06-12 09:08:54
id1479198816,9,af_skad_revenue,0,1,24.29,31.08,0,24,2023-06-12 09:08:54
id1479198816,10,af_skad_revenue,0,1,31.08,40.26,0,24,2023-06-12 09:08:54
id1479198816,11,af_skad_revenue,0,1,40.26,51.52,0,24,2023-06-12 09:08:54
id1479198816,12,af_skad_revenue,0,1,51.52,61.25,0,24,2023-06-12 09:08:54
id1479198816,13,af_skad_revenue,0,1,61.25,70.16,0,24,2023-06-12 09:08:54
id1479198816,14,af_skad_revenue,0,1,70.16,82.56,0,24,2023-06-12 09:08:54
id1479198816,15,af_skad_revenue,0,1,82.56,97.38,0,24,2023-06-12 09:08:54
id1479198816,16,af_skad_revenue,0,1,97.38,111.57,0,24,2023-06-12 09:08:54
id1479198816,17,af_skad_revenue,0,1,111.57,125.27,0,24,2023-06-12 09:08:54
id1479198816,18,af_skad_revenue,0,1,125.27,142.67,0,24,2023-06-12 09:08:54
id1479198816,19,af_skad_revenue,0,1,142.67,161.66,0,24,2023-06-12 09:08:54
id1479198816,20,af_skad_revenue,0,1,161.66,184.42,0,24,2023-06-12 09:08:54
id1479198816,21,af_skad_revenue,0,1,184.42,204.85,0,24,2023-06-12 09:08:54
id1479198816,22,af_skad_revenue,0,1,204.85,239.74,0,24,2023-06-12 09:08:54
id1479198816,23,af_skad_revenue,0,1,239.74,264.97,0,24,2023-06-12 09:08:54
id1479198816,24,af_skad_revenue,0,1,264.97,306.91,0,24,2023-06-12 09:08:54
id1479198816,25,af_skad_revenue,0,1,306.91,355.15,0,24,2023-06-12 09:08:54
id1479198816,26,af_skad_revenue,0,1,355.15,405.65,0,24,2023-06-12 09:08:54
id1479198816,27,af_skad_revenue,0,1,405.65,458.36,0,24,2023-06-12 09:08:54
id1479198816,28,af_skad_revenue,0,1,458.36,512.69,0,24,2023-06-12 09:08:54
id1479198816,29,af_skad_revenue,0,1,512.69,817.08,0,24,2023-06-12 09:08:54
id1479198816,30,af_skad_revenue,0,1,817.08,1819.03,0,24,2023-06-12 09:08:54
id1479198816,31,af_skad_revenue,0,1,1819.03,2544.74,0,24,2023-06-12 09:08:54
'''

def topwarMain():
    N = 32

    df = getTopwarPayDataFromMC(dayStr)

    # 将收入3000以上的用户的收入设置为3000
    df.loc[df['revenue']>3000,'revenue'] = 3000

    # 计算旧版本的Mape
    csv_file_like_object = io.StringIO(topwarCvMap20250102Str)    
    cvMapDf = pd.read_csv(csv_file_like_object)
    cvMapDf = cvMapDf[['conversion_value', 'min_event_revenue', 'max_event_revenue']]
    # 将cvMapDf写入MC
    app_package = 'id1479198816'
    name = 'af_cv_map'
    
    levels = cvMapDf['max_event_revenue'].dropna().tolist()
    cvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package, name)
    writeCvMapToMC(cvMapDf, app_package, name)
    
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)
    
    # 生成新的 levels
    name = 'new_cv_map'
    levels = makeLevels(df,usd='revenue',N=N)
    levels = [round(x,2) for x in levels]
    # 将新的 levels 转换为 cvMap 并写入数据库
    newCvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package,name)
    writeCvMapToMC(newCvMapDf,app_package,name)
    
    # 将mape写入MC
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)

    # 使用 KMeans 生成新的 levels
    name = 'new_cv_map_kmeans'
    levels = makeLevelsByKMeans(df,usd='revenue',N=N)
    levels = [round(x,2) for x in levels]
    # 将新的 levels 转换为 cv
    cvMapDf = levelsToCvMap(levels)
    deleteCvMapPartition(app_package,name)
    writeCvMapToMC(cvMapDf,app_package,name)

    # 将cvMapDf写入MC
    mape = checkLevels(df,levels,usd='revenue',cv='cv')
    deleteMapePartition(dayStr,app_package,name)
    writeMapeToMC(mape,dayStr,app_package,name)




if __name__ == '__main__':
    init()
    createCvMapTable()
    createMapeTable()

    topwarMain()
    topherosMain()
    lastwarMain()
    lastwarVNMain()
    print('Done')
import pandas as pd
import os
import sys

import datetime
import time

sys.path.append('/src')
from src.maxCompute import execSql,execSqlBj
from src.tools import getFilename

from src.predSkan.lize.userGroupByR2 import addCV
# 打标签
def makeLabel():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    # 将读csv中的多余索引去掉
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # 打入标签CV1
    cvMapDf1 = pd.read_csv(getFilename('cvMapDf1'))
    df = addCV(df,cvMapDf1,usd = 'r1usd',cvName = 'cv1')

    # 打入标签CV7
    cv1List = list(cvMapDf1['cv'].unique())
    cv1List.sort()

    for cv1 in cv1List:
        cvMapDf7 = pd.read_csv(getFilename('cvMapDf7_%s'%cv1))
        
        df.loc[df.cv1 == cv1,'cv7'] = 0
        cv7List = list(cvMapDf7['cv'].unique())
        cv7List.sort()
        for cv7 in cv7List:
            min_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['min_event_revenue'].values[0]
            max_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['max_event_revenue'].values[0]
            
            df.loc[
                (df.cv1 == cv1) &
                (df['r7usd'] > min_event_revenue) & (df['r7usd'] <= max_event_revenue),
                'cv7'
            ] = cv7
        df.loc[
            (df.cv1 == cv1) &
            (df['r7usd'] > max_event_revenue),
            'cv7'
        ] = len(cvMapDf7)-1
    # df['cv7']转成int
    df['cv7'] = df['cv7'].astype(int)
    # 显示df前几行
    print(df.head())

    labelDf = df[['customer_user_id','cv1','cv7']]

    labelDf.to_csv(getFilename('labelDf'),index = False)

# 将需要的label种提到的uid加入到mc中
def addLabelUidToMC():
    from src.maxCompute import createTableBjTmp,writeTableBjTmp
    labelDf = pd.read_csv(getFilename('labelDf'))
    df = labelDf[['customer_user_id']]
    createTableBjTmp()
    writeTableBjTmp(df)
    return


# 这个sql不能执行，太慢了，主要原因数据太多了
def getFeaturesFromMC1():
    sql = '''
    select 
        uid as customer_user_id,
        event,
        event_count
    from rg_ai_bj.dwd_userfeature_v2_baseevent_windows_day1
    where 
        dt >= '20220701'
        and dt <= '20230201'
    ;
    '''
    df = execSqlBj(sql)
    df.to_csv(getFilename('featuresFromMC1'),index = False)

def getEventNamesFromMC1():
    sql = '''
    select 
        DISTINCT event as event_name
    from rg_ai_bj.dwd_userfeature_v2_baseevent_windows_day1
    where 
        dt >= '20220701'
        and dt <= '20230201'
    ;
    '''
    df = execSqlBj(sql)
    df.to_csv(getFilename('eventNamesFromMC1'),index = False)

# 这个sql也是非常的缓慢，可能需要将大量数据下载到本地导致的
def getFeaturesOneHotFromMC1():
    eventNamesDf = pd.read_csv(getFilename('eventNamesFromMC1'))
    oneHotStr = ''
    for index,row in eventNamesDf.iterrows():
        oneHotStr += 'if(event = "%s",event_count,0) as "%s",\n'%(row['event_name'],row['event_name'])

    start_date = datetime.date(2022, 7, 1)
    end_date = datetime.date(2023, 2, 1)
    delta = datetime.timedelta(days=1)
    current_date = start_date

    while current_date <= end_date:
        dt = current_date.strftime('%Y%m%d')
        print(dt) # The user may want to see the output
        current_date += delta

        sql = '''
            SELECT
                %s
                uid as customer_user_id
            FROM
                dwd_userfeature_v2_baseevent_windows_day1
            JOIN tmp_uid_by_j ON dwd_userfeature_v2_baseevent_windows_day1.uid = tmp_uid_by_j.customer_user_id
            where 
                dt = '%s'
            ;
            '''%(oneHotStr,dt)
        # print(sql)
        # 统计并显示运行时间
        start_time = time.time()
        df = execSqlBj(sql)
        end_time = time.time()
        run_time = end_time - start_time
        print(f"代码运行时间为：{run_time}秒")
        df.to_csv(getFilename('featuresOneHotFromMC1_%s'%(dt)),index = False)

# 上面的还是太慢了，所以还是要用下面的，减少结果量级
def getFeaturesOneHotFromMC2():
    eventNamesDf = pd.read_csv(getFilename('eventNamesFromMC1'))
    oneHotStr = ''
    for index,row in eventNamesDf.iterrows():
        oneHotStr += "WHEN '%s' THEN '%d'\n"%(row['event_name'],index)

    start_date = datetime.date(2022, 7, 1)
    end_date = datetime.date(2023, 2, 1)
    delta = datetime.timedelta(days=1)
    current_date = start_date

    while current_date <= end_date:
        dt = current_date.strftime('%Y%m%d')
        print(dt) # The user may want to see the output
        current_date += delta

        sql = '''
            SELECT
                uid,
                wm_concat(
                    ',',
                    case event 
                        %s
                    else 'unknown'
                    end|| ':' || floor(event_count)) AS events
            FROM dwd_userfeature_v2_baseevent_windows_day1
            where dt = '%s'
            GROUP BY uid
            ;
            '''%(oneHotStr,dt)
        # print(sql)
        # 统计并显示运行时间
        start_time = time.time()
        df = execSqlBj(sql)
        end_time = time.time()
        run_time = end_time - start_time
        print(f"代码运行时间为：{run_time}秒")
        df.to_csv(getFilename('featuresOneHotFromMC2_%s'%(dt)),index = False)


# 将所有featuresOneHotFromMC1_*开头的csv文件合并成一个，文件都比较大，要尽量省内存
def mergeFeaturesOneHotFromMC1():
    import glob

    # 获取所有符合模式(featuresOneHotFromMC1_开头)的CSV文件路径
    all_files = glob.glob("/src/data/featuresOneHotFromMC2_*.csv")

    # 将所有CSV文件读入内存，合并
    dfs = []
    for filename in all_files:
        df = pd.read_csv(filename)
        dfs.append(df)
    merged_df = pd.concat(dfs, ignore_index=True)

    # 将合并后的DataFrame写入CSV文件
    merged_df.to_csv("/src/data/merged_featuresOneHotFromMC2.csv", index=False)

# 
def getFeaturesFromKV(df):
    # df 里面有两列，uid和events
    # 其中events里面是string，格式类似于：'event1:count1,event2:count2,...'
    # 本函数的目的是将events列拆分成多列，每一列是一个event，值是count
    # 例如：'event1:count1,event2:count2,...'拆分成event1列和event2列，值分别是count1和count2
    # 本函数的返回值是一个新的DataFrame，包含了原来的uid列和拆分后的event列

    events = df['events'].str.split(',', expand=True)
    events = events.apply(lambda x: x.str.split(':', expand=True))
    events.columns = events.columns.map(lambda x: f"event{x[0]}")
    events = events.astype(int)
    return pd.concat([df['uid'], events], axis=1)

# 降低内存使用，这是问了gpt4的答复，如果还不行就只能换方案了
def getFeaturesFromKVGPT4(df):
    events_list = df['events'].str.split(',')
    # 将每个字符串按冒号拆分成两个部分
    events_list = [x.split(':') for y in events_list for x in y]

    # 合并 event 和 count 两个列表
    event_list = [x[0] for x in events_list]
    count_list = [x[1] for x in events_list]

    # 转换为 DataFrame
    new_df = pd.DataFrame({'uid': df['uid'].repeat(len(event_list)), 'event': event_list * len(df), 'count': count_list * len(df)})
    return new_df

# def getFeaturesFromKVGPT4Chunk(fromFile,toFile):
# fromFile 是csv文件 里面有两列，uid和events
# toFile 是目标csv文件
# 其中events里面是string，格式类似于：'event1:count1,event2:count2,...'
# 本函数的目的是将events列拆分成多列，每一列是一个event，值是count
# 例如：'event1:count1,event2:count2,...'拆分成event1列和event2列，值分别是count1和count2
# 本函数的返回值是一个新的，包含了原来的uid列和拆分后的event列
# 要求采用流的形式处理，不能一次性读入内存

def getFeaturesFromKVGPT4Chunk(fromFile,toFile):
    # 按流处理数据
    chunksize = 1000
    reader = pd.read_csv(fromFile, chunksize=chunksize)
    for chunk in reader:
        # 拆分 events 列
        events_list = chunk['events'].str.split(',')

        # 将每个字符串按冒号拆分成两个部分
        events_list = [x.split(':') for y in events_list for x in y]

        # 合并 event 和 count 两个列表
        event_list = [x[0] for x in events_list]
        count_list = [x[1] for x in events_list]

        # 转换为 DataFrame
        new_df = pd.DataFrame({'uid': chunk['uid'].repeat(len(event_list)), 'event': event_list * len(chunk), 'count': count_list * len(chunk)})

        # 保存到 csv 文件中
        new_df.to_csv(toFile, mode='a', header=False, index=False)

def mergeFeaturesOneHotToLabel():
    label_df = pd.read_csv(getFilename('labelDf'))
    feature_df = pd.read_csv(getFilename('merged_featuresOneHotFromMC1'))
    merged_df = label_df.merge(feature_df, how='left', on='customer_user_id')

    merged_df.to_csv(getFilename('merged_featuresOneHotToLabel'),index = False)

# df是一个完整的DF，必须包括列：customer_user_id,cv1,cv7,features
# df可以先通过cv1过滤，这样只针对一种cv1，进行cv7分类
# 除了customer_user_id,cv1,cv7，剩下的列都被认为是特征
def getXY(df):
    # 按照cv7分类
    y = df['cv7'].to_numpy().reshape(-1,1)

    # 深度拷贝一份df
    dfCopy = df.copy(deep = True)

    # 去掉customer_user_id,cv1,cv7
    dfCopy = dfCopy.drop(['customer_user_id','cv1','cv7'],axis = 1,inplace = True)

    # 转成numpy
    x = dfCopy.to_numpy()

    return x,y

# 获得特征
# N是特征的个数
def getFeatures(x,y,N = 10):

    from sklearn.feature_selection import SelectKBest, f_classif
    # 特征选择：使用相关系数法选择10个最相关的特征
    selector = SelectKBest(score_func=f_classif, k=10)
    selector.fit(x, y)
    # 获取选择的特征的下标
    selected_features = selector.get_support(indices=True)

    # 打印选择的特征
    print("Selected features: ", selected_features)

    scores = selector.scores_
    selected_scores = scores[selected_features]
    print("Selected features' scores: ", selected_scores)

    return
# labelDf 列：customer_user_id,cv7
# featuresFromMC1 列：customer_user_id,event,event_count，其中event是事件名称，string类型
# 用featuresFromMC1的特征和labelDf的cv7标签 做 决策树 多分类

if __name__ == '__main__':
    # makeLabel()
    # getEventNamesFromMC1()
    # getFeaturesOneHotFromMC1()
    # getFeaturesOneHotFromMC2()
    # addLabelUidToMC()
    # mergeFeaturesOneHotFromMC1()

    # df = getFeaturesFromKV(pd.read_csv('/src/data/featuresOneHotFromMC2_20220701.csv'))
    # df = getFeaturesFromKVGPT4(pd.read_csv('/src/data/featuresOneHotFromMC2_20220701.csv'))
    # df.to_csv('/src/data/featuresOneHotFromMC2_20220701_fix.csv',index = False)

    getFeaturesFromKVGPT4Chunk('/src/data/featuresOneHotFromMC2_20220701.csv','/src/data/featuresOneHotFromMC2_20220701_fix.csv')
import pandas as pd
import os
import sys

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
    sql = '''
        SELECT
            %s
            uid as customer_user_id
        FROM
            rg_ai_bj.dwd_userfeature_v2_baseevent_windows_day1
        where 
            dt >= '20220701'
            and dt <= '20230201'
        ;
        '''%(oneHotStr)
    print(sql)
    df = execSqlBj(sql)
    df.to_csv(getFilename('featuresOneHotFromMC1'),index = False)

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
    getFeaturesOneHotFromMC1()
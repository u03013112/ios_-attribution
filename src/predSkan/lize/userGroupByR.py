# 还是不习惯ipynb的感受
import pandas as pd
import os
import sys

from sklearn.metrics import r2_score,mean_absolute_percentage_error

sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

# 分组的目的是有效的降低线性偏差
# 即分组之后的用户具有更加线性的状态
# 直接乘以倍率就可以较为准确的预测7日收入

# 所以需要评分的标准应该是分组前的MAPE与R2，和分组后的MAPE与R2
# 但是大盘总体的线性相关性过高，分组反而导致MAPE与R2下降
# 所以需要分媒体，只有安卓可以分媒体统计7日收入，所以用安卓来做范例
# 分媒体暂时只看几个大媒体：FB，GG，TT

def getDataFromMC():
    sql = '''
        select
            customer_user_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                    when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r1usd,
            sum(
                case
                    when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r7usd,
            media_source as media
        from
            ods_platform_appsflyer_events
        where
            app_id = 'com.topwar.gp'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= 20220701
            and day <= 20230201
        group by
            install_date,
            customer_user_id,
            media_source
    '''

    df = execSql(sql)
    return df

# 获取原始数据，需要跑mc，比较慢，可以跳过
def step1():
    df = getDataFromMC()
    df.to_csv(getFilename('aosCvR1R7Media_20220701_20230201'))

# score1 : 简单的拟合 y = w * x +b
# 这种方式可以解决x = 0 无法计算mape和r2的问题
def score1(df,r1usd = 'r1usd',r7usd = 'r7usd',cv1 = 'cv1',cv7 = 'cv7'):
    cv1List = list(df[cv1].unique())
    cv7List = list(df[cv7].unique())
    cv1List.sort()
    cv7List.sort()
    # print(cv1List,cv7List)
    for cv1 in cv1List:
        for cv7 in cv7List:
            cvDf = df.loc[(df.cv1 == cv1) & (df.cv7 ==cv7)]
            r1usdMean = cvDf[r1usd].mean()
            r7usdMean = cvDf[r7usd].mean()
            if r1usdMean != 0:
                w = r7usdMean/r1usdMean
            else:
                w = 0
            b = r7usdMean - r1usdMean * w
            df.loc[(df.cv1 == cv1) & (df.cv7 ==cv7),'w'] = w
            df.loc[(df.cv1 == cv1) & (df.cv7 ==cv7),'b'] = b

    df.loc[:,'%sp'%r7usd] = df[r1usd]*df['w']+df['b']
    # print(df)
    # print('saved')
    # df.to_csv('/src/data/tmp.csv')

    y = df[r7usd]
    yp = df['%sp'%r7usd]
    try:
        mape = mean_absolute_percentage_error(y,yp)
        r2 = r2_score(y,yp)
    except:
        mape = 0
        r2 = 0

    return mape,r2

def score1Print(message,df,r1usd = 'r1usd',r7usd = 'r7usd',cv1 = 'cv1',cv7 = 'cv7'):
    mape,r2 = score1(df,r1usd,r7usd,cv1,cv7)
    retStr = '%s MAPE:%.2f%% R2:%.2f'%(message,mape*100,r2)
    print(retStr)
    return mape,r2

mediaList = [
    {'name':'bytedance','codeList':['bytedanceglobal_int'],'sname':'Bd'},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads','facebook','FacebookAds'],'sname':'Fb'},
    {'name':'google','codeList':['googleadwords_int'],'sname':'Gg'},
    {'name':'unknown','codeList':[],'sname':'Og'}
]

def addMediaGroup(df):
    # Initialize the media_group column with default value 'unknown'
    df.loc[:,'media_group'] = 'unknown'

    # Iterate through the mediaList and update the media_group column accordingly
    for group in mediaList:
        df.loc[df['media'].isin(group['codeList']), 'media_group'] = group['name']
    return df

# 原始数据评分
# TODO:添加人数
# TODO:添加中位数
def rawScore(df = None):
    if df is None:
        df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    
    # 原始数据就是不分组
    df['cv1'] = 0
    df['cv7'] = 0
    line = []
    print('原始数据评分:')
    mape,r2 = score1Print('用户为单位计算',df)
    line.append(mape)
    line.append(r2)
    groupByDayDf = df.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum'
    })
    mape,r2 = score1Print('按天汇总计算',groupByDayDf)
    line.append(mape)
    line.append(r2)

    # 分媒体
    mediaDf = addMediaGroup(df)
    for media in mediaList:
        mediaName = media['name']
        mediaDf0 = mediaDf.loc[mediaDf.media_group == mediaName]
        mediaDf1 = mediaDf0.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
            'r1usd':'sum',
            'r7usd':'sum'
        })
        mape,r2 = score1Print('%s 按天汇总计算'%(mediaName),mediaDf1)
        line.append(mape)
        line.append(r2)

    return line

# 按照cvMap添加cv到cvName列
def addCV(df,cvMapDf,usd='r1usd',cvName = 'cv'):
    df.loc[:,cvName] = 0
    for i in range(len(cvMapDf)):
        min_event_revenue = cvMapDf.min_event_revenue[i]
        max_event_revenue = cvMapDf.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        df.loc[
            (df[usd] > min_event_revenue) & (df[usd] <= max_event_revenue),
            cvName
        ] = i
    df.loc[
        (df[usd] > max_event_revenue),
        cvName
    ] = len(cvMapDf)-1
    return df

# 分组后的数据评分
# 这里的分组是按照r1和r7金额进行分组
# 分组后将每个分组分别计算w与b，计算yp，之后将所有yb进行汇总，然后计算score
def groupScore(df,cvMapDf1,cvMapDf7):
    line = []
    df = addCV(df,cvMapDf1,usd = 'r1usd',cvName = 'cv1')
    df = addCV(df,cvMapDf7,usd = 'r7usd',cvName = 'cv7')
    
    print('分组数据评分:')
    mape,r2 = score1Print('用户为单位计算',df)
    line += [mape,r2]

    groupByDayDf = df.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum'
    })
    mape,r2 = score1Print('按天汇总计算',groupByDayDf)
    line += [mape,r2]
    # 分媒体
    mediaDf = addMediaGroup(df)
    for media in mediaList:
        mediaName = media['name']
        mediaDf0 = mediaDf.loc[mediaDf.media_group == mediaName]
        mediaDf1 = mediaDf0.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
            'r1usd':'sum',
            'r7usd':'sum'
        })
        mape,r2 = score1Print('%s 按天汇总计算'%(mediaName),mediaDf1)
        line += [mape,r2]

    return line

def makeCvMap(levels):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[0]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
        mapData['max_event_revenue'].append(levels[i])

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def makeLevels1(userDf,usd = 'r1usd',N = 7):
    df = userDf.sort_values([usd])
    # Filter out users with usd <= 0
    filtered_df = df[df[usd] > 0]

    # Calculate the total usd for all users
    total_usd = filtered_df[usd].sum()

    # Calculate the target usd for each group
    target_usd = total_usd / N

    # Initialize the levels array with zeros
    levels = [0] * (N - 1)

    # Initialize the current usd and group index
    current_usd = 0
    group_index = 0

    # Loop through each user and assign them to a group
    for index, row in filtered_df.iterrows():
        current_usd += row[usd]
        if current_usd >= target_usd:
            levels[group_index] = row[usd]
            current_usd = 0
            group_index += 1
            if group_index == N - 1:
                break

    return levels

def checkCvMap(userDf,cvMapDf,usd = 'r1usd'):
    import copy
    df = copy.deepcopy(userDf)
    df.loc[:,'cv'] = 0
    for i in range(len(cvMapDf)):
        min_event_revenue = cvMapDf.min_event_revenue[i]
        max_event_revenue = cvMapDf.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        df.loc[
            (df[usd] > min_event_revenue) & (df[usd] <= max_event_revenue),
            'cv'
        ] = i
    df.loc[
        (df[usd] > max_event_revenue),
        'cv'
    ] = len(cvMapDf)-1

    df.loc[:,'cv_usd'] = 0
    for i in range(len(cvMapDf)):
        min_event_revenue = cvMapDf.min_event_revenue[i]
        max_event_revenue = cvMapDf.max_event_revenue[i]
        avg = (min_event_revenue + max_event_revenue)/2
        if pd.isna(max_event_revenue):
            avg = 0
        if avg < 0:
            avg = 0
        df.loc[df.cv == i,'cv_usd'] = avg
    
    # print(df)
    mergeDf = df.groupby('install_date',as_index=False).agg({usd:'sum','cv_usd':'sum'})
    # print(mergeDf)
    # 计算mergeDf中usd列与'cv_usd'列的mape 和 r2_score
    from sklearn.metrics import mean_absolute_percentage_error, r2_score

    mape = mean_absolute_percentage_error(mergeDf[usd], mergeDf['cv_usd'])
    r2 = r2_score(mergeDf[usd], mergeDf['cv_usd'])

    print(f"MAPE: {mape}")
    print(f"R2 Score: {r2}")

def main():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    
    head = ['r1usd 分为N组','r7usd 分为M组','共计NxM组','用户为单位计算MAPE','用户为单位计算R2','按天汇总计算MAPE','按天汇总计算R2','bytedance MAPE','bytedance R2','facebook MAPE','facebook R2','google MAPE','google R2','unknown MAPE','unknown R2']

    line = []
    line += [1,1,1]
    line += rawScore(df)
    # line += '\n'
    # 将用户进行不同分组，然后确认分组结论
    lines = [head,line]
    
    for n1,n7 in (
        (4,8),(4,12),(4,16),(4,20),(4,24),
        (6,8),(6,12),(6,16),(6,20),(6,24),
        (8,8),(8,12),(8,16),(8,20),(8,24)
        ):
        line = []

        levels1 = makeLevels1(df,usd = 'r1usd',N=n1)
        levels7 = makeLevels1(df,usd = 'r7usd',N=n7)

        cvMapDf1 = makeCvMap(levels1)
        cvMapDf7 = makeCvMap(levels7)

        print('r1usd 分为%d组，r7usd 分为%d组，共计 %d组'%(n1,n7,n1*n7))
        # print('r1usd 分组金额：',levels1)
        line += [n1,n7,n1*n7]

        # checkCvMap(df,cvMapDf1,usd = 'r1usd')
        # print('r7usd 分组金额：',levels7)
        # checkCvMap(df,cvMapDf7,usd = 'r7usd')

        # cvMapDf1.to_csv(getFilename('cvMap1'))
        # cvMapDf7.to_csv(getFilename('cvMap7'))

        line += groupScore(df,cvMapDf1,cvMapDf7)

        # line += '\n'
        lines.append(line)

    return lines

from src.googleSheet import GSheet
if __name__ == '__main__':
    # step1()
    # rawScore()
    # groupScore()
    lines = main()

    GSheet().clearSheet('1111','Sheet2')
    GSheet().updateSheet('1111','Sheet2','A1',lines)
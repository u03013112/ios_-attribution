# 与userGroupByR.py的区别是，这个不再采用收日收入倍率来做校准
# 直接用分组区间的平均值来做校准，看看是否有更好的效果
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

def score1(df,cvMapDf1,cvMapDf7,r1usd = 'r1usd',r7usd = 'r7usd',cv1 = 'cv1',cv7 = 'cv7'):
    cv1List = list(df[cv1].unique())
    cv7List = list(df[cv7].unique())
    cv1List.sort()
    cv7List.sort()

    for cv1 in cv1List:
        for cv7 in cv7List:
            min_event_revenue = cvMapDf1.min_event_revenue[cv1]
            max_event_revenue = cvMapDf1.max_event_revenue[cv1]

            if pd.isna(max_event_revenue):
                avg = 0
            else:
                avg = (min_event_revenue + max_event_revenue) / 2
                if avg < 0:
                    avg = 0
            df.loc[(df.cv1 == cv1) & (df.cv7 ==cv7),'%sp'%r1usd] = avg * df['count']

            min_event_revenue = cvMapDf7.min_event_revenue[cv7]
            max_event_revenue = cvMapDf7.max_event_revenue[cv7]

            if pd.isna(max_event_revenue):
                avg = 0
            else:
                avg = (min_event_revenue + max_event_revenue) / 2
                if avg < 0:
                    avg = 0
            df.loc[(df.cv1 == cv1) & (df.cv7 ==cv7),'%sp'%r7usd] = avg * df['count']

    y1 = df[r1usd]
    y1p = df['%sp'%r1usd]

    y7 = df[r7usd]
    y7p = df['%sp'%r7usd]
    try:
        mape1 = mean_absolute_percentage_error(y1,y1p)
        r2Score1 = r2_score(y1,y1p)

        mape7 = mean_absolute_percentage_error(y7,y7p)
        r2Score7 = r2_score(y7,y7p)
    except:
        mape1 = 0
        r2Score1 = 0

        mape7 = 0
        r2Score7 = 0

    return mape1,r2Score1,mape7,r2Score7

def score1Print(message,df,cvMapDf1,cvMapDf7,r1usd = 'r1usd',r7usd = 'r7usd',cv1 = 'cv1',cv7 = 'cv7'):
    mape1,r2Score1,mape7,r2Score7 = score1(df,cvMapDf1,cvMapDf7,r1usd,r7usd,cv1,cv7)
    print(df)
    # retStr = '%s MAPE:%.2f%% R2:%.2f'%(message,mape*100,r2)
    # print(retStr)
    return mape1,r2Score1,mape7,r2Score7

mediaList = [
    {'name':'bytedance','codeList':['bytedanceglobal_int'],'sname':'Bd'},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads','facebook','FacebookAds'],'sname':'Fb'},
    {'name':'google','codeList':['googleadwords_int'],'sname':'Gg'},
    {'name':'unknown','codeList':[],'sname':'Og'}
]

# 添加媒体分组函数，codeGPT版本
# def addMediaGroupGPT(df): df是pandas dataframe，根据列'media'找到匹配的媒体组并记录到列'media_group'
# 对应方法是用 'media' 列内容匹配 在mediaList里的'codeList'，匹配到的媒体组名字'name',记录到 'media_group' 列，如果没有匹配到，记录为 'unknown'
def addMediaGroupGPT(df):
    def get_media_group(row):
        for media in mediaList:
            if row['media'] in media['codeList']:
                return media['name']
        return 'unknown'
    df['media_group'] = df.apply(get_media_group, axis=1)
    return df
# 以上代码是code gpt生成，略作修改，很有趣

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
    mape1,r2Score1,mape7,r2Score7 = score1Print('用户为单位计算',df)
    line += [mape1,r2Score1,mape7,r2Score7]
    
    groupByDayDf = df.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum'
    })
    mape1,r2Score1,mape7,r2Score7 = score1Print('按天汇总计算',groupByDayDf)
    line += [mape1,r2Score1,mape7,r2Score7]

    # 分媒体
    mediaDf = addMediaGroup(df)
    for media in mediaList:
        mediaName = media['name']
        mediaDf0 = mediaDf.loc[mediaDf.media_group == mediaName]
        mediaDf1 = mediaDf0.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
            'r1usd':'sum',
            'r7usd':'sum'
        })
        mape1,r2Score1,mape7,r2Score7 = score1Print('%s 按天汇总计算'%(mediaName),mediaDf1)
        line += [mape1,r2Score1,mape7,r2Score7]

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
    mape1,r2Score1,mape7,r2Score7 = score1Print('用户为单位计算',df,cvMapDf1,cvMapDf7)
    line += [mape1,r2Score1,mape7,r2Score7]

    groupByDayDf = df.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
        'count':'sum',
        'r1usd':'sum',
        'r7usd':'sum'
    })
    mape1,r2Score1,mape7,r2Score7 = score1Print('按天汇总计算',groupByDayDf,cvMapDf1,cvMapDf7)
    line += [mape1,r2Score1,mape7,r2Score7]
    # 分媒体
    mediaDf = addMediaGroup(df)
    for media in mediaList:
        mediaName = media['name']
        mediaDf0 = mediaDf.loc[mediaDf.media_group == mediaName]
        mediaDf1 = mediaDf0.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
            'count':'sum',
            'r1usd':'sum',
            'r7usd':'sum'
        })
        mape1,r2Score1,mape7,r2Score7 = score1Print('%s 按天汇总计算'%(mediaName),mediaDf1,cvMapDf1,cvMapDf7)
        line += [mape1,r2Score1,mape7,r2Score7]

    return line

def makeCvMap(levels,min = 0):
    mapData = {
        'cv':[0],
        'min_event_revenue':[-1],
        'max_event_revenue':[min]
    }
    for i in range(len(levels)):
        mapData['cv'].append(len(mapData['cv']))
        mapData['min_event_revenue'].append(mapData['max_event_revenue'][len(mapData['max_event_revenue'])-1])
        mapData['max_event_revenue'].append(levels[i])

    cvMapDf = pd.DataFrame(data=mapData)
    return cvMapDf

def makeLevels1(userDf,usd = 'r1usd',N = 7):    
    df = userDf.sort_values([usd])
    # 这是为了解决7日回收分组时少一组的情况（将0~最小值划分为0组，但是这一组应该是没有人的）
    if df[usd].min() > 0:
        print('min:',df[usd].min())
        N += 1

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
    print(mergeDf)
    # 计算mergeDf中usd列与'cv_usd'列的mape 和 r2_score
    from sklearn.metrics import mean_absolute_percentage_error, r2_score

    mape = mean_absolute_percentage_error(mergeDf[usd], mergeDf['cv_usd'])
    r2 = r2_score(mergeDf[usd], mergeDf['cv_usd'])

    print(f"MAPE: {mape}")
    print(f"R2 Score: {r2}")
    return mape,r2

def main():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    df.loc[:,'count'] = 1
    head = ['r1usd 分为N组','r7usd 分为M组','共计NxM组',
            '用户为单位计算MAPE 1','用户为单位计算R2 1','用户为单位计算MAPE 7','用户为单位计算R2 7',
            '按天汇总计算MAPE 1','按天汇总计算R2 1','按天汇总计算MAPE 7','按天汇总计算R2 7',
            'bytedance MAPE 1','bytedance R2 1','bytedance MAPE 7','bytedance R2 7',
            'facebook MAPE 1','facebook R2 1','facebook MAPE 7','facebook R2 7',
            'google MAPE 1','google R2 1','google MAPE 7','google R2 7',
            'unknown MAPE 1','unknown R2 1','unknown MAPE 7','unknown R2 7']

    line = []
    # line += [1,1,1]
    # line += rawScore(df)
    # line += '\n'
    # 将用户进行不同分组，然后确认分组结论
    lines = [head,line]
    
    for n1,n7 in (
        (4,8),(4,12)
        ,(4,16),(4,20),(4,24),
        (6,8),(6,12),(6,16),(6,20),(6,24),
        (8,8),(8,12),(8,16),(8,20),(8,24)
        ):
        line = []

        levels1 = makeLevels1(df,usd = 'r1usd',N=n1)
        levels7 = makeLevels1(df,usd = 'r7usd',N=n7)

        print(levels1)
        print(levels7)

        cvMapDf1 = makeCvMap(levels1)
        cvMapDf7 = makeCvMap(levels7)

        print('r1usd 分为%d组，r7usd 分为%d组，共计 %d组'%(n1,n7,n1*n7))
        # print('r1usd 分组金额：',levels1)
        line += [n1,n7,n1*n7]

        # checkCvMap(df,cvMapDf1,usd = 'r1usd')
        # # print('r7usd 分组金额：',levels7)
        # checkCvMap(df,cvMapDf7,usd = 'r7usd')

        # cvMapDf1.to_csv(getFilename('cvMap1'))
        # cvMapDf7.to_csv(getFilename('cvMap7'))

        line += groupScore(df,cvMapDf1,cvMapDf7)

        # line += '\n'
        lines.append(line)

    return lines

# 测试CV对大盘和媒体的分别影响
def cvCheck():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    df = addMediaGroup(df)

    lines = []
    head = ['N','Media','MAPE','R2']
    lines.append(head)
    for n1 in (64,32,16):
        line = [n1,'total']
        levels1 = makeLevels1(df,usd = 'r1usd',N=n1)
        cvMapDf1 = makeCvMap(levels1)
        print(cvMapDf1)
        cvMapDf1.to_csv(getFilename('cvMap1_%d'%n1))
        mape,r2 = checkCvMap(df,cvMapDf1,usd = 'r1usd')
        line += [mape,r2]
        lines.append(line)

        for media in mediaList:
            mediaName = media['name']
            line = [n1,mediaName]
            mediaDf = df[df.media_group == mediaName]
            mape,r2 = checkCvMap(mediaDf,cvMapDf1,usd = 'r1usd')
            line += [mape,r2]
            lines.append(line)

    return lines
        
        





from src.googleSheet import GSheet
if __name__ == '__main__':
    # step1()
    # rawScore()
    # groupScore()


    # lines = main()

    # GSheet().clearSheet('1111','Sheet2')
    # GSheet().updateSheet('1111','Sheet2','A1',lines)

    lines = cvCheck()
    GSheet().clearSheet('1111','Sheet3')
    GSheet().updateSheet('1111','Sheet3','A1',lines)
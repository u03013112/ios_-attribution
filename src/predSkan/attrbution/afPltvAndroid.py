# 按照AF的方案做类似尝试，用安卓先测试
import copy
import datetime
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename,afCvMapDataFrame

# 获得安卓用户信息
# afid + media + campaign + r1usd + r7usd
# 暂时可以忽略campaign，先尝试到分媒体就好。
def getDataFromAF():
    sql = '''
        select
            appsflyer_id,
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
            and zone = 0
            and day >= 20221001
            and day <= 20230205
            and install_time >= '2022-10-01'
            and install_time < '2023-02-01'
        group by
            install_date,
            appsflyer_id,
            media
        ;
    '''
    df = execSql(sql)
    return df

# 将首日付费按照目前的地图映射到CV
def addCV(userDf,mapDf = None):
    userDf.loc[:,'cv'] = 0
    if mapDf is None:
        map = afCvMapDataFrame
    else:
        map = mapDf
    for i in range(len(map)):
        min_event_revenue = map.min_event_revenue[i]
        max_event_revenue = map.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        userDf.loc[
            (userDf.r1usd > min_event_revenue) & (userDf.r1usd <= max_event_revenue),
            'cv'
        ] = i
    userDf.loc[
        (userDf.r1usd > max_event_revenue),
        'cv'
    ] = len(map)-1

    return userDf

# 随机的忽略掉80%的用户ID信息，这里改为给这些用户一个标记，标记为需要预测用户
# frac 是可归因的用户占比
def emuSKAN(df,frac = .2):
    userDf = copy.deepcopy(df)
    userDf.loc[:,'idfa'] = 0
    sampleDf = userDf.sample(frac = frac)
    userDf.loc[sampleDf.index,'idfa'] = 1
    return userDf
    
# 在相同的范围内，在一定的时间内进行用户随机抽取，用以获得类似的CV分布
# 时间可以从 T-0 ~ T-30 中间定几个档位都试试
# 精度部分可以尝试精确到百分之一，千分之一之间做调整。看看有没有可能在这中间找到更加合适的档位。
def getSimilarUser(userDf,exampleDf,n=100):
    # n 是获取多少个类似用户，n也代表着精度，按照cv分布来算的话，如果n=100，那么精度也只能到1%，不到1%的cv分布会被忽略掉
    # n 也是有上限的，在用户分群里，选定类似用户范围，比如媒体，然后锁定时间范围，比如一周内，用户数就是n的上限
    # 另外 n设定过大也是不现实的，比如希望有足够多的大R，在范围内找不出足够多的大R，就只能将大R复制，这种方案可以尝试
    # 由于拥有idfa的用户还是少数，所以这种挑选数量不宜太多，获取应该设定一个比例，即与待预测用户数的一个比例。这个比例不宜太高也不易太低，比如就按照idfa比例来，就20%或者10%

    exampleUserCount = exampleDf['count'].sum()
    
    retDf = None
    isFirst = True

    for cv in range(64):
        rate = exampleDf.loc[exampleDf.cv == cv,'count'].sum()/exampleUserCount
        c = round(n * rate)
        # print(cv,rate,c)
        if c > 0:
            cvDf = userDf.loc[userDf.cv == cv]
            if len(cvDf) <= 0:
                # 这种情况好尴尬
                continue
            # print(c,len(cvDf))
            if c >= len(cvDf):
                s = cvDf.sample(c,replace=True)
            else:
                s = cvDf.sample(c)
            if isFirst :
                isFirst = False
                retDf = s
            else:
                retDf = retDf.append(s,ignore_index=True)
    
    # print('AAA:',len(retDf))
    return retDf
            
# 计算结果，并记录日志
# 格式是 n,t,mape
# n 是找到多少相似用户
# t 是从最近多久的用户找，0代表只找当天的
# idfa 是idfa用户比例，可以考虑从20%~30%之间
def createOneLine(getSimilarUserRetDf,exampleDf,message):
    r1usd = getSimilarUserRetDf['r1usd'].sum()
    if r1usd <= 0:
        r1usd = 1.0
    r7usd = getSimilarUserRetDf['r7usd'].sum()

    p71 = r7usd/r1usd

    # 做一些限定，用于减少误差
    if p71 < 1:
        p71 = 1
    
    # 10倍是随意想出来的，先暂定10倍吧
    if p71 >10:
        p71 = 10

    py = exampleDf['r1usd'].sum() * p71
    y = exampleDf['r7usd'].sum()
    mape = (py - y)/y
    if mape < 0:
        mape *= -1

    return '%s,%f,%f,%f\n'%(message,mape,py,y)

# 主题流程，将上述步骤重复N次
# 输入df是添加完cv的DataFrame
def main(df):
    
    logFile = '/src/data/doc/cv/afPltv1.csv'

    with open(logFile, 'w') as f:
        f.write('install_date,sample_n,t,idfa,media,mape,py,y\n')

    df.loc[:,'count'] = 1

    # 暂时只看着3个媒体
    mediaList = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int'
    ]

    for _ in range(10):
        # 整体多来几次，看看是否取均值会更加稳定
        for idfa in (.2,.25,.3,.35):
            idfaDf = emuSKAN(df,idfa)

            for media in mediaList:

                sinceTime = datetime.datetime.strptime('20221001','%Y%m%d')
                unitlTime = datetime.datetime.strptime('20230201','%Y%m%d')
                for i in range((unitlTime - sinceTime).days):
                    day = sinceTime + datetime.timedelta(days=i)
                    dayStr = day.strftime('%Y-%m-%d')
                    # 获得待预测用户数据（CV分布）
                    exampleDf = idfaDf.loc[
                        (idfaDf.install_date == dayStr)
                        & (idfaDf.media == media)
                        & (idfaDf.idfa == 0)
                    ]
                    exampleDf = exampleDf.groupby(['cv'],as_index=False).agg({
                        'r1usd':'sum',
                        'r7usd':'sum',
                        'count':'sum'
                    }).sort_values(by = ['cv'])

                    for t in (0,7,14,30):
                        installDate0 = day - datetime.timedelta(days=t)
                        installDate0Str = installDate0.strftime('%Y-%m-%d')
                        # 获得准备用户数据库
                        userDf = idfaDf.loc[
                            (idfaDf.install_date >= installDate0Str)
                            & (idfaDf.install_date <= dayStr)
                            & (idfaDf.media == media)
                            & (idfaDf.idfa == 1)
                        ]
                        for n in (100,200,300,400,500,1000,2000,3000,4000,5000):
                            getSimilarUserRetDf = getSimilarUser(userDf,exampleDf,n=n)
                            message = '%s,%d,%d,%.2f,%s'%(dayStr,n,t,idfa,media)
                            line = createOneLine(getSimilarUserRetDf,exampleDf,message)
                            print(line)
                            with open(logFile, 'a') as f:
                                f.write(line)


# 日志部分，要将上述步骤可以有效的记录下来

if __name__ == '__main__':
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getDataFromAF()
        df.to_csv(getFilename('androidUserData20221001_20230201'))

    df = pd.read_csv(getFilename('androidUserData20221001_20230201'))
    df = addCV(df)
    # df = emuSKAN(df)

    main(df)

    
    # df.to_csv(getFilename('androidUserData20221001_20230201_'))

    # 尝试看看每天的用户idfa含量
    # df.loc[:,'count'] = 1
    # df = df.groupby(['install_date'],as_index=False).agg({
    #     'count':'sum',
    #     'idfa':'sum'
    # })

    # df.loc[:,'idfa_rate']=df['idfa']/df['count']
    # print(df)

    # 获得待预测用户数据（CV分布）
    # df.loc[:,'count'] = 1
    # exampleDf = df.loc[
    #     (df.install_date == '2022-10-01')
    #     & (df.media == 'googleadwords_int')
    #     & (df.idfa == 0)
    # ]
    # exampleDf = exampleDf.groupby(['cv'],as_index=False).agg({
    #     'r1usd':'sum',
    #     'r7usd':'sum',
    #     'count':'sum'
    # }).sort_values(by = ['cv'])
    
    # # 获得准备用户数据库
    # userDf = df.loc[
    #     (df.install_date == '2022-10-01')
    #     & (df.media == 'googleadwords_int')
    #     & (df.idfa == 1)
    # ]

    # print(getSimilarUser(userDf,exampleDf))
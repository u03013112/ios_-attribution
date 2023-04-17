# 计算安卓平台的用户分群档位与对应的MAPE与R2
# 使用目前安卓数据进行计算
# 使用削减付费用户中付费金额最高的1%用户进行计算
# 使用削减付费用户中付费金额最高的2%用户进行计算

import pandas as pd
import os
import sys

from sklearn.metrics import r2_score,mean_absolute_percentage_error

sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename
from src.predSkan.lize.retCheck2 import makeLevels1,makeCvMap,cvMapFixAvg1,addCv,check


# 数据获得，安卓海外，2022-10-01~2023-04-01，共6个月
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
            and day >= 20221001
            and day <= 20230401
        group by
            install_date,
            customer_user_id,
            media_source
    '''

    df = execSql(sql)
    return df

def saveData(df):
    df.to_csv(getFilename('android_20221001_20230401'),index=False)

def loadData():
    df = pd.read_csv(getFilename('android_20221001_20230401'))
    # df 列customer_user_id 改名为 uid
    df.rename(columns={'customer_user_id':'uid'},inplace=True)
    # df 列install_date 改名为 dt
    df.rename(columns={'install_date':'dt'},inplace=True)
    # df 只保留dt >= '2022-10-01'的行
    df = df[df['dt']>='2022-10-01']

    return df

# 旧数据获得，安卓海外，2022-07-01~2023-02-01，共7个月
def loadDataOld():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    # df 列customer_user_id 改名为 uid
    df.rename(columns={'customer_user_id':'uid'},inplace=True)
    # df 列install_date 改名为 dt
    df.rename(columns={'install_date':'dt'},inplace=True)
    # df 只保留dt >= '2022-10-01'的行
    df = df[df['dt']>='2022-10-01']

    return df

def nerfMaxR(rate=0.01):
    df = loadData()
    # 找到df中，r1usd>0中的 (1-rate)分位数
    df = df.loc[df['r1usd']>0]
    print('total r1usd pay user:',len(df))
    maxR = df['r1usd'].quantile(1-rate)
    print('r1usd %.2f%%:'%((1-rate)*100),maxR)
    # 打印大于(1-rate)分位数的行数
    print('nerf:',len(df.loc[df['r1usd']>maxR]))
    # 将df中，r1usd>0中的 (1-rate)分位数以上的行 r1usd = (1-rate)分位数
    # 在实际变化之前，计算r1usd的损失值，占r1usd金额总和的比例
    loss = (df.loc[df['r1usd']>maxR,'r1usd'].sum()-maxR*len(df.loc[df['r1usd']>maxR]))/df['r1usd'].sum()
    print('nerf r1usd loss:',loss)

    df.loc[df['r1usd']>maxR,'r1usd'] = maxR

    df = df.loc[df['r7usd']>0]
    print('total r7usd pay user:',len(df))
    maxR = df['r7usd'].quantile(1-rate)
    print('r7usd %.2f%%:'%((1-rate)*100),maxR)
    print('nerf:',len(df.loc[df['r7usd']>maxR]))

    loss = (df.loc[df['r7usd']>maxR,'r7usd'].sum()-maxR*len(df.loc[df['r7usd']>maxR]))/df['r7usd'].sum()
    print('nerf r7usd loss:',loss)
    df.loc[df['r7usd']>maxR,'r7usd'] = maxR

    return df

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

def main(df):
    # 将重复的uid行去掉
    df = df.drop_duplicates(subset=['uid'])
    levels = makeLevels1(df,usd='r1usd')
    # print(levels)
    cvMapDf = makeCvMap(levels)
    # print(cvMapDf)
    cvMapDf = cvMapFixAvg1(df,cvMapDf,usd='r1usd')
    # print(cvMapDf)
    cvMapDf.to_csv(getFilename('cvMapAndroid_20221001_20230401'))
    tmpDf = addCv(df,cvMapDf,usd='r1usd',usdp='r1usdP')
    df = df.merge(tmpDf,how='left',on='uid')

    tmpDf = pd.DataFrame()
    for cv1 in df['cv1'].unique():
        # print('cv1:',cv1)
        N = 8
        if cv1 > 0:
            N = 9
        cv1Df = df[df['cv1']==cv1]
        levelsCv1 = makeLevels1(cv1Df,usd='r7usd',N=N)
        # print(levelsCv1)
        cv1MapDf = makeCvMap(levelsCv1)
        # print(cv1MapDf)
        cv1MapDf = cvMapFixAvg1(cv1Df,cv1MapDf,usd='r7usd')
        # print(cv1MapDf)
        cv1MapDf.to_csv(getFilename('cvMapAndroid_cv1_'+str(int(cv1))+'_20221001_20230401'))
        tmpDf = tmpDf.append(addCv(cv1Df,cv1MapDf,cv='cv7',usd='r7usd',usdp='r7usdP'))

    df = df.merge(tmpDf,how='left',on='uid') 

    df.to_csv(getFilename('android_20221001_20230401_withCv'),index=False)

    # 计算只分64组的MAPE与R2
    mape,r2 = check(df,usd='r1usd',usdp='r1usdP')
    print('r1usd mape:',mape,'r2:',r2)
    mape,r2 = check(df,usd='r7usd',usdp='r7usdP')
    print('r7usd mape:',mape,'r2:',r2)

    df = addMediaGroupGPT(df)

    # 计算分媒体的MAPE与R2
    for media in mediaList:
        mediaDf = df.loc[df['media_group'] == media['name']]
        mape,r2 = check(mediaDf,usd='r1usd',usdp='r1usdP')
        print(media['name'],'r1usd mape:',mape,'r2:',r2)
        mape,r2 = check(mediaDf,usd='r7usd',usdp='r7usdP')
        print(media['name'],'r7usd mape:',mape,'r2:',r2)


def test(df):
    # 由于这个数据看起来不是很正常，所以做一些查看

    # # 查看首日付费金额中最大值
    # print('max r1usd:',df['r1usd'].max())
    # # 查看首日付费金额中最小值
    # print('min r1usd:',df['r1usd'].min())
    # # 查看7日付费金额中最大值
    # print('max r7usd:',df['r7usd'].max())
    # # 查看7日付费金额中最小值
    # print('min r7usd:',df['r7usd'].min())
    # # 查看平均每日首日付费用户数，将df r1usd >0 先按照dt分组，然后每个uid算一个人，计数
    # print('avg r1usd per day:',df.loc[df['r1usd']>0].groupby('dt')['uid'].count().mean())
    # # 查看平均每日7日付费用户数，将df先按照dt分组，然后每个uid算一个人，计数
    # print('avg r7usd per day:',df.loc[df['r7usd']>0].groupby('dt')['uid'].count().mean())
    # # 查看首日没有付费，7日付费的用户数
    # print('r1usd=0,r7usd>0:',df.loc[(df['r1usd']==0) & (df['r7usd']>0)].shape[0])

    # 计算r1usd 超过200,300,500 3个值的用户数占所有付费用户数的百分比
    print('r1usd>200:',df.loc[df['r1usd']>200].shape[0]/df.loc[df['r1usd']>0].shape[0])
    print('r1usd>300:',df.loc[df['r1usd']>300].shape[0]/df.loc[df['r1usd']>0].shape[0])
    print('r1usd>500:',df.loc[df['r1usd']>500].shape[0]/df.loc[df['r1usd']>0].shape[0])



if __name__ == '__main__':
    # df = getDataFromMC()
    # saveData(df)

    # # raw
    # df = loadData()
    # # df = loadDataOld()
    # main(df)

    # df = loadData()
    # # df = loadDataOld()
    # df = nerfMaxR(0.01)
    # main(df)

    # df = loadData()
    # # df = loadDataOld()
    # df = nerfMaxR(0.02)
    # main(df)

    # df = loadData()
    # test(df)

    df = loadData()
    df = nerfMaxR(0.0029426338089411576)
    main(df)

    df = loadData()
    df = nerfMaxR(0.0016685037061006562)
    main(df)

    df = loadData()
    df = nerfMaxR(0.0004348221779535044)
    main(df)
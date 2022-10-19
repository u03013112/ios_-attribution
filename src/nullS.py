import datetime
import pandas as pd

from odps import ODPS
##@resource_reference{"config.py"}
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('config.py'))) #引入资源至工作空间。

from config import accessId,secretAccessKey,defaultProject,endPoint

def execSql(sql):
    o = ODPS(accessId, secretAccessKey, defaultProject,
            endpoint=endPoint)
    with o.execute_sql(sql).open_reader() as reader:
        pd_df = reader.to_pandas()
        return pd_df

sql = '''
    select 
        * 
    from ods_skan_cv_map
    where
        app_id="id1479198816"
;
'''
afCvMapDataFrame = execSql(sql)
# print (afCvMapDataFrame)

def cvToUSD2(retDf):
    global afCvMapDataFrame
    # 列名 usd
    retDf.insert(retDf.shape[1],'usd',0)
    for i in range(len(afCvMapDataFrame)):
        try:
            min_event_revenue = float(afCvMapDataFrame.min_event_revenue[i])
            max_event_revenue = float(afCvMapDataFrame.max_event_revenue[i])
            avg = (min_event_revenue + max_event_revenue)/2
        except :
            avg = 0
        
        count = retDf.loc[retDf.cv==i,'count']
        retDf.loc[retDf.cv==i,'usd'] = count * avg
    return retDf

def getSkanDF(sinceTimeStr,unitlTimeStr):
    # 将day的格式改为install_time格式，即 20220501 =》2022-05-01
    sinceTimeStr2 = list(sinceTimeStr)
    sinceTimeStr2.insert(6,'-')
    sinceTimeStr2.insert(4,'-')
    sinceTimeStr2 = ''.join(sinceTimeStr2)

    unitlTimeStr2 = list(unitlTimeStr)
    unitlTimeStr2.insert(6,'-')
    unitlTimeStr2.insert(4,'-')
    unitlTimeStr2 = ''.join(unitlTimeStr2)
    # 由于skan报告普遍要晚2~3天，所以unitlTimeStr要往后延长3天
    unitlTime = datetime.datetime.strptime(unitlTimeStr2,'%Y-%m-%d')
    unitlTimeStr = (unitlTime+datetime.timedelta(days=3)).strftime('%Y%m%d')

    sql='''
        select
            count(*) as count,
            media_source,
            skad_conversion_value as cv,
            install_date
        from ods_platform_appsflyer_skad_details
        where
            app_id="id1479198816"
            and event_name in ('af_skad_redownload','af_skad_install')
            and day>=%s and day <=%s
            and install_date >="%s" and install_date<="%s"
        group by 
            media_source,
            skad_conversion_value,
            install_date
        ;
        '''%(sinceTimeStr,unitlTimeStr,sinceTimeStr2,unitlTimeStr2)
    # print(sql)
    pd_df = execSql(sql)
    return pd_df



def predictCv2(historyDf,df):
    # 额外添加一个分media的结论
    ret = {
        'media':[],
        'cv':[],
        'count':[],
    }
    medias = df['media_source'].unique()
    for media in medias:
        # 每个media有个cv表,media没有null值
        nullCount = df.loc[pd.isna(df.cv) & (df.media_source == media),'count'].sum()
        if nullCount == 0:
            # 没有需要填充的null值
            continue
        # historyDf.to_csv('/src/data/historyDf.csv')
        mediaHistory = historyDf.loc[(historyDf.media_source == media)].groupby('cv').agg('sum')
        if mediaHistory['count'].sum() <= 0:
            # 没有足够的样本来做抽样，直接放弃
            continue
        sampleRet = mediaHistory.sample(n = nullCount,weights = mediaHistory['count'],replace=True)
        sampleRet = sampleRet.reset_index()
        
        for i in range(0,64):
            # 这里要取行数，因为是整行抽样，不要取count
            c = len(sampleRet.loc[(sampleRet.cv == i)])

            ret['cv'].append(i)
            ret['count'].append(c)
            ret['media'].append(media)

    return pd.DataFrame(data = ret)

from odps.models import Schema, Column, Partition
def createTable():
    o = ODPS(accessId, secretAccessKey, defaultProject,
            endpoint=endPoint)
    columns = [
        Column(name='media', type='string', comment='like applovin_int,bytedanceglobal_int,googleadwords_int'),
        Column(name='revenueusd', type='double', comment='the media skan revenue in usd'),
        Column(name='nullusd', type='double', comment='the media skan predict null revenue in usd'),
    ]
    partitions = [
        Partition(name='install_date', type='string', comment='like 2022-10-18')
    ]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table('topwar_skan_media_null', schema, if_not_exists=True)
    return table

# import pyarrow as pa
def writeTable(df,dayStr):
    o = ODPS(accessId, secretAccessKey, defaultProject,
            endpoint=endPoint)
    t = o.get_table('topwar_skan_media_null')
    with t.open_writer(partition='install_date=%s'%(dayStr), create_partition=True, arrow=True) as writer:
        # batch = pa.RecordBatch.from_pandas(df)
        # writer.write(batch)
        print(df)
        writer.write(df)

def main2(sinceTimeStr,unitlTimeStr,n=7):
    createTable()
    # 按照media细分的log
    logByMedia = {
        'install_date':[],
        'media':[],
        'revenueusd':[],
        'nullusd':[]
    }

     # for sinceTimeStr->unitlTimeStr
    sinceTime = datetime.datetime.strptime(sinceTimeStr,'%Y%m%d')
    unitlTime = datetime.datetime.strptime(unitlTimeStr,'%Y%m%d')

    day_n = sinceTime - datetime.timedelta(days=n)
    day_nStr = day_n.strftime('%Y%m%d')

    # 从起始日往前n天，到截止日所有skan数值
    skanDf = getSkanDF(day_nStr,unitlTimeStr)
    skanUsdDf = cvToUSD2(skanDf)

    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')

        logByMediaOneDay = {
            'install_date':[],
            'media':[],
            'revenueusd':[],
            'nullusd':[]
        }

        # 获得参考数值，应该是day-n~day-1，共n天
        day_n = day - datetime.timedelta(days=n)
        day_nStr = day_n.strftime('%Y-%m-%d')
        day_1 = day - datetime.timedelta(days=1)
        day_1Str = day_1.strftime('%Y-%m-%d')
        
        historyDf = skanDf[(skanDf.install_date >= day_nStr) & (skanDf.install_date <= day_1Str)]
        
        df = skanDf[skanDf.install_date == dayStr]
        predictCvMediaDf = predictCv2(historyDf,df)

        
        # log by media
        predictUsdMediaDf = cvToUSD2(predictCvMediaDf)
        medias = predictUsdMediaDf['media'].unique()
        for media in medias:
            revenueUsd = skanUsdDf.loc[(skanUsdDf.media_source == media) & (skanUsdDf.install_date == dayStr),'usd'].sum()
            nullUsd = predictUsdMediaDf.loc[(predictUsdMediaDf.media == media),'usd'].sum()
            logByMedia['install_date'].append(dayStr)
            logByMedia['media'].append(media)
            logByMedia['revenueusd'].append(revenueUsd)
            logByMedia['nullusd'].append(nullUsd)

            logByMediaOneDay['install_date'].append(dayStr)
            logByMediaOneDay['media'].append(media)
            logByMediaOneDay['revenueusd'].append(revenueUsd)
            logByMediaOneDay['nullusd'].append(nullUsd)
            
        writeTable(pd.DataFrame(data=logByMediaOneDay),dayStr)
        # sql insert here

    logByMediaDf = pd.DataFrame(data=logByMedia)
    # logByMediaDf.to_csv('/src/data/logNullS%s_%s_%d_byMedia.csv'%(sinceTimeStr,unitlTimeStr,n))
    return logByMediaDf

# start here!
# sinceTimeStr = args['sinceTimeStr']
# unitlTimeStr = args['unitlTimeStr']

main2('20220601','20220730',n=28)
main2('20220801','20220930',n=28)
main2('20221001','20221015',n=28)
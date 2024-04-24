# 解决融合归因没有有效的将SKAN完全分配
# 特别是applivin 差异更大，为什么

import pandas as pd


# 其中afData可以在4次融合归因后100%分配成功，biData在10次融合归因后仍有10%以上的金额不能有效分配
# 先进行一些对比，将两个表的uid进行关联merge
# 可以对比
# 应该是分成3种情况
# 1、安装日期不同
# 2、首日付费金额不同
# 3、相同的uid，在af数据中，被拆分成多行
# 针对上面3种情况，分别计算
# 1、安装日期不同的uid数量占比，即人数占比。
# 2、首日付费金额不同的uid数量占比，即人数占比。和首日付费金额不同的首日付费金额占比，即金额占比
# 3、相同的uid，在af数据中，被拆分成多行的uid数量占比，即人数占比。和相同的uid，在af数据中，被拆分成多行的首日付费金额占比，即金额占比

# 针对某一媒体，为什么会出现偏差更大的情况。这可能需要在skan中加入media维度。然后再查看分配失败的媒体占比。
# 可以预见到，applivin的分配失败率会更高。但是为什么他会更高，不容易分析。可能是他的付费分布导致的。
# 所以可以在上面的分析中增加付费金额维度，即是什么付费范围的用户差异比例更大。


def dataStep1():
    afData = pd.read_csv('/src/data/zk/userDf2_.csv')
    # afData数据处理
    # appsflyer_id,customer_user_id,install_timestamp,r1usd,install_date,country_code1,cv,country_code
    # 1481545847985-4157174,"""{""""uid"""":""""1357835302000272""""}""",1707728917,0.0,2024-02-12,US,0.0,US
    # 1481546403523-1791649,"""{""""uid"""":""""1270556775000286""""}""",1708105919,0.0,2024-02-16,US,0.0,US
    # 1481548478395-1253919,"""{""""uid"""":""""1297794970000271""""}""",1707669097,0.0,2024-02-11,US,0.0,US
    # 1481553164014-5759144,"""{""""uid"""":""""1323911025000238""""}""",1706837143,0.0,2024-02-02,US,0.0,US
    # 1691392800421-1871719,"""{""""uid"""":""""1377981835000315""""}""",1708332335,0.0,2024-02-19,HK,0.0,other
    # 1696636750740-2656402,"""{""""uid"""":""""2f048d488c8051c59013544bb4ae50b7839be009_n3d""""}""",1707335328,0.0,2024-02-07,CA,0.0,other
    # 1697177894825-0883783,"""{""""uid"""":""""1655433196000301""""}""",1708566915,0.0,2024-02-22,US,0.0,US
    # 1697205680778-8644747,"""{""""uid"""":""""1228629461000259""""}""",1707382310,0.0,2024-02-08,US,0.0,US
    # 1697836567430-1108768,1026543191000241,1706891714,0.99,2024-02-02,US,1.0,US
    # 对于customer_user_id是json字符串的，类似于""{""""uid"""":""""1357835302000272""""}""",需要处理成1357835302000272
    # afData['customer_user_id'] = afData['customer_user_id'].apply(lambda x: x.split('"uid":"')[1].split('"')[0] if 'uid' in x else x)
    # afData['customer_user_id'] = afData['customer_user_id'].apply(lambda x: x.split('"uid":"')[1].split('"')[0] if ('uid' in x and '"uid":"' in x) else x)

    print(afData['customer_user_id'].head(10))

    import re
    def extract_uid(x):
        match = re.search(r'''"{""uid"":""(.*?)""}"''', str(x))
        return match.group(1) if match else x

    afData['customer_user_id'] = afData['customer_user_id'].apply(extract_uid)

    afData.to_csv('/src/data/zk/userDf2_fix.csv', index=False)

def dataStep2():
    biData = pd.read_csv('/src/data/zk/userDf2.csv')
    biData['install_date'] = biData['install_timestamp'].apply(lambda x: pd.to_datetime(x, unit='s').strftime('%Y-%m-%d'))
    biData.to_csv('/src/data/zk/userDf2_bi.csv', index=False)

def dataStep3():
    afData = pd.read_csv('/src/data/zk/userDf2_fix.csv')

    afData = afData.groupby(['appsflyer_id','customer_user_id','install_timestamp','install_date']).agg(
        {'r1usd':'sum','cv':'max'}
    ).reset_index()

    afData.to_csv('/src/data/zk/userDf2_fix2.csv', index=False)

def debug():
    afData = pd.read_csv('/src/data/zk/userDf2_fix2.csv')
    biData = pd.read_csv('/src/data/zk/userDf2_bi.csv')

    afData = afData[['appsflyer_id','customer_user_id','install_timestamp','r1usd','install_date','cv']]
    biData = biData[['customer_user_id','install_timestamp','r1usd','install_date','cv']]

    # 过滤，只要[2024-02-15~2024-02-29]的数据
    afData = afData[(afData['install_date'] >= '2024-02-15') & (afData['install_date'] < '2024-02-29')]
    biData = biData[(biData['install_date'] >= '2024-02-15') & (biData['install_date'] < '2024-02-29')]

    print('afSum:',afData['r1usd'].sum())
    print('biSum:',biData['r1usd'].sum())

    print('afCount:',len(afData))
    print('biCount:',len(biData))

    df = pd.merge(afData, biData, how='outer', on=['customer_user_id'], suffixes=('_af', '_bi')).reindex()
    print('mergeCount:',len(df))
    df = df.fillna(0)

    df['date_diff'] = df['install_date_af'] != df['install_date_bi']
    df['cv_diff'] = df['cv_af'] != df['cv_bi']
    df['uid_split'] = df.duplicated('customer_user_id', keep=False)

    df['date_diff_amount'] = df['date_diff'] * df['r1usd_af']
    df['cv_diff_amount'] = df['cv_diff'] * df['r1usd_af']
    df['uid_split_amount'] = df['uid_split'] * df['r1usd_af']

    print(df[['customer_user_id','cv_af','cv_bi','r1usd_af','r1usd_bi']].head(100))

    af_result = afData.groupby(['cv']).agg(
        af_users=('customer_user_id', 'count'),
        af_amount=('r1usd', 'sum'),
    ).reset_index()

    bi_result = biData.groupby(['cv']).agg(
        bi_users=('customer_user_id', 'count'),
        bi_amount=('r1usd', 'sum'),
    ).reset_index()

    diff_result = df.groupby(['cv_af']).agg(
        date_diff_users=('date_diff', 'sum'),
        date_diff_amount=('date_diff_amount', 'sum'),
        cv_diff_users=('cv_diff', 'sum'),
        cv_diff_amount=('cv_diff_amount', 'sum'),
        uid_split_users=('uid_split', 'sum'),
        uid_split_amount=('uid_split_amount', 'sum'),
    ).reset_index()

    result = pd.merge(af_result, bi_result, on=['cv'], suffixes=('_af', '_bi'))
    result = pd.merge(result, diff_result, left_on=['cv'], right_on=['cv_af']).drop(columns=['cv_af'])

    result.to_csv('/src/data/zk/debug_result.csv', index=False)


def debug1():
    afData = pd.read_csv('/src/data/zk/userDf2_fix2.csv')
    biData = pd.read_csv('/src/data/zk/userDf2_bi.csv')

    afData = afData[['appsflyer_id','customer_user_id','install_timestamp','r1usd','install_date','cv']]
    biData = biData[['customer_user_id','install_timestamp','r1usd','install_date','cv']]

    # 过滤，只要[2024-02-15~2024-02-29]的数据
    afData = afData[(afData['install_date'] >= '2024-02-15') & (afData['install_date'] < '2024-02-29')]
    biData = biData[(biData['install_date'] >= '2024-02-15') & (biData['install_date'] < '2024-02-29')]

    afData = afData[afData['cv'] > 30]
    biData = biData[biData['cv'] > 30]

    df = pd.merge(afData, biData, how='outer', on=['customer_user_id'], suffixes=('_af', '_bi')).reindex()

    print(df[['customer_user_id','r1usd_af','r1usd_bi','cv_af','cv_bi']])

    df.to_csv('/src/data/zk/debug01.csv', index=False)

# TODO：
# 找到对应时间的SKAN，对比CV分布
# 可以保守一些，找到合法范围完全在这个时间段内的SKAN数据
# 如果仍然有较大比例的CV缺失，可能是打点问题

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql

def getDiffData(startDayStr,endDayStr):
    filename1 = f'/src/data/zk/q1f1_{startDayStr}_{endDayStr}.csv'
    filename2 = f'/src/data/zk/q1f2_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename1):    
        sql1 = f'''
            select
                campaign_id,
                media,
                cv,
                sum(count) as count,
                sum(usd) as usd
            from lastwar_ios_rh_skan
            where
                day between '{startDayStr}' and '{endDayStr}'
            group by
                campaign_id,
                media,
                cv
            ;
        '''
        print(sql1)
        df1 = execSql(sql1)
        df1.to_csv(filename1, index=False)
    else:
        print('read from file:',filename1)
        df1 = pd.read_csv(filename1)

    if not os.path.exists(filename2):    
        sql2 = f'''
            select
                campaign_id,
                media,
                cv,
                sum(count) as count,
                sum(usd) as usd
            from lastwar_ios_rh_skan_failed
            where
                day between '{startDayStr}' and '{endDayStr}'
            group by
                campaign_id,
                media,
                cv
            ;
        '''
        print(sql2)
        df2 = execSql(sql2)
        df2.to_csv(filename2, index=False)
    else:
        print('read from file:',filename2)
        df2 = pd.read_csv(filename2)

    df = pd.merge(df1, df2, how='outer',on=['campaign_id','media','cv'], suffixes=('_skan', '_failed')).reindex()
    df = df.fillna(0)
    df.to_csv('/src/data/zk/q1f3.csv', index=False)

    # 整体统计，失败的count占比，失败的usd占比
    print('整体统计，失败的count占比，失败的usd占比')
    count_failed_rate = df['count_failed'].sum() / df['count_skan'].sum()
    usd_failed_rate = df['usd_failed'].sum() / df['usd_skan'].sum()
    print(count_failed_rate,usd_failed_rate)

    # # 分媒体统计，失败的count占比，失败的usd占比
    # print('分媒体统计，失败的count占比，失败的usd占比')
    # groupByMediaDf = df.groupby(['media']).agg('sum').reset_index()
    # groupByMediaDf['count_failed_rate'] = groupByMediaDf['count_failed'] / groupByMediaDf['count_skan']
    # groupByMediaDf['usd_failed_rate'] = groupByMediaDf['usd_failed'] / groupByMediaDf['usd_skan']
    # print(groupByMediaDf[['media','count_failed_rate','usd_failed_rate']])
    
    # # 针对每个cv，失败的count占比，失败的usd占比
    # print('针对每个cv，失败的count占比，失败的usd占比')
    # groupByCvDf = df.groupby(['cv']).agg('sum').reset_index()
    # groupByCvDf['count_failed_rate'] = groupByCvDf['count_failed'] / groupByCvDf['count_skan']
    # groupByCvDf['usd_failed_rate'] = groupByCvDf['usd_failed'] / groupByCvDf['usd_skan']
    # groupByCvDf = groupByCvDf.sort_values(by='cv',ascending=True)
    # print(groupByCvDf[['cv','count_skan','count_failed','count_failed_rate','usd_failed_rate']])

    # # 针对每个媒体，再分cv，失败的count占比，失败的usd占比
    # print('针对每个媒体，再分cv，失败的count占比，失败的usd占比')
    # mediaList = df['media'].unique()
    # for media in mediaList:
    #     mediaDf = df[df['media'] == media]
    #     mediaDf = mediaDf.groupby(['cv']).agg('sum').reset_index()
    #     mediaDf['count_failed_rate'] = mediaDf['count_failed'] / mediaDf['count_skan']
    #     mediaDf['usd_failed_rate'] = mediaDf['usd_failed'] / mediaDf['usd_skan']
    #     print(media)
    #     print(mediaDf[['cv','count_skan','count_failed','count_failed_rate','usd_failed_rate']])


def getDiffData2(startDayStr,endDayStr):
    filename1 = f'/src/data/zk/q1f1_2_{startDayStr}_{endDayStr}.csv'
    filename2 = f'/src/data/zk/q1f2_2_{startDayStr}_{endDayStr}.csv'
    if not os.path.exists(filename1):    
        sql1 = f'''
            select
                media,
                cv,
                sum(count) as count,
                sum(usd) as usd
            from lastwar_ios_rh_skan_raw
            where
                day between '{startDayStr}' and '{endDayStr}'
            group by
                media,
                cv
            ;
        '''
        print(sql1)
        df1 = execSql(sql1)
        df1.to_csv(filename1, index=False)
    else:
        print('read from file:',filename1)
        df1 = pd.read_csv(filename1)

    if not os.path.exists(filename2):    
        sql2 = f'''
            select
                media,
                cv,
                sum(count) as count,
                sum(usd) as usd
            from lastwar_ios_rh_skan_raw_failed
            where
                day between '{startDayStr}' and '{endDayStr}'
            group by
                media,
                cv
            ;
        '''
        print(sql2)
        df2 = execSql(sql2)
        df2.to_csv(filename2, index=False)
    else:
        print('read from file:',filename2)
        df2 = pd.read_csv(filename2)

    df = pd.merge(df1, df2, how='outer',on=['media','cv'], suffixes=('_skan', '_failed')).reindex()
    df = df.fillna(0)
    # df.to_csv('/src/data/zk/q1f3.csv', index=False)

    # 整体统计，失败的count占比，失败的usd占比
    print('整体统计，失败的count占比，失败的usd占比')
    count_failed_rate = df['count_failed'].sum() / df['count_skan'].sum()
    usd_failed_rate = df['usd_failed'].sum() / df['usd_skan'].sum()
    print(count_failed_rate,usd_failed_rate)

    # # 分媒体统计，失败的count占比，失败的usd占比
    # print('分媒体统计，失败的count占比，失败的usd占比')
    # groupByMediaDf = df.groupby(['media']).agg('sum').reset_index()
    # groupByMediaDf['count_failed_rate'] = groupByMediaDf['count_failed'] / groupByMediaDf['count_skan']
    # groupByMediaDf['usd_failed_rate'] = groupByMediaDf['usd_failed'] / groupByMediaDf['usd_skan']
    # print(groupByMediaDf[['media','count_failed_rate','usd_failed_rate']])
    
    # # 针对每个cv，失败的count占比，失败的usd占比
    # print('针对每个cv，失败的count占比，失败的usd占比')
    # groupByCvDf = df.groupby(['cv']).agg('sum').reset_index()
    # groupByCvDf['count_failed_rate'] = groupByCvDf['count_failed'] / groupByCvDf['count_skan']
    # groupByCvDf['usd_failed_rate'] = groupByCvDf['usd_failed'] / groupByCvDf['usd_skan']
    # groupByCvDf = groupByCvDf.sort_values(by='cv',ascending=True)
    # print(groupByCvDf[['cv','count_skan','count_failed','count_failed_rate','usd_failed_rate']])

    # # 针对每个媒体，再分cv，失败的count占比，失败的usd占比
    # print('针对每个媒体，再分cv，失败的count占比，失败的usd占比')
    # mediaList = df['media'].unique()
    # for media in mediaList:
    #     mediaDf = df[df['media'] == media]
    #     mediaDf = mediaDf.groupby(['cv']).agg('sum').reset_index()
    #     mediaDf['count_failed_rate'] = mediaDf['count_failed'] / mediaDf['count_skan']
    #     mediaDf['usd_failed_rate'] = mediaDf['usd_failed'] / mediaDf['usd_skan']
    #     print(media)
    #     print(mediaDf[['cv','count_skan','count_failed','count_failed_rate','usd_failed_rate']])



if __name__ == '__main__':
    # dataStep1()
    # dataStep2()
    # dataStep3()
    # debug()
    # debug1()
    # getDiffData('20240215','20240229')

    for dayStr in ['20240413','20240414','20240415','20240416','20240417']:
        print(dayStr)
        getDiffData2(dayStr,dayStr)

    # getDiffData('20240416','20240416')
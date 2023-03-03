# 撞库实践，用安卓进行尝试
# https://rivergame.feishu.cn/docx/H4osdK7umoleAexSqxdcE8ztnXe

import copy
import numpy as np
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import ssotCvMapDataFrame

def getFilename(filename):
    return '/src/data/zk/%s.csv'%(filename)

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
            install_timestamp,
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
            install_timestamp,
            media
        ;
    '''
    print(sql)
    df = execSql(sql)
    print('sql finish')
    return df

# 将首日付费按照目前的地图映射到CV
def addCV(userDf,mapDf = None):
    userDf.loc[:,'cv'] = 0
    if mapDf is None:
        map = ssotCvMapDataFrame
    else:
        map = mapDf
    for i in range(len(map)):
        min_event_revenue = map.min_event_revenue[i]
        max_event_revenue = map.max_event_revenue[i]
        # print(i,min_event_revenue,max_event_revenue)
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

# 暂时只看着3个媒体
mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads']},
    {'name':'unknown','codeList':[]}
]
def addMediaGroup(df):
    # 所有不属于mediaList的都算是unknown，和自然量一起处理
    df.insert(df.shape[1],'media_group','unknown')
    for media in mediaList:
        name = media['name']
        for code in media['codeList']:
            df.loc[df.media == code,'media_group'] = name
    return df

# 下面步骤说明，参考文档
# https://rivergame.feishu.cn/docx/H4osdK7umoleAexSqxdcE8ztnXe

# 从数据表 ods_platform_appsflyer_events 中获得 安卓用户数据
# 主要字段：install_date，media，cv（SSOT版本Map），uid（安卓只能用af id），首日收入，7日收入
def step1():
    if __debug__:
        print('debug 模式，并未真的sql')
    else:
        df = getDataFromAF()
        df.to_csv(getFilename('zkAndroid0'))
    
    # step 1
    df = pd.read_csv(getFilename('zkAndroid0'))
    df1 = addCV(df)
    df1 = addMediaGroup(df1)
    df1.to_csv(getFilename('zkAndroid1'))
    return df1

# 模拟iOS，随机将70%用户标记为不可归因用户。所有不可归因用户+自然量生成 AF报告。
def step2(df = None):
    if df == None:
        df = pd.read_csv(getFilename('zkAndroid1'))
    # 采用 0.25 的idfa比率是2023年1月1日以来，idfa用户占比在26%左右
    userDf = copy.deepcopy(df)
    userDf.loc[:,'idfa'] = 0
    sampleDf = userDf.sample(frac = 0.25)
    userDf.loc[sampleDf.index,'idfa'] = 1

    userDf.to_csv(getFilename('zkAndroid2'))
    return userDf

# 模拟SKAN，按照苹果文档，对非付费用户进行24~48小时的随机延后，对付费用户进行24~72小时的延后，记作SKAN postback时间，生成 postback报告。
def step3(df2 = None):
    if df2 == None:
        df2 = pd.read_csv(getFilename('zkAndroid2'))
    # postback报告只和media数据有关，自然量忽略不计
    mediaDf = df2.loc[df2.media_group != 'unknown']

    mediaDf.loc[mediaDf.cv == 0,'rand_delay'] = np.random.randint(24*3600,48*3600,len(mediaDf.loc[mediaDf.cv == 0]))
    mediaDf.loc[mediaDf.cv > 0,'rand_delay'] = np.random.randint(24*3600,72*3600,len(mediaDf.loc[mediaDf.cv > 0]))
    mediaDf.loc[:,'timestamp'] = mediaDf['install_timestamp'] + mediaDf['rand_delay']

    mediaDf.to_csv(getFilename('zkAndroid3'))
    return mediaDf

# 基于第2和第3步骤结果，被标记为不可归因用户中的媒体（字节、fb、gg）用户生成AF版本的安装日期（后续简称推测安装日期）并汇总成 SKAN报告（对应iOS排除掉SSOT置位之后的SKAN报告）。
def step4(df3 = None):
    if df3 == None:
        df3 = pd.read_csv(getFilename('zkAndroid3'))
    # skan报告里暂时只获取无法归因用户，ssot应该可以帮助排除
    df3 = df3.loc[df3.idfa == 0]
    df3.loc[df3.cv == 0,'install_timestamp'] = df3['timestamp'] - 36*3600
    df3.loc[df3.cv > 0,'install_timestamp'] = df3['timestamp'] - 48*3600

    df3.loc[:,'install_date_af'] = pd.to_datetime(df3['install_timestamp'],unit='s').dt.strftime('%Y-%m-%d')
    df3.loc[:,'count'] = 1
    df4 = df3.groupby(by=['media_group','install_date_af','cv'],as_index=False).agg({
        'count':'sum',
        # 'r1usd':'sum',
        # 'r7usd':'sum',
    })

    df4.to_csv(getFilename('zkAndroid4'))
    return df4

# 按照推测安装日期对 SKAN报告 和 AF报告 进行7日汇总。
def step5(df2 = None,df4 = None):
    if df4 == None:
        df4 = pd.read_csv(getFilename('zkAndroid4'))

    dateDf = pd.DataFrame({'install_date':df4['install_date_af'].unique()})
    dateDf = dateDf.sort_values(by = ['install_date'],ignore_index=True)
    dateDf.loc[:,'i0'] = np.arange(len(dateDf))
    dateDf.loc[:,'i1'] = dateDf['i0']%(7)
    dateDf.loc[:,'install_date_group'] = pd.to_datetime(
        (pd.to_datetime(dateDf['install_date'],format='%Y-%m-%d').astype(int)/ 10**9 - dateDf['i1']*24*3600),
        unit='s'
    ).dt.strftime('%Y-%m-%d')

    # af 不可归因报告 7日汇总
    if df2 == None:
        df2 = pd.read_csv(getFilename('zkAndroid2'))
    # 只是把idfa用户排除掉，剩下的都是可能被skan匹配到的
    afDf = df2.loc[df2.idfa == 0]
    afGroup = afDf.merge(dateDf,how='left',on=['install_date'],suffixes = ('','_g'))
    # print(afGroup)
    afGroup.loc[:,'count'] = 1
    afGroup2 = afGroup.groupby(by=['install_date_group','cv'],as_index=False).agg({
        'count':'sum',
        'r1usd':'sum',
        'r7usd':'sum',
    })
    afGroup2.to_csv(getFilename('zkAndroid5Af'))

    # skan报告7日汇总
    skanGroup = df4.merge(dateDf,how='left',left_on=['install_date_af'],right_on=['install_date'],suffixes = ('','_g'))
    skanGroup2 = skanGroup.groupby(by=['media_group','install_date_group','cv'],as_index=False).agg({
        'count':'sum',
        # 'r1usd':'sum',
        # 'r7usd':'sum',
    })

    
    skanGroup2.to_csv(getFilename('zkAndroid5Skan'))

    return afGroup2,skanGroup2

# 针对步骤5结论进行 分配式归因，并根据 分配式归因结果 计算 媒体SKAN7日收入
def step6(df5af = None,df5skan = None):
    if df5af == None:
        df5af = pd.read_csv(getFilename('zkAndroid5Af'))
    if df5skan == None:
        df5skan = pd.read_csv(getFilename('zkAndroid5Skan'))

    # 大致思路，用af数据count 作为 sum数据
    # 用skan数据计算各媒体在sum中占比
    # 然后将af报告中的cv用户标记媒体比例
    # 按照比例计算媒体7日收入
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaSkanDf = df5skan.loc[df5skan.media_group == name]
        mediaGroupDf = mediaSkanDf.groupby(['install_date_group','cv'],as_index=False).agg({
            'count':'sum'
        })

        df5af = df5af.merge(mediaGroupDf,how='left',on=['install_date_group','cv'],suffixes=('','_%s'%name))
        df5af.loc[:,name] = df5af['count_%s'%(name)]/df5af['count']
        # TODO: 应该针对超过100%的做一些处理目前先简单的将超过100%的改为100%
        df5af.loc[df5af[name] > 1,name] = 1
        df5af.loc[:,'%s_r7usd'%name] = df5af[name]*df5af['r7usd']


    df6af = df5af.fillna(0)
    df6af.to_csv(getFilename('zkAndroid6Skan'))

    df6af = df6af.groupby(['install_date_group'],as_index=False).agg({
        'google_r7usd':'sum',
        'bytedance_r7usd':'sum',
        'facebook_r7usd':'sum'
    })
    df6af.to_csv(getFilename('zkAndroid6'))

    return df6af

# 媒体SAKN7日收入 + 已归因媒体7日收入 -> 推测媒体7日总收入
def step7(df2 = None,df6 = None):
    # 这段代码从step5中抄过来，逻辑类似，只是这里选的是有idfa的部分
    # 选df4的原因是和前面统一
    df4 = None
    if df4 == None:
        df4 = pd.read_csv(getFilename('zkAndroid4'))
    dateDf = pd.DataFrame({'install_date':df4['install_date_af'].unique()})
    dateDf = dateDf.sort_values(by = ['install_date'],ignore_index=True)
    dateDf.loc[:,'i0'] = np.arange(len(dateDf))
    dateDf.loc[:,'i1'] = dateDf['i0']%(7)
    dateDf.loc[:,'install_date_group'] = pd.to_datetime(
        (pd.to_datetime(dateDf['install_date'],format='%Y-%m-%d').astype(int)/ 10**9 - dateDf['i1']*24*3600),
        unit='s'
    ).dt.strftime('%Y-%m-%d')

    if df2 == None:
        df2 = pd.read_csv(getFilename('zkAndroid2'))
    afDf = df2.loc[df2.idfa == 1]
    afGroup = afDf.merge(dateDf,how='left',on=['install_date'],suffixes = ('','_g'))
    afGroup.loc[:,'count'] = 1
    afGroup2 = afGroup.groupby(by=['install_date_group','media_group'],as_index=False).agg({
        'r7usd':'sum'
    })
    afGroup2.to_csv(getFilename('zkAndroid7AF'))

    if df6 == None:
        df6 = pd.read_csv(getFilename('zkAndroid6'))
        df6 = df6.loc[:,~df6.columns.str.match('Unnamed')]

    df6 = df6.rename(columns={
        'google_r7usd':'google',
        'bytedance_r7usd':'bytedance',
        'facebook_r7usd':'facebook'
    })

    df6Melt = pd.melt(df6,id_vars='install_date_group',var_name='media_group',value_name='r7usd')

    df6Melt.to_csv(getFilename('zkAndroid6Melt'))

    sumDf = df6Melt.append(afGroup2,ignore_index=True)
    sumDf = sumDf.groupby(['install_date_group','media_group'],as_index=False).agg({
        'r7usd':'sum'
    })

    sumDf.to_csv(getFilename('zkAndroid7'))
    return sumDf

# 推测媒体7日总收入 与 真实媒体7日收入 计算 MAPE
def step8(df1 = None,df7 = None):
    if df1 == None:
        df1 = pd.read_csv(getFilename('zkAndroid1'))
        df1 = df1.loc[:,~df1.columns.str.match('Unnamed')]

    # 将原始数据的维度和推测数据维度保持一致
    df1Sum = df1.groupby(by = ['install_date','media_group'],as_index=False).agg({
        'r7usd':'sum'
    })

    df4 = None
    if df4 == None:
        df4 = pd.read_csv(getFilename('zkAndroid4'))
    dateDf = pd.DataFrame({'install_date':df4['install_date_af'].unique()})
    dateDf = dateDf.sort_values(by = ['install_date'],ignore_index=True)
    dateDf.loc[:,'i0'] = np.arange(len(dateDf))
    dateDf.loc[:,'i1'] = dateDf['i0']%(7)
    dateDf.loc[:,'install_date_group'] = pd.to_datetime(
        (pd.to_datetime(dateDf['install_date'],format='%Y-%m-%d').astype(int)/ 10**9 - dateDf['i1']*24*3600),
        unit='s'
    ).dt.strftime('%Y-%m-%d')

    df1Sum = df1Sum.merge(dateDf,how='left',on=['install_date'],suffixes = ('','_g'))
    df1Sum = df1Sum.groupby(by=['install_date_group','media_group'],as_index=False).agg({
        'r7usd':'sum'
    })
    df1Sum.to_csv(getFilename('zkAndroid8RawGroup'))

    if df7 == None:
        df7 = pd.read_csv(getFilename('zkAndroid7'))
        df7 = df7.loc[:,~df7.columns.str.match('Unnamed')]

    mergeDf = df1Sum.merge(df7,how = 'left',on=['install_date_group','media_group'],suffixes = ('_real','_perd'))
    # print(mergeDf)
    mergeDf.loc[:,'mape'] = (mergeDf['r7usd_real'] - mergeDf['r7usd_perd'])/mergeDf['r7usd_real']
    mergeDf.loc[mergeDf.mape < 0 ,'mape'] *= -1

    mergeDf.to_csv(getFilename('zkAndroid8'))

    return mergeDf

import matplotlib.pyplot as plt
# 重复第2~第8步骤，计算整体MAPE
def report(df8 = None):
    if df8 == None:
        df8 = pd.read_csv(getFilename('zkAndroid8'))
        df8 = df8.loc[:,~df8.columns.str.match('Unnamed')]

    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue

        mediaDf = df8.loc[df8.media_group == name]
        mape = mediaDf['mape'].mean()
        corr = mediaDf.corr()['r7usd_real']['r7usd_perd']
        print(name,'mape:',mape,'corr:',corr)

        plt.title("%s 7day revenue"%(name))
        plt.figure(figsize=(10.8, 3.2))
        mediaDf['r7usd_real'].plot(label='real')
        mediaDf['r7usd_perd'].plot(label='predict')

        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.tight_layout()
        plt.savefig('/src/data/zk/zk%s.png'%(name))
        plt.clf()

# 安装日期组的测试
# 主要针对根据skan报告推测的安装日期，与真实安装日期差异
# 获得的差异是7日收入金额的差异
def installDateGroupTest():
    
    df4 = pd.read_csv(getFilename('zkAndroid4'))

    dateDf = pd.DataFrame({'install_date':df4['install_date_af'].unique()})
    dateDf = dateDf.sort_values(by = ['install_date'],ignore_index=True)
    dateDf.loc[:,'i0'] = np.arange(len(dateDf))
    dateDf.loc[:,'i1'] = dateDf['i0']%(7)
    dateDf.loc[:,'install_date_group'] = pd.to_datetime(
        (pd.to_datetime(dateDf['install_date'],format='%Y-%m-%d').astype(int)/ 10**9 - dateDf['i1']*24*3600),
        unit='s'
    ).dt.strftime('%Y-%m-%d')

    # af 不可归因报告 7日汇总

    df2 = pd.read_csv(getFilename('zkAndroid2'))
    # 只是把idfa用户排除掉，剩下的都是可能被skan匹配到的
    afDf = df2.loc[df2.idfa == 0]
    afGroup = afDf.merge(dateDf,how='left',on=['install_date'],suffixes = ('','_g'))
    # print(afGroup)
    afGroup.loc[:,'count'] = 1
    afGroup2 = afGroup.groupby(by=['install_date_group','media_group'],as_index=False).agg({
        'r7usd':'sum'
    })
    
    df3 = pd.read_csv(getFilename('zkAndroid3'))
    df3 = df3.loc[df3.idfa == 0]
    df3.loc[df3.cv == 0,'install_timestamp'] = df3['timestamp'] - 36*3600
    df3.loc[df3.cv > 0,'install_timestamp'] = df3['timestamp'] - 48*3600

    df3.loc[:,'install_date_af'] = pd.to_datetime(df3['install_timestamp'],unit='s').dt.strftime('%Y-%m-%d')
    df3.loc[:,'count'] = 1
    df4 = df3.groupby(by=['media_group','install_date_af','cv'],as_index=False).agg({
        'r7usd':'sum',
    })

    skanGroup = df4.merge(dateDf,how='left',left_on=['install_date_af'],right_on=['install_date'],suffixes = ('','_g'))
    skanGroup2 = skanGroup.groupby(by=['media_group','install_date_group'],as_index=False).agg({
        'r7usd':'sum'
    })

    mergeDf = afGroup2.merge(skanGroup2,how='left',on=['install_date_group','media_group'],suffixes = ('_real','_perd'))
    mergeDf.loc[:,'mape'] = (mergeDf['r7usd_real'] - mergeDf['r7usd_perd'])/mergeDf['r7usd_real']
    mergeDf.loc[mergeDf.mape < 0 ,'mape'] *= -1

    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue

        mediaDf = mergeDf.loc[mergeDf.media_group == name]
        mape = mediaDf['mape'].mean()
        corr = mediaDf.corr()['r7usd_real']['r7usd_perd']
        print(name,'mape:',mape,'corr:',corr)

    mergeDf.to_csv(getFilename('installDateGroupTest'))

def main():
    # step1()
    # step2()
    # step3()
    step4()
    step5()
    step6()
    step7()
    step8()
    report()
    # installDateGroupTest()


if __name__ == '__main__':
    main()
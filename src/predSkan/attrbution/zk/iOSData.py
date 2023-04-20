# 为撞库做准备，尝试获得iOS数据

import datetime
import pandas as pd

import os
import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import afCvMapDataFrame

# 
def getFilename(filename):
    return '/src/data/zk/%s.csv'%(filename)

# iOS数据需要AF数据
# SSOT之前版本
# 包括 安装日期 idfv media idfa r1usd r7usd
# 按照SSOT之前的map进行CV转化，并进行汇总
def getDataFromAF():
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
            continue
        whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

    sql = '''
        select
            cv,
            count(*) as count,
            media_source as media,
            sum(r1usd) as sumR1usd,
            sum(r7usd) as sumR7usd,
            install_date,
            had_idfa
        from
            (
                select
                    idfv,
                    had_idfa,
                    media_source,
                    case
                        when r1usd = 0
                        or r1usd is null then 0
                        %s
                        else 63
                    end as cv,
                    r1usd,
                    r7usd,
                    install_date
                from
                    (
                        SELECT
                            t0.idfv,
                            t0.install_date,
                            t0.had_idfa,
                            t0.media_source,
                            t1.r1usd,
                            t1.r7usd
                        FROM
                            (
                                select
                                    idfv,
                                    case
                                        when idfa is null then 0
                                        else 1
                                    end as had_idfa,
                                    media_source,
                                    install_date
                                from
                                    (
                                        select
                                            idfv,
                                            idfa,
                                            media_source,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date
                                        from
                                            ods_platform_appsflyer_events
                                        where
                                            app_id = 'id1479198816'
                                            and event_name = 'install'
                                            and zone = 0
                                            and day >= 20220501
                                            and day <= 20230301
                                            and install_time >= "2022-05-01"
                                            and install_time < "2023-02-28"
                                        union
                                        all
                                        select
                                            idfv,
                                            idfa,
                                            media_source,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date
                                        from
                                            tmp_ods_platform_appsflyer_origin_install_data
                                        where
                                            app_id = 'id1479198816'
                                            and zone = '0'
                                            and install_time >= "2022-05-01"
                                            and install_time <= "2023-02-28"
                                    )
                                group by
                                    idfv,
                                    had_idfa,
                                    media_source,
                                    install_date
                            ) as t0
                            LEFT JOIN (
                                select
                                    idfv,
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
                                    ) as r7usd
                                from
                                    ods_platform_appsflyer_events
                                where
                                    app_id = 'id1479198816'
                                    and event_name = 'af_purchase'
                                    and zone = 0
                                    and day >= 20220501
                                    and day <= 20230301
                                    and install_time >= "2022-05-01"
                                    and install_time < "2023-02-28"
                                group by
                                    install_date,
                                    idfv
                            ) as t1 ON t0.idfv = t1.idfv
                    )
            )
        group by
            cv,
            had_idfa,
            media_source,
            install_date;
    '''%(whenStr)
    print(sql)
    df = execSql(sql)
    return df

# 更改时间，获得更新的数据
def getDataFromAF2():
    whenStr = ''
    for i in range(len(afCvMapDataFrame)):
        min_event_revenue = afCvMapDataFrame.min_event_revenue[i]
        max_event_revenue = afCvMapDataFrame.max_event_revenue[i]
        if pd.isna(min_event_revenue) or pd.isna(max_event_revenue):
            continue
        whenStr += 'when r1usd>%d and r1usd<=%d then %d\n'%(min_event_revenue, max_event_revenue,i)

    sql = '''
        select
            cv,
            count(*) as count,
            media_source as media,
            sum(r1usd) as sumR1usd,
            sum(r7usd) as sumR7usd,
            install_date,
            had_idfa
        from
            (
                select
                    idfv,
                    had_idfa,
                    media_source,
                    case
                        when r1usd = 0
                        or r1usd is null then 0
                        %s
                        else 63
                    end as cv,
                    r1usd,
                    r7usd,
                    install_date
                from
                    (
                        SELECT
                            t0.idfv,
                            t0.install_date,
                            t0.had_idfa,
                            t0.media_source,
                            t1.r1usd,
                            t1.r7usd
                        FROM
                            (
                                select
                                    idfv,
                                    case
                                        when idfa is null then 0
                                        else 1
                                    end as had_idfa,
                                    media_source,
                                    install_date
                                from
                                    (
                                        select
                                            idfv,
                                            idfa,
                                            media_source,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date
                                        from
                                            ods_platform_appsflyer_events
                                        where
                                            app_id = 'id1479198816'
                                            and event_name = 'install'
                                            and zone = 0
                                            and day >= 20220501
                                            and day <= 20230301
                                            and install_time >= "2022-05-01"
                                            and install_time < "2023-02-28"
                                        union
                                        all
                                        select
                                            idfv,
                                            idfa,
                                            media_source,
                                            to_char(
                                                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                                                "yyyy-mm-dd"
                                            ) as install_date
                                        from
                                            tmp_ods_platform_appsflyer_origin_install_data
                                        where
                                            app_id = 'id1479198816'
                                            and zone = '0'
                                            and install_time >= "2022-05-01"
                                            and install_time <= "2023-02-28"
                                    )
                                group by
                                    idfv,
                                    had_idfa,
                                    media_source,
                                    install_date
                            ) as t0
                            LEFT JOIN (
                                select
                                    idfv,
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
                                    ) as r7usd
                                from
                                    ods_platform_appsflyer_events
                                where
                                    app_id = 'id1479198816'
                                    and event_name = 'af_purchase'
                                    and zone = 0
                                    and day >= 20220501
                                    and day <= 20230301
                                    and install_time >= "2022-05-01"
                                    and install_time < "2023-02-28"
                                group by
                                    install_date,
                                    idfv
                            ) as t1 ON t0.idfv = t1.idfv
                    )
            )
        group by
            cv,
            had_idfa,
            media_source,
            install_date;
    '''%(whenStr)
    print(sql)
    df = execSql(sql)
    return df



# 这里区别是要将SKAN中的安装时间做处理
# 按照AF的方式，无转化用户向前推36小时，付费用户向前推48小时
def getDataFromSKAN():
    sql = '''
        select
            case
                when skad_conversion_value > 0 then 
                    to_char(dateadd(
                        to_date(timestamp, "yyyy-mm-dd hh:mi:ss"),
                        -48,
                        'hh'
                    ),'yyyy-mm-dd'
                )
                else 
                    to_char(
                        dateadd(
                            to_date(timestamp, "yyyy-mm-dd hh:mi:ss"),
                            -36,
                            'hh'
                        ),'yyyy-mm-dd'
                    )
            end as install_date_af,
            media_source as media,
            skad_conversion_value as cv,
            count(*) as count
        from
            ods_platform_appsflyer_skad_details
        where
            app_id = 'id1479198816'
            and event_name in ("af_skad_install","af_skad_redownload")
            and day >= 20220501
            and day <= 20230301
        group by
            install_date_af,
            media,
            cv
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

# 从2022-05-01~2023-02-27，按7天汇总
def getDateGroupDf():
    sinceTime = datetime.datetime.strptime('20220501','%Y%m%d')
    unitlTime = datetime.datetime.strptime('20230227','%Y%m%d')
    installDate = []
    installDateGroup = []
    for i in range((unitlTime - sinceTime).days + 1):
        day = sinceTime + datetime.timedelta(days=i)
        dayStr = day.strftime('%Y-%m-%d')
        installDate.append(dayStr)
        j = i%7
        groupDay = day - datetime.timedelta(days=j)
        groupDayStr = groupDay.strftime('%Y-%m-%d')
        installDateGroup.append(groupDayStr)

    df = pd.DataFrame({
        'install_date':installDate,
        'install_date_group':installDateGroup
    })

    return df

def addDateGroup(df,dateGroupDf):
    # 直接将安装日期做过滤
    df = df.loc[
        (df.install_date >= '2022-05-01') &
        (df.install_date < '2023-02-28')
    ]
    mergeDf = df.merge(dateGroupDf,how='left',on=['install_date'])
    return mergeDf

# 暂时只看着3个媒体
mediaList = [
    {'name':'google','codeList':['googleadwords_int']},
    {'name':'bytedance','codeList':['bytedanceglobal_int']},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads','facebook','FacebookAds']},
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


# 1、将af数据中的idfa单独拿出来，记为idfa数据
# 1.1、将af数据中的非idfa拿出来，按天求人数总和，记作总数
# 2、用skan数据 - idfa数据 ，记作待分配数据
# 3、待分配数据中按媒体统计每天用户数，和第2步中的总用户数计算出比例
# 4、用比例 计算首日付费+7日付费
# 5、第4步的结论+idfa结论，记作最终结论
# 6、最终分开媒体后，出报告，每个媒体的单独ROI报表+付费增长率的曲线

def step1(afDf):
    idfaDf = afDf.loc[afDf.media_group != 'unknown']
    noIdfaDf = afDf.loc[afDf.media_group == 'unknown']
    if __debug__:
        idfaDf.to_csv(getFilename('step1_idfaDf'))
        noIdfaDf.to_csv(getFilename('step1_noIdfaDf'))
    return idfaDf, noIdfaDf

def step2(skanDf,idfaDf):
    idfaDf2 = idfaDf.drop(['sumr1usd','sumr7usd'], axis=1)
    mergeDf = skanDf.merge(idfaDf2,on=['install_date_group','media_group','cv'],suffixes=('_skan','_idfa'))
    mergeDf.loc[:,'count'] = mergeDf['count_skan'] - mergeDf['count_idfa']
    mergeDf.loc[mergeDf['count'] < 0,'count'] = 0
    if __debug__:
        mergeDf.to_csv(getFilename('step2'))
    return mergeDf

def step3(step2Df,noIdfaDf):
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaSkanDf = step2Df.loc[step2Df.media_group == name]
        mediaGroupDf = mediaSkanDf.groupby(['install_date_group','cv'],as_index=False).agg({
            'count':'sum'
        })

        noIdfaDf = noIdfaDf.merge(mediaGroupDf,how='left',on=['install_date_group','cv'],suffixes=('','_%s'%name))
        noIdfaDf.loc[:,name] = noIdfaDf['count_%s'%(name)]/noIdfaDf['count']
        noIdfaDf.loc[noIdfaDf[name] > 1,name] = 1
        noIdfaDf.loc[:,'%s_r1usd'%name] = noIdfaDf[name]*noIdfaDf['sumr1usd']
        noIdfaDf.loc[:,'%s_r7usd'%name] = noIdfaDf[name]*noIdfaDf['sumr7usd']

    step3Df = noIdfaDf.fillna(0)
    # print(step3Df)
    if __debug__:
        step3Df.to_csv(getFilename('step3'))
    return step3Df
    
def step4(step3Df):
    # 这里主要是将表格的格式转一下，方便计算
    sumDf = step3Df.groupby(by = ['install_date_group'],as_index = False).agg({
        'google_r1usd':'sum',
        'google_r7usd':'sum',
        'bytedance_r1usd':'sum',
        'bytedance_r7usd':'sum',
        'facebook_r1usd':'sum',
        'facebook_r7usd':'sum'
    })
    
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaDf = pd.DataFrame({
            'install_date_group':sumDf['install_date_group'],
            'r1usd':sumDf['%s_r1usd'%(name)],
            'r7usd':sumDf['%s_r7usd'%(name)],
        })
        mediaDf.to_csv(getFilename('step4_%s'%(name)))

# 将已归因用户加入到分配式归因的结果中
def step5(idfaDf):
    idfaSumDf = idfaDf.groupby(by = ['install_date_group','media_group'],as_index = False).agg({
        'sumr1usd':'sum',
        'sumr7usd':'sum'
    })
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaDf = pd.read_csv(getFilename('step4_%s'%(name)))
        mediaIdfaDf = idfaSumDf.loc[idfaSumDf.media_group == name]
        mergeDf = mediaDf.merge(mediaIdfaDf,on = ['install_date_group'])
        mergeDf['s5_r1usd'] = mergeDf['r1usd'] + mergeDf['sumr1usd']
        mergeDf['s5_r7usd'] = mergeDf['r7usd'] + mergeDf['sumr7usd']
        mergeDf = mergeDf.rename(columns={
            'r1usd':'r1usd_attrbution',
            'r7usd':'r7usd_attrbution',
            'sumr1usd':'r1usd_idfa',
            'sumr7usd':'r7usd_idfa',
            's5_r1usd':'r1usd',
            's5_r7usd':'r7usd',
        })
        mergeDf = cleanDf(mergeDf)
        mergeDf.to_csv(getFilename('step5_%s'%(name)))


# 获得广告花费
def getAdCost():
    sql = '''
        select
            mediasource as media,
            to_char(
                to_date(day, "yyyymmdd"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(cost) as cost
        from
        (
            select
                day,
                mediasource,
                getapppackagev2(
                    app,
                    mediasource,
                    campaign_name,
                    adset_name,
                    ad_name
                ) as app_package,
                campaign_name,
                adset_name,
                ad_name,
                cost
            from
                ods_realtime_mediasource_cost
            where
                app = 102
                and day >= 20220501
                and day < 20230228
        )
        where
            app_package = 'id1479198816'
        group by
            mediasource,
            day
        ;
    '''
    print(sql)
    df = execSql(sql)
    return df

import matplotlib.pyplot as plt
def report():
    # 针对7日付费金额/首日付费金额，分媒体看看曲线
    plt.title("7d revenue/1d revenue")
    plt.figure(figsize=(10.8, 3.2))
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaDf = pd.read_csv(getFilename('step5_%s'%(name)))
        mediaDf.set_index(["install_date_group"], inplace=True)
        mediaDf['r7/r1'] = mediaDf['r7usd']/mediaDf['r1usd']

        mediaDf['r7/r1'].plot(label = name)
        mediaDf.to_csv(getFilename('r7Pr1_%s'%(name)))
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig('/src/data/zk/zk_r7Pr1.png')
    plt.clf()

    # adCostDf = getAdCost()
    # adCostDf.to_csv(getFilename('adCost'))
    adCostDf = pd.read_csv(getFilename('adCost'))
    adCostDf = adCostDf.loc[:,~adCostDf.columns.str.match('Unnamed')]
    dateGroupDf = getDateGroupDf()
    adCostDf2 = addDateGroup(adCostDf,dateGroupDf)
    adCostDf3 = addMediaGroup(adCostDf2)
    adCostDf4 = adCostDf3.groupby(by=['install_date_group','media_group'],as_index=False).agg({
        'cost':'sum'
    }).sort_values(by = ['install_date_group','media_group'],ignore_index=True)
    if __debug__:
        adCostDf3.to_csv(getFilename('adCost3'))
        adCostDf4.to_csv(getFilename('adCost4'))

    # 分媒体计算ROI，并画图 
    plt.title("7d ROI")
    plt.figure(figsize=(10.8, 3.2))
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaDf = pd.read_csv(getFilename('step5_%s'%(name)))
        mediaAdCost = adCostDf4.loc[adCostDf4.media_group == name]
        mergeDf = mediaDf.merge(mediaAdCost,how='left',on=['install_date_group'])
        mergeDf.set_index(["install_date_group"], inplace=True)
        mergeDf.loc[:,'roi'] = mergeDf['r7usd']/mergeDf['cost']
        mergeDf['roi'].plot(label = name)
        mergeDf = cleanDf(mergeDf)
        mergeDf.to_csv(getFilename('roi_%s'%(name)))
    
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig('/src/data/zk/zk_roi.png')
    plt.clf()

def report2():
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaDf = pd.read_csv(getFilename('step5_%s'%(name)))
        mediaDf.set_index(["install_date_group"], inplace=True)

        plt.title("7d revenue")
        plt.figure(figsize=(10.8, 3.2))
        mediaDf['r7usd'].plot(label = 'total')
        mediaDf['r7usd_idfa'].plot(label = 'idfa')
        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.tight_layout()
        plt.savefig('/src/data/zk/zk_r7_%s.png'%(name))
        plt.clf()

        mediaDf['r7/r1'] = mediaDf['r7usd']/mediaDf['r1usd']
        mediaDf['r7/r1_idfa'] = mediaDf['r7usd_idfa']/mediaDf['r1usd_idfa']
        
        plt.title("7d revenue/1d revenue")
        plt.figure(figsize=(10.8, 3.2))
        mediaDf['r7/r1'].plot(label = 'r7/r1')
        mediaDf['r7/r1_idfa'].plot(label = 'r7/r1 idfa')
        plt.xticks(rotation=45)
        plt.legend(loc='best')
        plt.tight_layout()
        plt.savefig('/src/data/zk/zk_r7Pr1_%s.png'%(name))
        plt.clf()

    # plt.title("7d revenue/1d revenue")
    # plt.figure(figsize=(10.8, 3.2))
    # for media in mediaList:
    #     name = media['name']
    #     if name == 'unknown':
    #         continue
    #     mediaDf = pd.read_csv(getFilename('step5_%s'%(name)))
    #     mediaDf.set_index(["install_date_group"], inplace=True)
    #     mediaDf['r7/r1_idfa'] = mediaDf['r7usd_idfa']/mediaDf['r1usd_idfa']
    #     mediaDf['r7/r1_idfa'].plot(label = name)
    # plt.xticks(rotation=45)
    # plt.legend(loc='best')
    # plt.tight_layout()
    # plt.savefig('/src/data/zk/zk_2_r7Pr1.png')
    # plt.clf()

# 将概率归因比例比较大的比例所占金额，和总金额进行比较
def report4():
    df = pd.read_csv('/src/data/zk/step3.csv')
    sum = df['sumr7usd'].sum()
    for p in (0.2,0.5,0.8):
        mediaSum = 0
        for media in mediaList:
            name = media['name']
            if name == 'unknown':
                continue
            mediaSum += df.loc[df[name]>p]['%s_r7usd'%name].sum()
        print(p,mediaSum/sum)
    return


def main(afDf = None,skanDf = None):
    if __debug__:
        print('debug 模式，会保存中间过程，效率较低')

    if afDf == None:
        afDf = pd.read_csv(getFilename('iOSAF20220501_20230227'))
        afDf = afDf.loc[:,~afDf.columns.str.match('Unnamed')]
    if skanDf == None:
        skanDf = pd.read_csv(getFilename('iOSSKAN20220501_20230227'))
        skanDf = skanDf.loc[:,~skanDf.columns.str.match('Unnamed')]

    skanDf = skanDf.rename(columns={'install_date_af':'install_date'})
    dateGroupDf = getDateGroupDf()

    afDf2 = addDateGroup(afDf,dateGroupDf)
    skanDf2 = addDateGroup(skanDf,dateGroupDf)

    afDf3 = addMediaGroup(afDf2)
    skanDf3 = addMediaGroup(skanDf2)

    # 做一次汇总,不再关注idfa，只是看他是否有AF归因结果，相信模糊归因
    # afDf4 = afDf3.groupby(by=['install_date_group','media_group','cv','had_idfa'],as_index=False).agg({
    afDf4 = afDf3.groupby(by=['install_date_group','media_group','cv'],as_index=False).agg({
        'count':'sum',
        'sumr1usd':'sum',
        'sumr7usd':'sum',
    })

    skanDf4 = skanDf3.groupby(by=['install_date_group','media_group','cv'],as_index=False).agg({
        'count':'sum'
    })

    if __debug__:
        skanDf3.to_csv(getFilename('step0_skanDf3'))
        skanDf4.to_csv(getFilename('step0_skanDf4'))
        afDf4.to_csv(getFilename('step0_afDf4'))
    idfaDf, noIdfaDf = step1(afDf4)
    step2Df = step2(skanDf4,idfaDf)

    # print(stepDf2)
    step3Df = step3(step2Df,noIdfaDf)
    step4(step3Df)
    step5(idfaDf)

def cleanDf(df):
    df = df.loc[:,~df.columns.str.match('Unnamed')]
    return df

def report3():
    # 主要是计算一下各种相关度
    for media in mediaList:
        name = media['name']
        if name == 'unknown':
            continue
        mediaDf = pd.read_csv(getFilename('step5_%s'%(name)))
        mediaDf.loc[:,'p71'] = mediaDf['r7usd_attrbution']/mediaDf['r1usd_attrbution']
        mediaDf.loc[:,'p71_idfa'] = mediaDf['r7usd_idfa']/mediaDf['r1usd_idfa']
        
        corr = mediaDf.corr()
        print(name,'首日付费金额 idfa 与 分配式归因 关联',corr['r1usd_idfa'].values[1])
        print(name,'7日付费金额 idfa 与 分配式归因 关联',corr['r7usd_idfa'].values[2])
        print(name,'7日付费金额/首日付费金额 idfa 与 分配式归因 关联',corr['p71_idfa'].values[7])

if __name__ == '__main__':
    df = getDataFromAF()
    df.to_csv(getFilename('iOSAF20220501_20230227'))

    # df = getDataFromSKAN()
    # df.to_csv(getFilename('iOSSKAN20220501_20230227'))

    # main()
    # report()
    # report2()
    # report3()
    report4()
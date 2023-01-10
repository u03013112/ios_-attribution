# 获得数据
import datetime
import pandas as pd


import sys
sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

# 花费、收入、展示、点击、安装、回收（次数 or 人数）
# 安装数不太准，可能要用付费用户（1日&7日）来替代


# 获取广告部分信息
# 花费、收入、展示、点击
# 其他的也先取出来，但是不准，看看是否有帮助，比如首日留存，次日留存
# 暂时不分媒体，直接获取完整的
def getAdData(sinceTimeStr,unitlTimeStr):
    sql = '''
        select
            install_day,
            sum(kpi_impressions) as impressions,
            sum(kpi_clicks) as clicks,
            sum(kpi_installs) as installs,
            sum(cost_value_usd) as cost,
            sum(kpi_retention_0) as u0,
            sum(kpi_retention_1) as u1
        from 
            dwd_base_summary_cost
        where
            install_day >=%s and install_day <%s
            and app_package = 'id1479198816'
            and app = 102
        group by
            install_day
        ;
    '''%(sinceTimeStr,unitlTimeStr)

    print(sql)
    pd_df = execSql(sql)
    return pd_df

# 获得首日付费人数，首日总收入，7日总收入
# 暂时没有付费人数，分开查吧
def getPurchaseData(sinceTimeStr,unitlTimeStr):
    sql = '''
        select
            install_day,
            sum(if(life_cycle <= 0, revenue_value_usd, 0)) as r1usd,
            sum(if(life_cycle <= 6, revenue_value_usd, 0)) as r7usd
        from
            (
                select
                    game_uid as uid,
                    install_day,
                    revenue_value_usd,
                    DATEDIFF(
                        to_date(day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) as life_cycle
                from
                    dwd_base_event_purchase_afattribution
                where
                    app_package = "id1479198816"
                    and app = 102
                    and zone = 0
                    and window_cycle = 9999
                    and install_day >= %s
                    and install_day <= %s
            )
        group by
            install_day
    '''%(sinceTimeStr,unitlTimeStr)

    print(sql)
    pd_df = execSql(sql)
    return pd_df
    
# 获得首日付费人数
def getPurchaseUserData(sinceTimeStr,unitlTimeStr):
    sql = '''
        select
            install_day,
            count(distinct uid) as users
        from
            (
                select
                    game_uid as uid,
                    install_day,
                    revenue_value_usd,
                    DATEDIFF(
                        to_date(day, 'yyyymmdd'),
                        to_date(install_day, 'yyyymmdd'),
                        'dd'
                    ) as life_cycle
                from
                    dwd_base_event_purchase_afattribution
                where
                    app_package = "id1479198816"
                    and app = 102
                    and zone = 0
                    and window_cycle = 9999
                    and install_day >= %s
                    and install_day <= %s
            )
        where
            life_cycle <= 0
        group by
            install_day
    '''%(sinceTimeStr,unitlTimeStr)

    print(sql)
    pd_df = execSql(sql)
    return pd_df
    
# 指标包括ROI（d1 d7 p7）、CPM、点击率 转化率、付费率 CPA
# 由于安装数不准，所以付费率也不准，可能暂时不能用
# 可能需要用cpup来做
def getAdvData(sinceTimeStr,unitlTimeStr):
    # 先把之前数据组合起来
    adDataDf = pd.read_csv(getFilename('adData20220501_20221201'))
    purchaseDataDf = pd.read_csv(getFilename('purchaseData20220501_20221201'))
    purchaseUserCountDataDf = pd.read_csv(getFilename('purchaseUserCountData20220501_20221201'))

    # print(adDataDf,purchaseDataDf)
    adDataDf = adDataDf.loc[:,~adDataDf.columns.str.match('Unnamed')]
    purchaseDataDf = purchaseDataDf.loc[:,~purchaseDataDf.columns.str.match('Unnamed')]
    purchaseUserCountDataDf = purchaseUserCountDataDf.loc[:,~purchaseUserCountDataDf.columns.str.match('Unnamed')]


    mergeDf0 = pd.merge(adDataDf,purchaseDataDf,on=['install_day'])
    mergeDf1 = pd.merge(mergeDf0,purchaseUserCountDataDf,on=['install_day'])

    mergeDf1 = mergeDf1.sort_values('install_day',ignore_index=True)

    # 添加一些高阶数据
    advDf = mergeDf1

    advDf['cpm'] = advDf['cost']/advDf['impressions']*1000
    advDf['ctr'] = advDf['clicks']/advDf['impressions']
    advDf['cvr'] = advDf['installs']/advDf['clicks']
    advDf['cc'] = advDf['installs']/advDf['impressions']
    advDf['payrate'] = advDf['users']/advDf['installs']
    advDf['cpup'] = advDf['cost']/advDf['users']
    advDf['cpi'] = advDf['cost']/advDf['installs']
    advDf['l1'] = advDf['u1']/advDf['installs']
    advDf['roi1'] = advDf['r1usd']/advDf['cost']
    advDf['roi'] = advDf['r7usd']/advDf['cost']

    return advDf

# 尝试获得标签数据
# 标签是当日应该如何操作才能尽量保持ROI趋向于KPI
def getLabelData():
    df = pd.read_csv(getFilename('advData20220501_20221201'))
    # 先添加KPI，这个据说年底改了，到时候再详细添加，暂时统一添加6%
    df['kpi'] = 0.06
    # 定个大概得宽容幅度，防止频繁的变化，暂定10%
    lossRange = 0.1
    df['kpi0'] = df['kpi']*(1 - lossRange)
    df['kpi1'] = df['kpi']*(1 + lossRange)
    # 定个可视时间窗口，暂定7天，即判断是否要改变预算，由后续7天的平均值来定
    afterNDay = 7
    # 这里要倒序是因为pd只有向前的rolling
    dfR = df.sort_values(by=['install_day'],ascending = False).reset_index()
    dfR['costN'] = dfR['cost'].rolling(window=afterNDay).mean()
    dfR['r7usdN']= dfR['r7usd'].rolling(window=afterNDay).mean()
    dfR['roiN'] = dfR['r7usdN']/dfR['costN']
    dfR = dfR.sort_values(by=['install_day'],ascending = True).reset_index()
    df['roiN'] = dfR['roiN']

    # 暂时把这种标签叫做label0，之后如果有别的标签往后顺延
    # 0代表不需要增减花费
    df['label0'] = 0
    # 1代表建议增加预算
    df.loc[df.roiN > df.kpi1,'label0'] = 1
    # 2代表建议减少预算
    df.loc[df.roiN < df.kpi0,'label0'] = 2

    return df
    

def addMACD(df,name):
    exp1 = df[name].ewm(span=12, adjust=False).mean()
    exp2 = df[name].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    exp3 = macd.ewm(span=9, adjust=False).mean()
    his = (macd - exp3) * 2

    df['%s_dif'%name] = macd
    df['%s_dea'%name] = exp3
    df['%s_his'%name] = his

def addMACDMain():
    df = pd.read_csv(getFilename('advData20220501_20221201'))
    addMACD(df,'r1usd')
    addMACD(df,'users')
    addMACD(df,'cpm')
    addMACD(df,'cpup')
    addMACD(df,'roi1')
    addMACD(df,'roi')
    df.to_csv(getFilename('advData20220501_20221201'))

# 尝试对osc进行分析
# N是分析周期
def osc(df,nameList,N = 3):
    df['install_day'] = df['install_day'].astype('string')
    installDayList = df['install_day'].unique()

    nameList2 = []
    for name in nameList:
        n = '%s_his'%name
        nameList2.append(n)

    for installDay in installDayList:
        untilDay = datetime.datetime.strptime(installDay,'%Y%m%d')
        sinceDay = untilDay - datetime.timedelta(days=N)
        sinceDayStr = sinceDay.strftime("%Y%m%d")
        # 拿到最近N天的数据
        dfNDay = df.loc[(df.install_day >= sinceDayStr) & (df.install_day <= installDay)]
        dfNDayDiff =  dfNDay[nameList2].diff()
        dfNDayDiff = dfNDayDiff.fillna(0)
        
        for name in nameList2:
            # 先判断是否全部大于0，认为是一个上涨状态
            if all(dfNDay[name] > 0):
                if all(dfNDayDiff[name] >= 0):
                    # 加速上升中
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'fastUp'
                elif all(dfNDayDiff[name] <= 0):
                    # 减速上升中
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'slowUp'
                else:
                    # 变速上升，但是没有明显趋势，忽快忽慢
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'up'
            elif all(dfNDay[name] < 0):
                if all(dfNDayDiff[name] >= 0):
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'slowDown'
                elif all(dfNDayDiff[name] <= 0):
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'fastDown'
                else:
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'down'
            else:
                if dfNDay[name].iloc[len(dfNDay)-1] > 0:
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'changeToUp'
                else:
                    df.loc[df.install_day == installDay,'%s_osc%d'%(name,N)] = 'changeToDown'
    return df



# 尝试添加前N日的数据统计
# 包括 前N日的平均值，达到KPI的平均值（超过10%的也可以统计一下）
# 这个值在所有数据中处于什么水平，在达到KPI的数值中处于什么水平
def valueAnalyze(df,nameList,N = 30):
    # 先添加KPI，这个据说年底改了，到时候再详细添加，暂时统一添加6%
    df['kpi'] = 0.06

    df['install_day'] = df['install_day'].astype('string')
    installDayList = df['install_day'].unique()
    for installDay in installDayList:
        untilDay = datetime.datetime.strptime(installDay,'%Y%m%d')
        sinceDay = untilDay - datetime.timedelta(days=N)
        sinceDayStr = sinceDay.strftime("%Y%m%d")
        # print(sinceDayStr,installDay)
        # print(df)
        # 找到这段时间的数据
        dfNDay = df.loc[(df.install_day >= sinceDayStr) & (df.install_day <= installDay)]
        dfNDayKPI = dfNDay.loc[dfNDay.roi > dfNDay.kpi]
        for name in nameList:
            dfIndex = df[df.install_day == installDay].index.tolist()[0]
            nameValue = df[name].loc[dfIndex]

            # 前N日的平均值，这个可以直接roll，但是和后面统一，就这么写吧
            df.loc[df.install_day == installDay,'%s%d_mean'%(name,N)] = dfNDay[name].mean()
            # 达到KPI的平均值
            df.loc[df.install_day == installDay,'%s%d_kpi_mean'%(name,N)] = dfNDayKPI[name].mean()
            
            # 在所有数值中的位置
            allDf = dfNDay.sort_values(by=name).reset_index(drop=True)
            allDfIndex = allDf[allDf[name] == nameValue].index.tolist()[0] + 1
            df.loc[df.install_day == installDay,'%s%d_pos'%(name,N)] = allDfIndex / len(allDf)
            # 在满足KPI数值中的位置
            if df['roi'].loc[dfIndex] > df['kpi'].loc[dfIndex]:
                kpiDf = dfNDayKPI.sort_values(by=name).reset_index(drop=True)
                kpiDfIndex = kpiDf[kpiDf[name] == nameValue].index.tolist()[0] + 1
                # print(name,installDay,kpiDfIndex,len(kpiDf))
                df.loc[df.install_day == installDay,'%s%d_kpi_pos'%(name,N)] = kpiDfIndex / len(kpiDf)
            else:
                # 此数据不在kpi范围内，就直接在最低位置
                df.loc[df.install_day == installDay,'%s%d_kpi_pos'%(name,N)] = -0.1
    return df


def getCorr(df):
    c = df.corr()
    print(c)
    return c

if __name__ == '__main__':
    # if __debug__:
    #     print('debug 模式，并未真的sql')
    # else:
    #     df = getAdData('20220501','20221201')
    #     df.to_csv(getFilename('adData20220501_20221201'))

    #     df = getPurchaseData('20220501','20221201')
    #     df.to_csv(getFilename('purchaseData20220501_20221201'))

    #     df = getPurchaseUserData('20220501','20221201')
    #     df.to_csv(getFilename('purchaseUserCountData20220501_20221201'))
    
    # df = getAdvData('20220501','20221201')
    # df.to_csv(getFilename('advData20220501_20221201'))
    # getCorr(df).to_csv(getFilename('corr20220501_20221201'))

    # df = getLabelData()
    # df.to_csv(getFilename('labelData20220501_20221201'))

    # df = pd.read_csv(getFilename('advData20220501_20221201'))
    # retDf = valueAnalyze(df,['r1usd','users','cpm','cpup','roi1','roi'])
    # retDf.to_csv(getFilename('vaData20220501_20221201'))

    df = pd.read_csv(getFilename('vaData20220501_20221201'))
    retDf = osc(df,['r1usd','users','cpm','cpup','roi1','roi'])
    retDf.to_csv(getFilename('oscData20220501_20221201'))
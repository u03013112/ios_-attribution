# 将iOSWeekly的代码进行整理
# 逻辑统一整理为：获取数据 -> 生成数据 -> 生成报告
# 将数据整理成一个标准的数据结构，然后根据数据结构生成报告

import pandas as pd

import sys
sys.path.append('/src')

from src.report.geo import getIOSGeoGroup01
from src.report.media import getIOSMediaGroup01


directory = '/src/data/report/iOSWeekly20231018_20231025'

def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)


# 输入参数：
# df: DataFrame，要求至少包含 install_date:类似20231001 string
# target:需要进行分析的目标，类似{'name':'roi','op':'/', 'targetList':['revenue','cost']}，name是目标的名称，op是目标的操作，targetList是目标的列表
#   target 中的op枚举，目前支持'/'和'0'，其中'/'代表除法，'0'代表求直接用targetList中的第一个值
#   tatget 添加了format，用于格式化输出，比如'.2f%'，则输出的时候会保留两位小数，并且加上百分号
# groupBy：类似'geoGroup' or 'mediaGroup'，本report将针对install_date+groupBy进行groupby并生成报告，groupBy可以是None
# startDayStr1:分析目标的开始日期，类似20231001 string
# endDayStr1:分析目标的结束日期，类似20231001 string
# df2：DataFrame，对比目标，如果为空，则不进行对比
# startDayStr2:对比目标的开始日期，类似20231001 string，如果df2为空，此参数无效
# endDayStr2:对比目标的结束日期，类似20231001 string，如果df2为空，此参数无效
# compareNameStr:对比的名称，string类型，比如‘环比’或者‘同比’，如果df2为空，此参数无效
# needMACD:是否需要计算MACD，bool类型，如果需要计算MACD
# 返回值：reportStr string，报告的内容，markdown格式
# 对于此函数的调用，如果希望针对过滤后的数据做出报告，可以在调用之前先对df进行过滤
def getReport(df,target,groupBy = [],startDayStr1='20231001',endDayStr1='20231007',df2=None,startDayStr2='',endDayStr2='',compareNameStr='',needMACD=False):
    reportStr = ''

    df1 = df[(df['install_date'] >= startDayStr1) & (df['install_date'] <= endDayStr1)].copy()

    aggDict = {}
    for targetItem in target['targetList']:
        aggDict[targetItem] = 'sum'
    groupByList = ['install_date']
    if groupBy != None:
        groupNameList = df1[groupBy].unique().tolist()
        groupByList.append(groupBy)
    else:
        groupNameList = ['all']
    df1 = df1.groupby(groupByList).agg(aggDict).reset_index()
    
    if df2 is not None:
        df2 = df2[(df2['install_date'] >= startDayStr2) & (df2['install_date'] <= endDayStr2)].copy()
        df2 = df2.groupby(groupByList).agg(aggDict).reset_index()
    
    for groupName in groupNameList:
        reportStr += '\\textbf{%s}\n\n'%groupName
        if groupBy != None:
            groupDf = df1[df1[groupBy] == groupName].copy()
        else:
            groupDf = df1.copy()

        ret1 = groupDf[target['targetList'][0]].sum()
        if target['op'] == '/':
            ret1 = groupDf[target['targetList'][0]].sum()/groupDf[target['targetList'][1]].sum()

        ret1Str = '%f'%ret1
        if target['format'] == '.2f%':
            ret1Str = '%.2f%%'%(ret1*100)

        reportStr += '%s~%s %s %s\n\n'%(startDayStr1,endDayStr1,target['name'],ret1Str)

        if isinstance(df2, pd.DataFrame):
            if groupBy != None:
                groupDf2 = df2[df2[groupBy] == groupName].copy()
            else:
                groupDf2 = df2.copy()
            ret2 = groupDf2[target['targetList'][0]].sum()
            if target['op'] == '/':
                ret2 = groupDf2[target['targetList'][0]].sum()/groupDf2[target['targetList'][1]].sum()

            # 差异比例
            rate = (ret1 - ret2)/ret2
            color = 'red'
            if rate < 0:
                color = 'green'

            ret2Str = '%f'%ret2
            if target['format'] == '.2f%':
                ret2Str = '%.2f%%'%(ret2*100)

            reportStr += '%s %s~%s %s %s(\\protect\\textcolor{%s}{%.2f\\%%})\n\n'%(compareNameStr,startDayStr2,endDayStr2,target['name'],ret2Str,color,rate*100)
    
    return reportStr

from iOSWeekly import getAdCostData,getDataFromMC,getDataFromMC2

if __name__ == '__main__':
    startDayStr = '20230826'
    endDayStr = '20231025'

    geoGroupList = getIOSGeoGroup01()

    adCostDf = getAdCostData(startDayStr,endDayStr)
    adCostDf.rename(columns={'day':'install_date'},inplace=True)
    adCostDf.loc[adCostDf.media_source == 'Facebook Ads','media_source'] = 'facebook'
    adCostDf.loc[adCostDf.media_source == 'googleadwords_int','media_source'] = 'google'
    adCostDf.loc[adCostDf.media_source == 'bytedanceglobal_int','media_source'] = 'bytedanceglobal'
    # 将所有media_source == 'tiktokglobal_int' 的行删掉，这个不知道是干嘛的
    adCostDf = adCostDf.loc[adCostDf.media_source != 'tiktokglobal_int']

    adCostDf.loc[~adCostDf.media_source.isin(['facebook','google','bytedanceglobal']),'media_source'] = 'other'
    adCostDf.rename(columns={'media_source':'media'},inplace=True)
    
    adCostDf['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        adCostDf.loc[adCostDf.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    adCostDf = adCostDf.groupby(['install_date','geoGroup','media'],as_index=False).agg({'cost':'sum'})
    

    df = getDataFromMC(startDayStr,endDayStr)
    df = getDataFromMC2(df)
    df['geoGroup'] = 'other'
    for geoGroup in geoGroupList:
        df.loc[df.country_code.isin(geoGroup['codeList']),'geoGroup'] = geoGroup['name']
    
    df = df.groupby(['install_date','geoGroup','media'],as_index=False).sum().reset_index(drop=True)
    df = df.merge(adCostDf,on=['geoGroup','install_date','media'],how='outer').fillna(0)

    df['install_date'] = df['install_date'].astype(str)
    # 生成报告
    reportStr = getReport(
        df,
        {'name':'ROI_1d','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},
        groupBy='geoGroup',
        startDayStr1='20231018',
        endDayStr1='20231024',
        df2=df,
        startDayStr2='20231011',
        endDayStr2='20231017',
        compareNameStr='环比',
        )
    # 输出报告
    print(reportStr)
# 将iOSWeekly的代码进行整理
# 逻辑统一整理为：获取数据 -> 生成数据 -> 生成报告
# 将数据整理成一个标准的数据结构，然后根据数据结构生成报告

import pandas as pd
import matplotlib.pyplot as plt

import sys
sys.path.append('/src')

from src.report.geo import getIOSGeoGroup01
from src.report.media import getIOSMediaGroup01
from src.report.iOSWeekly import toPdf,headStr


directory = '/src/data/report/iOSWeekly20231018_20231025'

def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)


# 将macdAnalysis从iOSWeekly中移动到这里
def macdAnalysis(df,target='ROI_1d',startDayStr='20231001',endDayStr='20231007', analysisDayCount=7,picFilenamePrefix=''):
    # print('macdAnalysis:',startDayStr,endDayStr,analysisDayCount)
    # 画图
    df = df.copy()
    df.sort_values(by=['install_date'], inplace=True)

    df['EMA12'] = df[target].ewm(span=12).mean()
    df['EMA26'] = df[target].ewm(span=26).mean()

    # 计算MACD值
    df['MACD'] = df['EMA12'] - df['EMA26']

    # 计算9日EMA作为信号线
    df['Signal'] = df['MACD'].ewm(span=9).mean()

    # 选择最近两周的数据（升序排序后的前14行）
    last_draw_days = df.loc[(df['install_date']>=startDayStr) & (df['install_date']<=endDayStr)]

    # 将install_date转换为datetime格式
    df['install_date'] = pd.to_datetime(df['install_date'])
    
    # 创建一个画布，包含两个子图
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6, 6), sharex=True)

    # 绘制ROI走势图
    ax1.plot(last_draw_days['install_date'], last_draw_days[target], label='ROI', color='b')
    ax1.legend(loc='upper left')
    ax1.set_ylabel('ROI Value')
    ax1.set_title(f'{target} Trend ({startDayStr}~{endDayStr})')
    ax1.grid()

    # 绘制MACD和信号线
    ax2.plot(last_draw_days['install_date'], last_draw_days['MACD'], label='MACD', color='b')
    ax2.plot(last_draw_days['install_date'], last_draw_days['Signal'], label='Signal', color='r')
    ax2.legend(loc='upper left')
    ax2.set_xlabel('Install Date')
    ax2.set_ylabel('MACD Value')
    ax2.set_title(f'MACD Trend for {target} ({startDayStr}~{endDayStr})')
    ax2.grid()
    # 设置x轴刻度标签的旋转角度
    plt.xticks(rotation=45)
    # 保存图像到文件
    picFilename = getFilename(f'{picFilenamePrefix}_{startDayStr}_{endDayStr}_{target}_macd', 'png')
    plt.savefig(picFilename, dpi=300, bbox_inches='tight')
    print(f'生成图片：{picFilename}')
    plt.close()

    # 分析最近analysisDayCount天的MACD趋势
    reportStr = ''
    last_analysis_days = df.loc[(df['install_date'] >= startDayStr) & (df['install_date'] <= endDayStr)].tail(analysisDayCount)
    macd_cross = (last_analysis_days['MACD'] - last_analysis_days['Signal'])

    # 计算每天的趋势，存到安装日期，趋势这样的表中，每天一个趋势，按照安装日期升序
    daily_trend = pd.DataFrame()
    daily_trend['install_date'] = last_analysis_days['install_date']
    daily_trend['trend'] = macd_cross > 0
    daily_trend['trend'] = daily_trend['trend'].apply(lambda x: "\\protect\\textcolor{red}{上涨}" if x else "\\protect\\textcolor{green}{下跌}")

    # 从第一行遍历，记录初始趋势，每次找到趋势与初始趋势不一致的时候停止，输出需要的结果
    reportStr += "#### 趋势分析（MACD分析）\n\n"
    
    reportStr += f'最近{analysisDayCount}天的主要趋势：\n\n'
    start_date = daily_trend['install_date'].iloc[0]
    initial_trend = daily_trend['trend'].iloc[0]
    for i in range(1, len(daily_trend)):
        current_trend = daily_trend['trend'].iloc[i]
        if current_trend != initial_trend:
            end_date = daily_trend['install_date'].iloc[i - 1]
            if start_date != end_date:
                reportStr += f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')} {initial_trend}\n\n"
            start_date = daily_trend['install_date'].iloc[i]
            initial_trend = current_trend
    reportStr += f"{start_date.strftime('%Y-%m-%d')}~{daily_trend['install_date'].iloc[-1].strftime('%Y-%m-%d')} {initial_trend}\n\n\n"

    reportStr += "\\textbf{针对最近趋势进行展望：}\n\n"
    reportStr += f"({start_date.strftime('%Y-%m-%d')}~{daily_trend['install_date'].iloc[-1].strftime('%Y-%m-%d')})"
    # 对最后一波趋势进行细化分析
    last_trend_duration = (daily_trend['install_date'].iloc[-1] - start_date).days + 1
    if last_trend_duration <= 1:
        reportStr += "\n这是一个波动期。没有明显趋势。"
    else:
        reportStr += f"\n这是一个持续{initial_trend}趋势。"
        macd_signal_diff = last_analysis_days['MACD'] - last_analysis_days['Signal']
        if abs(macd_signal_diff.iloc[-1]) - abs(macd_signal_diff.iloc[-2]) < 0:
            reportStr += "但是趋势有减弱情况，这个趋势可能即将结束。"
        else:
            reportStr += "趋势还在增强，这个趋势可能仍将继续。"
        reportStr += '\n\n'
    # 

    reportStr += r'''
\begin{figure}[!h]
    \centering
    \includegraphics{''' + f'./{picFilenamePrefix}_{startDayStr}_{endDayStr}_{target}_macd.png'+'''}
\end{figure}
\FloatBarrier
    '''

    return reportStr



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

        if needMACD:
            # 计算analysisDayCount，endDayStr1 - startDayStr1 + 1
            analysisDayCount = (pd.to_datetime(endDayStr1) - pd.to_datetime(startDayStr1)).days + 1

            groupDf[target['name']] = groupDf[target['targetList'][0]]
            if target['op'] == '/':
                groupDf[target['name']] = groupDf[target['targetList'][0]]/groupDf[target['targetList'][1]]

            reportStr += macdAnalysis(groupDf,target=target['name'],startDayStr=startDayStr1,endDayStr=endDayStr1,analysisDayCount=analysisDayCount,picFilenamePrefix='%s_%s'%(groupName,target['name']))
    
    return reportStr

def debug(df):
    df = df.groupby(['install_date'],as_index=False).sum().reset_index(drop=True)
    print(df)

from data.adCost import getAdCostDataIOSGroupByGeoAndMedia
from data.revenue import getRevenueDataIOSGroupByGeoAndMedia
if __name__ == '__main__':
    startDayStr = '20230826'
    endDayStr = '20231025'

    geoGroupList = getIOSGeoGroup01()

    
    adCostDf = getAdCostDataIOSGroupByGeoAndMedia(startDayStr,endDayStr,directory)
    revenueDf = getRevenueDataIOSGroupByGeoAndMedia(startDayStr,endDayStr,directory)
    df = revenueDf.merge(adCostDf,on=['geoGroup','install_date','media'],how='outer').fillna(0)

    # 生成报告
    reportStr = headStr
    reportStr += getReport(
        df,
        {'name':'ROI_1d','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},
        groupBy='geoGroup',
        startDayStr1='20231018',
        endDayStr1='20231024',
        df2=df,
        startDayStr2='20231011',
        endDayStr2='20231017',
        compareNameStr='环比',
        needMACD=True
    )
    # 输出报告
    print(reportStr)
    with open(f'{directory}/report.md','w',encoding='utf-8') as f:
        f.write(reportStr)

    print(f'{directory}/report.md')
    toPdf(directory)
# iOS 海外，用融合归因结论作为归因结论的周报
# 与上一级目录中的同名文件相比，这个版本利用data中的api
# 增加了更多的数据，并且不仅局限于分媒体和分国家，而且进一步细分到campaign

import os
import datetime
import subprocess
import pandas as pd

import sys
sys.path.append('/src')

from src.report.data.ad import getAdDataIOSGroupByCampaignAndGeoAndMedia
from src.report.data.revenue import getRevenueDataIOSGroupByCampaignAndGeoAndMedia
from src.report.report.report import toPdf,headStr,getReport


def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)

headStr = '''
---
CJKmainfont: WenQuanYi Zen Hei
---

---
header-includes:
  - \\usepackage{color}
  - \\usepackage{xcolor}
  - \\usepackage{placeins}
---

'''

# 获得目前的UTC0日期，格式20231018
today = datetime.datetime.utcnow()
todayStr = today.strftime('%Y%m%d')

# for test
# todayStr = '20231025'
# today = datetime.datetime.strptime(todayStr,'%Y%m%d')

print('今日日期：',todayStr)
# 获得N天的数据
N = 7
# 获得一周前的UTC0日期，格式20231011，往前一天，不获取今天的不完整数据。
startDayStr = (today - datetime.timedelta(days=N+1)).strftime('%Y%m%d')
endDayStr = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')
print('查询日期：',startDayStr,'~',endDayStr)

directory = f'/src/data/report/iOSWeekly{startDayStr}_{endDayStr}'

def main():
    if not os.path.exists(directory):
        os.makedirs(directory)

    # 为了可以环比，需要额外多获得N天的数据
    # 另外，由于收入输入采用融合归因结论，融合归因只能获得大前天的数据，所以再多往前获得2天的数据，保险起见，直接多获得5天的数据
    startDayStrFix = (today - datetime.timedelta(days=N*2+5)).strftime('%Y%m%d')
    endDayStrFix = (today - datetime.timedelta(days=1)).strftime('%Y%m%d')

    adCostDf = getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStrFix,endDayStrFix,directory)
    revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMedia(startDayStrFix,endDayStrFix,directory)

    df = pd.merge(adCostDf,revenueDf,on=[
        'install_date','campaign_id','campaign_name','media','geoGroup'
        ],how='outer',suffixes=('_ad','_revenue'))

    df = df.fillna(0)

    # 生成报告需求
    # 1、基础数据，目前所有的基础数据：CPM，CTR，CVR，CPI，Cost，ROI1，ROI3，ROI7
    # 2、分维度：大盘，媒体，国家
    # 3、在上面中的媒体中，为每个媒体单独的生成一份报告，在媒体的报告中，分国家、分campaign进行分析

    reportStr = '' + headStr
    reportStr += f'# {startDayStr}~{endDayStr} iOS 海外周报 大盘版\n\n'

    reportStr += '## 大盘汇总数据\n\n'

    # 不涉及融合归因的部分就用这个时间段，往前一天，不获取今天的不完整数据。
    startDayStr1 = startDayStr
    endDayStr1 = endDayStr
    # 环比，将上面日期往前推N天
    startDayStr2 = (datetime.datetime.strptime(startDayStr1, "%Y%m%d") - datetime.timedelta(days=N)).strftime("%Y%m%d")
    endDayStr2 = (datetime.datetime.strptime(endDayStr1, "%Y%m%d") - datetime.timedelta(days=N)).strftime("%Y%m%d")

    reportStr += '### CPM\n\n'
    reportStr += getReport(df,{'name':'CPM','op':'/*1000','targetList':['cost','impression'],'format':'.2f'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=True)

    reportStr += '### CTR\n\n'
    reportStr += getReport(df,{'name':'CTR','op':'/','targetList':['click','impression'],'format':'.2f%'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=True)
    

    reportStr += '### CVR\n\n'
    reportStr += getReport(df,{'name':'CVR','op':'/','targetList':['install_revenue','click'],'format':'.2f%'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=True)

    reportStr += '### CPI\n\n'
    reportStr += getReport(df,{'name':'CPI','op':'/','targetList':['cost','install_revenue'],'format':'.2f'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=True)

    reportStr += '### Cost\n\n'
    reportStr += getReport(df,{'name':'Cost','op':'','targetList':['cost'],'format':'.2f'},startDayStr1=startDayStr,endDayStr1=endDayStr,df2=df,startDayStr2=startDayStr2,endDayStr2=endDayStr2,compareNameStr='环比',needMACD=True)

    # ROI1 涉及融合归因结论，所以需要再往前2天
    startDayStr1ROI1 = (datetime.datetime.strptime(startDayStr1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr1ROI1 = (datetime.datetime.strptime(endDayStr1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    startDayStr2ROI1 = (datetime.datetime.strptime(startDayStr2, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr2ROI1 = (datetime.datetime.strptime(endDayStr2, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")

    reportStr += '### ROI1\n\n'
    reportStr += getReport(df,{'name':'ROI1','op':'/','targetList':['revenue_1d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI1,endDayStr1=endDayStr1ROI1,df2=df,startDayStr2=startDayStr2ROI1,endDayStr2=endDayStr2ROI1,compareNameStr='环比',needMACD=True)

    # ROI3 为了获得3天的数据，需要再往前2天
    startDayStr1ROI3 = (datetime.datetime.strptime(startDayStr1ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr1ROI3 = (datetime.datetime.strptime(endDayStr1ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    startDayStr2ROI3 = (datetime.datetime.strptime(startDayStr2ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")
    endDayStr2ROI3 = (datetime.datetime.strptime(endDayStr2ROI1, "%Y%m%d") - datetime.timedelta(days=2)).strftime("%Y%m%d")

    reportStr += '### ROI3\n\n'
    reportStr += getReport(df,{'name':'ROI3','op':'/','targetList':['revenue_3d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI3,endDayStr1=endDayStr1ROI3,df2=df,startDayStr2=startDayStr2ROI3,endDayStr2=endDayStr2ROI3,compareNameStr='环比',needMACD=True)

    # ROI7 为了获得7天的数据，需要再往前4天
    startDayStr1ROI7 = (datetime.datetime.strptime(startDayStr1ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")
    endDayStr1ROI7 = (datetime.datetime.strptime(endDayStr1ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")
    startDayStr2ROI7 = (datetime.datetime.strptime(startDayStr2ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")
    endDayStr2ROI7 = (datetime.datetime.strptime(endDayStr2ROI3, "%Y%m%d") - datetime.timedelta(days=4)).strftime("%Y%m%d")

    reportStr += '### ROI7\n\n'
    reportStr += getReport(df,{'name':'ROI7','op':'/','targetList':['revenue_7d','cost'],'format':'.2f%'},startDayStr1=startDayStr1ROI7,endDayStr1=endDayStr1ROI7,df2=df,startDayStr2=startDayStr2ROI7,endDayStr2=endDayStr2ROI7,compareNameStr='环比',needMACD=True)

    print(reportStr)


if __name__ == '__main__':
    main()
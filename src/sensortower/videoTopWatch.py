# 视频版topWatch
import datetime

import sys
sys.path.append('/src')

from src.sensortower.intel import getTopApp,getUnifiedAppIds,getCreatives

import rpyc

def videoTopWatch(isDebug=False,N = 20):
    # N是榜单的前N名
    

    # 支持多个国家放在一起
    countryList2 = [
        {'name':'US','codeList':['US']},
        {'name':'KR','codeList':['KR']},
        {'name':'JP','codeList':['JP']},
        {'name':'TW','codeList':['TW']}
    ]

    reportRetList = []

    lastMonday = datetime.datetime.now() - datetime.timedelta(days=7)
    lastMonday = lastMonday - datetime.timedelta(days=lastMonday.weekday())
    lastSunday = lastMonday + datetime.timedelta(days=6)
    lastMondayStr = lastMonday.strftime('%Y-%m-%d')
    lastSundayStr = lastSunday.strftime('%Y-%m-%d')

    print('查询时间段：',lastMondayStr,'-',lastSundayStr)

    for country2 in countryList2:
        # 选用组合的第一个国家作为查询标准，获得APP列表
        country = country2['codeList'][0]
        
        # 由于直接用UnifiedAppId，所以不需要区分平台
        # 先找到top slg N
        topAppDf = getTopApp(os='android', custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='month',limit=20,category='all',countries=country,startDate=lastMondayStr,endDate=lastSundayStr)
        topAppIdList = topAppDf['appId'].tolist()

        topAppUnifiedIds = getUnifiedAppIds(app_id_type='android',app_ids=topAppIdList)
        # print(topAppUnifiedIds)

        # 再将这些app的id放到Creatives获取，找到TopVideo
        unifiedIds = []
        for topAppUnifiedId in topAppUnifiedIds:
            unifiedIds.append(topAppUnifiedId['unified_app_id'])

        creatives = getCreatives(unifiedAppIds=unifiedIds,countries=country2['codeList'],limit=N,start_date=lastMondayStr,end_date=lastSundayStr)
        # 
        for creative in creatives:
            first_seen_at = creative['first_seen_at']
            # first_seen_at 类似 2024-01-02
            # 如果first_seen_at > lastMondayStr，就是在这个时间段内第一次出现的，需要通知
            if first_seen_at > lastMondayStr:
                reportRet = {
                    'country':country2['name'],
                    'appUnifiedId':creative['app_id'],
                    'firstSeenAt':first_seen_at,
                    'creativeUrl':creative['creative_url'],
                }
                reportRetList.append(reportRet)

    # print(reportRetList)
    if len(reportRetList) == 0:
        reportStr = ('本周没有新上榜的视频')
    else:
        reportStr = f'{lastMondayStr}~{lastSundayStr}新上榜Top{N}视频\n'
        lastCountry = ''
        lastAppName = ''
        for reportRet in reportRetList:
            appUnifiedId = reportRet['appUnifiedId']
            app_name = '未知'
            for topAppUnifiedId in topAppUnifiedIds:
                if topAppUnifiedId['unified_app_id'] == appUnifiedId:
                    app_name = topAppUnifiedId['name']
                    break
            if app_name != '未知':
                country = reportRet['country']
                if country != lastCountry:
                    lastCountry = country
                    reportStr += f"{country}：\n"
                if app_name != lastAppName:
                    lastAppName = app_name
                    reportStr += f" {app_name} \n"
                reportStr += f"  首次看到时间 {reportRet['firstSeenAt']}\n"
                reportStr += f"  链接地址：{reportRet['creativeUrl']}\n"
            else:
                print('未找到app_name，这个不该出现',appUnifiedId)

            reportStr += '\n'

    print(reportStr)

    if not isDebug:
        conn = rpyc.connect("192.168.40.62", 10001)
        conn.root.sendMessageWithoutToken(reportStr,'oc_7fc211dc09a35cb55cfe041afb0bae4c')

    return reportStr


            

                
if __name__ == "__main__":
    videoTopWatch()
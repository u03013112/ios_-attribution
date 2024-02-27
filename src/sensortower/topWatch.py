# 监控top榜单
import datetime

import sys
sys.path.append('/src')

from src.sensortower.intel import getRanking
from src.sensortower.iosIdToName import iOSIdToNameWithCountry
from src.sensortower.androidIdToName import androidIdToName

import rpyc

def topWatch():
    # N是榜单的前N名
    # days是一个阈值，当一个app在今天出现在topN中，并且过去days天内都不在topN中，就将这个app报上去
    N = 10
    days = 7

    platformList = [
        'ios','android'
    ]
    countryList = [
        'US','KR','JP','TW'
    ]

    retList = []

    for platform in platformList:
        if platform == 'ios':
            chartTypeIdsList = [
                'topfreeapplications',
                'toppaidapplications',
                'topgrossingapplications'
            ]
            # https://app.sensortower.com/api/docs/static/category_ids.json
            # "7017": "Games/Strategy"
            category = '7017'
        else:
            chartTypeIdsList = [
                'topselling_free',
                'topselling_paid',
                'topgrossing'
            ]
            # "game_strategy": "Strategy"
            category = 'game_strategy'

        chartTypeNamesList = [
            '免费榜',
            '付费榜',
            '畅销榜'
        ]
        
        for country in countryList:
            for i in range(len(chartTypeIdsList)):
                chartTypeName = chartTypeNamesList[i]
                chartTypeId = chartTypeIdsList[i]

                todayAppIdList = []
                lastAppIdList = []

                today = datetime.datetime.now()
                for day in range(days+1):
                    dayStr = (today - datetime.timedelta(days=day)).strftime('%Y-%m-%d')
                    # ranking 是类似 [6448786147, 6473006839, 6476766567, 1660160760,……] 的列表
                    ranking = getRanking(platform=platform,category=category,countries=country,chartTypeIds=chartTypeId,date=dayStr)
                
                    if day == 0:
                        todayAppIdList += ranking[:N]
                    else:
                        lastAppIdList += ranking[:N]

                # 今天的榜单中有，过去days天的榜单中没有
                for j in range(len(todayAppIdList)):
                    appId = todayAppIdList[j]
                    if appId not in lastAppIdList:
                        if platform == 'ios':
                            appName = iOSIdToNameWithCountry(appId,country)
                        else:
                            appName = androidIdToName(appId)

                        ret = {
                            'platform':platform,
                            'country':country,
                            'chartTypeName':chartTypeName,
                            'index':j+1,
                            'appName':appName,
                            'appId':appId,
                            'days':days
                        }
                        retList.append(ret)
                        print(f"{platform} {country} {chartTypeName} 《{appName}》 appID：{appId} 在过去{days}天内首次出现在top{N}中")

    retStr = '' + today.strftime('%Y-%m-%d') + '策略游戏榜单监控结果：\n'
    for ret in retList:
        retStr += f"{ret['platform']} {ret['country']} {ret['chartTypeName']} 第{ret['index']}名 《{ret['appName']}》 appID：{ret['appId']} 在过去{ret['days']}天内首次出现在top{N}中\n"
    
    if len(retList) > 0:    
        conn = rpyc.connect("192.168.40.62", 10001)
        conn.root.sendMessageWithoutToken(retStr,'oc_353fdbdcf86e05d80123fc5e0fca7daa')

def debug():
    conn = rpyc.connect("192.168.40.62", 10001)
    conn.root.sendMessageWithoutToken('测试','oc_1e418dff75881d2b0d85a5f701262cb8')

if __name__ == '__main__':
    topWatch()
    
    

    

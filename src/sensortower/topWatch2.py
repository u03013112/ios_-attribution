# 监控top榜单
# 由于st的api调用次数受限，所以改进算法
# 改为每天抓取前一天的榜单，再存入数据库
# 结果通过查询数据库得到

import datetime
import pandas as pd

import sys
sys.path.append('/src')

from src.maxCompute import getO

from src.sensortower.intel import getRanking
from src.sensortower.iosIdToName import iOSIdToNameWithCountry2
from src.sensortower.androidIdToName import androidIdToName2

import rpyc
import json
o = getO()
# 下面部分就只有线上环境可以用了
from odps.models import Schema, Column, Partition
def createTable():
    columns = [
        Column(name='platform', type='string', comment=''),
        Column(name='country', type='string', comment=''),
        Column(name='chart_type_name', type='string', comment=''),
        Column(name='index', type='bigint', comment=''),
        Column(name='app_name', type='string', comment=''),
        Column(name='app_id', type='string', comment=''),
        Column(name='url', type='string', comment=''),
    ]
    
    partitions = [
        Partition(name='day', type='string', comment='postback time,like 20221018')
    ]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table('sensortower_ranking', schema, if_not_exists=True)
    return table

def writeTable(df,dayStr):
    t = o.get_table('sensortower_ranking')
    t.delete_partition('day=%s'%(dayStr), if_exists=True)
    with t.open_writer(partition='day=%s'%(dayStr), create_partition=True, arrow=True) as writer:
        writer.write(df)

def getDataFromSensorTower():
    today = datetime.datetime.now()
    # 需要往前推一天，今天的榜单未完整，可能会导致漏过一些app
    today = today - datetime.timedelta(days=1)
    dayStr = today.strftime('%Y-%m-%d')

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
                
                # ranking 是类似 [6448786147, 6473006839, 6476766567, 1660160760,……] 的列表
                ranking = getRanking(platform=platform,category=category,countries=country,chartTypeIds=chartTypeId,date=dayStr)
                
                for j in range(len(ranking)):
                    appId = ranking[j]
                    
                    if platform == 'ios':
                        appName,url = iOSIdToNameWithCountry2(appId,country)
                    else:
                        appName,url = androidIdToName2(appId)
                    index = j+1
            
                    retList.append({
                        'platform': platform,
                        'country': country,
                        'chart_type_name': chartTypeName,
                        'index': index,
                        'app_name': appName,
                        'app_id': appId,
                        'url': url,
                    })
               
    df = pd.DataFrame(retList)
    writeTable(df,dayStr)
    print(f'{dayStr} 数据写入成功')
    return

def toGpt(text):
    content = '''
请读下面文本，如果里面没有在书名号中的韩文或者日文，那么请直接将下面文本原样返回。
如果有韩文或者日文，那么请在原有的书名号周免添加"(翻译：中文翻译内容)"。
比如原文中提到 “ ios KR 第8名 《집, 행성 & 사냥꾼》 [跳转至商店](https://apps.apple.com/kr/app/%EC%A7%91-%ED%96%89%EC%84%B1-%EC%82%AC%EB%83%A5%EA%BE%BC/id6478843819?uo=4) ”
那么就将这一行改为 “ ios KR 第8名 《집, 행성 & 사냥꾼》（翻译：家园、星球和猎人） [跳转至商店](https://apps.apple.com/kr/app/%EC%A7%91-%ED%96%89%EC%84%B1-%EC%82%AC%EB%83%A5%EA%BE%BC/id6478843819?uo=4) ”
    '''
    content += text

    message = [
        {"role":"user","content":content}
    ]

    conn = rpyc.connect("192.168.40.62", 10002,config={"sync_request_timeout": 300})
    message_str = json.dumps(message)  # 将message转换为字符串
    x = conn.root.getAiResp(message_str)
    # print(x)
    return x
    

def topWatch(isDebug=False,gpt=False):
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
                # 需要往前推一天，今天的榜单未完整，可能会导致漏过一些app
                today = today - datetime.timedelta(days=1)

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
                            appName,url = iOSIdToNameWithCountry2(appId,country)
                        else:
                            appName,url = androidIdToName2(appId)

                        ret = {
                            'platform':platform,
                            'country':country,
                            'chartTypeName':chartTypeName,
                            'index':j+1,
                            'appName':appName,
                            'appId':appId,
                            'days':days,
                            'url':url,
                        }
                        retList.append(ret)
                        # print(f"{platform} {country} {chartTypeName} 《{appName}》 appID：{appId} 在过去{days}天内首次出现在top{N}中")

    retStr = '' + today.strftime('%Y-%m-%d') + f' {days}天内首次出现在策略游戏类top{N}中的APPs：\n'
    for chartTypeName in chartTypeNamesList:
        retStr += chartTypeName + '：\n'
        count = 0
        for ret in retList:
            if ret['chartTypeName'] == chartTypeName:
                count += 1
                retStr += f"{ret['platform']} {ret['country']} 第{ret['index']}名 《{ret['appName']}》 [跳转至商店]({ret['url']})\n"
        if count == 0:
            retStr += '无\n'
        retStr += '\n'
    
    conn = rpyc.connect("192.168.40.62", 10001)
    if len(retList) > 0:  
        print(retStr)  
        if gpt:
            retStr = toGpt(retStr)
        if not isDebug:
            conn.root.sendMessageWithoutToken(retStr,'oc_353fdbdcf86e05d80123fc5e0fca7daa')
        else:
            conn.root.sendMessageDebug(retStr)
    else:
        retStr = today.strftime('%Y-%m-%d') + '没有发现新的app上榜\n'
        print(retStr)
        if not isDebug:
            conn.root.sendMessageWithoutToken(retStr,'oc_353fdbdcf86e05d80123fc5e0fca7daa')
        else:
            conn.root.sendMessageDebug(retStr)

def test():
    retStr = '''
2024-04-10 7天内首次出现在策略游戏类top10中的APPs：
免费榜：
ios KR 第8名 《집, 행성 & 사냥꾼》 [跳转至商店](https://apps.apple.com/kr/app/%EC%A7%91-%ED%96%89%EC%84%B1-%EC%82%AC%EB%83%A5%EA%BE%BC/id6478843819?uo=4)
android KR 第8名 《Raid Rush: Tower Defense TD》 [跳转至商店](https://play.google.com/store/apps/details?id=com.wireless.defenseland)
android KR 第9名 《Age of Apes》 [跳转至商店](https://play.google.com/store/apps/details?id=com.tap4fun.ape.gplay)
android JP 第9名 《Raid Rush: Tower Defense TD》 [跳转至商店](https://play.google.com/store/apps/details?id=com.wireless.defenseland)

付费榜：
无

畅销榜：
ios KR 第8名 《랑그릿사》 [跳转至商店](https://apps.apple.com/kr/app/%EB%9E%91%EA%B7%B8%EB%A6%BF%EC%82%AC/id1450127722?uo=4)
android KR 第10名 《Rise of Castles: Ice and Fire》 [跳转至商店](https://play.google.com/store/apps/details?id=com.im30.ROE.gp)
    '''
    retStr = toGpt(retStr)
    print(retStr)
    print(type(retStr))

    conn = rpyc.connect("192.168.40.62", 10001)
    conn.root.sendMessageDebug(retStr)


if __name__ == '__main__':
    # topWatch(isDebug=True,gpt=True)
    # topWatch(gpt=True)

    createTable()
    getDataFromSensorTower()

    


    
    
    

    

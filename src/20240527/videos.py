# 从st获取最近视频，保存到本地

# 获取主要信息包括：
# 时间段、平台、APP名称、广告网络、国家、首次看到时间与最后看到时间和占有份额。

# 目的是明确的找出那些是较好的视频广告，而哪些是较差的视频广告。
# 之后再尝试找到这些视频广告的特点，以及为什么这些视频广告会受欢迎。

import datetime
import pandas as pd

import sys
sys.path.append('/src')

from src.sensortower.intel import getTopApp,getUnifiedAppIds,getCreatives2

# 支持多个国家放在一起，暂时就按下面分组，后续可以根据需求调整
countryList2 = [
    {'name':'US','codeList':['US']},
    # {'name':'KR','codeList':['KR']},
    # {'name':'JP','codeList':['JP']},
    # {'name':'TW','codeList':['TW']}
]

def main():
    # 暂定获取最近一个月的数据
    startDay = datetime.datetime.now() - datetime.timedelta(days=30)
    endDay = datetime.datetime.now() - datetime.timedelta(days=1)
    startDayStr = startDay.strftime('%Y-%m-%d')
    endDayStr = endDay.strftime('%Y-%m-%d')
    print('查询时间段：',startDayStr,'-',endDayStr)

    countryList = []
    osList = []
    appIdList = []
    networkList = []
    firstSeenAtList = []
    lastSeenAtList = []
    # 对于一个素材多个分辨率的情况，取第一个加入结果
    creativeUrlList = []
    videoDurationList = []
    shareList = []

    # 分国家
    for country2 in countryList2:
        # 选用组合的第一个国家作为查询标准，获得APP列表
        country = ','.join(country2['codeList'])
        
        for os in ['android','ios']:
            # 分别获取android和ios的数据
            # 先找到top slg N
            N = 5
            topAppDf = getTopApp(os=os, custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='month',limit=N,category='all',countries=country,startDate=startDayStr,endDate=endDayStr)
            topAppIdList = topAppDf['appId'].tolist()
            # print(topAppIdList)

            networks = []
            if os == 'ios':
                networks = [
                    "Admob",
                    "Applovin",
                    # "Youtube"
                ]
            else:
                networks = [
                    "Admob",
                    "Applovin",
                    # "Unity"
                ]


            for appId in topAppIdList:
                # 分开媒体获取数据，是因为st的API不提供具体展示次数，只有比例。多个媒体加在一起，小媒体的数据会被大媒体淹没。
                for network in networks:
                    # for page in range(1,6):
                        # 由于API调用次数有限，直接加大limit,page=1就可以了
                        page = 1
                        limit = 100
                        creatives = getCreatives2(os=os,appIds=[appId],countries=country2['codeList'],networks=network,limit=limit,page = page,start_date=startDayStr,end_date=endDayStr)
                        adUnits = creatives['ad_units']
                        # 直接取第一个，这应该是多分辨率中随意的一个
                        if len(adUnits) <= 0:
                            continue
                        for adUnit in adUnits:
                            firstSeenAt = adUnit['first_seen_at'][:10]
                            lastSeenAt = adUnit['last_seen_at'][:10]

                            countryList.append(country2['name'])
                            osList.append(os)
                            appIdList.append(appId)
                            networkList.append(network)
                            firstSeenAtList.append(firstSeenAt)
                            lastSeenAtList.append(lastSeenAt)
                            creativeUrlList.append(adUnit['creatives'][0]['creative_url'])
                            videoDurationList.append(adUnit['creatives'][0]['video_duration'])
                            shareList.append(adUnit['share'])
                        
                        # # 数量不足，就不再获取了
                        # if len(adUnits) < limit:
                        #     break

    df = pd.DataFrame({
        'country':countryList,
        'os':osList,
        'appId':appIdList,
        'network':networkList,
        'firstSeenAt':firstSeenAtList,
        'lastSeenAt':lastSeenAtList,
        'creativeUrl':creativeUrlList,
        'videoDuration':videoDurationList,
        'share':shareList
    })
    df.to_csv('/src/data/videoTopWatch.csv',index=False)                    

if __name__ == '__main__':
    main()

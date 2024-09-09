# 视频版topWatch
import datetime

import sys
sys.path.append('/src')

from src.sensortower.intel import getTopApp,getUnifiedAppIds,getCreatives


country = 'TW'

lastMonday = datetime.datetime.now() - datetime.timedelta(days=7)
lastMonday = lastMonday - datetime.timedelta(days=lastMonday.weekday())
lastSunday = lastMonday + datetime.timedelta(days=6)
lastMondayStr = lastMonday.strftime('%Y-%m-%d')
lastSundayStr = lastSunday.strftime('%Y-%m-%d')

topAppDf = getTopApp(os='android', custom_fields_filter_id='600a22c0241bc16eb899fd71',time_range='month',limit=20,category='all',countries=country,startDate=lastMondayStr,endDate=lastSundayStr)
topAppIdList = topAppDf['appId'].tolist()

topAppUnifiedIds = getUnifiedAppIds(app_id_type='android',app_ids=topAppIdList)
print(topAppUnifiedIds)

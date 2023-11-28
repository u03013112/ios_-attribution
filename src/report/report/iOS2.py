# 针对iOS的长期报告，长期指30,60,90,120日的报告
import os
import datetime
import pandas as pd

import sys
sys.path.append('/src')

from src.report.data.ad import getAdDataIOSGroupByCampaignAndGeoAndMedia
from src.report.data.revenue import getRevenueDataIOSGroupByCampaignAndGeoAndMedia2

directory = '/src/data/'
def getFilename(filename,ext='csv'):
    return '%s/%s.%s'%(directory,filename,ext)

def main(startDayStr,endDayStr):
    print('查询日期：',startDayStr,'~',endDayStr)

    global directory
    directory = f'/src/data/report/iOS2_{startDayStr}_{endDayStr}'

    if not os.path.exists(directory):
        os.makedirs(directory)

    # adCostDf = getAdDataIOSGroupByCampaignAndGeoAndMedia(startDayStr,endDayStr,directory)
    # revenueDf = getRevenueDataIOSGroupByCampaignAndGeoAndMedia2(startDayStr,endDayStr,directory)

    # df = pd.merge(adCostDf,revenueDf,on=[
    #     'install_date','campaign_id','campaign_name','media','geoGroup'
    #     ],how='outer',suffixes=('_ad','_revenue'))

    # df = df.fillna(0)

    # df.to_csv(getFilename('merge'),index=False)

    df = pd.read_csv(getFilename('merge'))
    df = df.groupby(['install_date','media','geoGroup']).sum().reset_index()
    df.to_csv(getFilename('merge_groupby'),index=False)



if __name__ == '__main__':
    main('20230401','20230731')
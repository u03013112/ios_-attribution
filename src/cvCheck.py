# 尝试比如数数中的cv值总量与skan中的cv值总量是否有线性关系
import sys
sys.path.append('/src')

from src.smartCompute import SmartCompute

def getFilename(filename):
    return '/src/data/%s.csv'%(filename)

def getSkanDataFromSmartCompute(sinceTimeStr,unitlTimeStr,filename):
    sql='''
            select
                *
            from ods_platform_appsflyer_skad_details
            where
                app_id="id1479198816"
                and skad_conversion_value>0
                and day>=%s and day <=%s
        '''%(sinceTimeStr,unitlTimeStr)
    smartCompute = SmartCompute()
    pd_df = smartCompute.execSql(sql)
    smartCompute.writeCsv(pd_df,getFilename(filename))

import pandas as pd
from src.ss import Data
def getCVFromSS(sinceTimeStr,unitlTimeStr):
    ret = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    afCvMapDataFrame = pd.read_csv('/src/afCvMap.csv')
    data = Data(since=sinceTimeStr,until=unitlTimeStr).get24HPayUserInfoEasy()
    for usd in data:
        cv = len(afCvMapDataFrame.max_event_revenue)-1
        # 暂时不考虑开闭区间问题，卡在区间边缘的数据并不常见
        cvDataFrame = afCvMapDataFrame[(afCvMapDataFrame.min_event_revenue<=usd) & (afCvMapDataFrame.max_event_revenue>usd)]
        if len(cvDataFrame) == 1:
            # 这里索引值就是cv值
            cv = cvDataFrame.conversion_value.index[0]
        else:
            # print("付费金额%f找不到对应的cv值"%(usd))
            pass
        ret[cv] += 1
    retStr = ''
    for i in range(1,64):
        retStr += ',%d'%(ret[i])
    return retStr

def cvTotal(filename):
    df = pd.read_csv(getFilename(filename))
    retStr = filename
    for cv in range(1,64):
        count = len(df[(df.skad_conversion_value) == cv])
        # print('cv is %s total count:%s'%(cv,count))
        retStr += ',%s'%(count)
    print(retStr)

if __name__ == '__main__':
    # taskList = [
    #     # 开始日期，结束日期，文件名
    #     ['20220101','20220131','202201'],
    #     ['20220201','20220228','202202'],
    #     ['20220301','20220331','202203'],
    #     ['20220401','20220430','202204'],
    #     ['20220501','20220531','202205'],
    #     ['20220601','20220630','202206'],
    #     ['20220701','20220731','202207'],
    #     ['20220801','20220831','202208'],
    # ]
    # for i in range(len(taskList)):
    #     getSkanDataFromSmartCompute(taskList[i][0],taskList[i][1],taskList[i][2])
    #     cvTotal(taskList[i][2])

    # print('2022-01',getCVFromSS('2022-01-01','2022-01-31'))
    # print('2022-02',getCVFromSS('2022-02-01','2022-02-28'))
    # print('2022-03',getCVFromSS('2022-03-01','2022-03-31'))
    # print('2022-04',getCVFromSS('2022-04-01','2022-04-30'))
    # print('2022-05',getCVFromSS('2022-05-01','2022-05-31'))
    # print('2022-06',getCVFromSS('2022-06-01','2022-06-30'))
    print('2022-07',getCVFromSS('2022-07-01','2022-07-31'))
    # print('2022-08',getCVFromSS('2022-08-01','2022-08-31'))
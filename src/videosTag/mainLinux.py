import os
import sys
import pandas as pd
import datetime
import cv2

sys.path.append('/src')
from src.maxCompute import execSql

def getDataFromMaxCompute(installDayStartStr, installDayEndStr, earliestDayStartStr, earliestDayEndStr):
    mediasourceList = [
        # 'Applovin',
        # 'Facebook',
        'Google',
        # 'Mintegral',
        # 'Moloco',
        # 'Snapchat',
        # 'Twitter',
        # 'tiktok',
    ]
    sql = f'''
select
    material_name,
    video_url,
    earliest_day,
    sum(cost_value_usd) as cost
from rg_bi.dws_material_overseas_data_public
where
    app = '502'
    and install_day between {installDayStartStr} and {installDayEndStr}
    and earliest_day between {earliestDayStartStr} and {earliestDayEndStr}
    and mediasource in ({','.join(f"'{source}'" for source in mediasourceList)})
    and country in ('US')
group by
    material_name,
    video_url,
    earliest_day
order by
    cost desc
;
    '''
    print(sql)
    data = execSql(sql)
    return data


# 从数据库获取数据
def trainDataPrepare1():
    df = pd.DataFrame()

    startDate = datetime.datetime(2025, 1, 6)
    endDate = datetime.datetime(2025, 4, 7)
    mondayList = []

    # 计算每周一的日期
    currentDate = startDate
    while currentDate <= endDate:
        if currentDate.weekday() == 0:  # 0表示周一
            mondayList.append(currentDate.strftime('%Y%m%d'))
        currentDate += datetime.timedelta(days=1)

    for monday in mondayList:
        sunday = (datetime.datetime.strptime(monday, '%Y%m%d') + datetime.timedelta(days=6)).strftime('%Y%m%d')
        lastMonday = (datetime.datetime.strptime(monday, '%Y%m%d') - datetime.timedelta(days=7)).strftime('%Y%m%d')
        lastSunday = (datetime.datetime.strptime(monday, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
        # print('当前周一：', monday)
        # print('当前周日：', sunday)
        # print('上周一：', lastMonday)
        # print('上周日：', lastSunday)

        weekDf = getDataFromMaxCompute(monday, sunday, lastMonday, lastSunday)
        print(weekDf)

        df = pd.concat([df, weekDf], axis=0)

    # 重置索引
    df.reset_index(drop=True, inplace=True)

    return df


def trainDataPrepare():
    trainDataFilename1 = '/src/data/videosTag_train1.csv'
    if os.path.exists(trainDataFilename1):
        trainDf1 = pd.read_csv(trainDataFilename1)
    else:
        trainDf1 = trainDataPrepare1()
        trainDf1.to_csv(trainDataFilename1, index=False)

    print('trainDf1:')
    print(trainDf1)



if __name__ == '__main__':
    trainDataPrepare()
    



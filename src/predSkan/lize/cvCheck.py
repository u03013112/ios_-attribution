import pandas as pd
import os
import sys

from sklearn.metrics import r2_score,mean_absolute_percentage_error

sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename


def getDataFromMC():
    sql = '''
        select
            customer_user_id,
            to_char(
                to_date(install_time, "yyyy-mm-dd hh:mi:ss"),
                "yyyy-mm-dd"
            ) as install_date,
            sum(
                case
                    when event_timestamp - install_timestamp <= 1 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r1usd,
            sum(
                case
                    when event_timestamp - install_timestamp <= 7 * 24 * 3600 then cast (event_revenue_usd as double)
                    else 0
                end
            ) as r7usd,
            media_source as media
        from
            ods_platform_appsflyer_events
        where
            app_id = 'id1479198816'
            and event_name = 'af_purchase'
            and zone = 0
            and day >= 20220701
            and day <= 20230201
        group by
            install_date,
            customer_user_id,
            media_source
    '''

    df = execSql(sql)
    return df

def step1():
    df = getDataFromMC()
    df.to_csv(getFilename('iosCvR1R7Media_20220701_20230201'))

def addMediaGroup(df):
    # Initialize the media_group column with default value 'unknown'
    df.loc[:,'media_group'] = 'unknown'

    # Iterate through the mediaList and update the media_group column accordingly
    for group in mediaList:
        df.loc[df['media'].isin(group['codeList']), 'media_group'] = group['name']
    return df

mediaList = [
    {'name':'bytedance','codeList':['bytedanceglobal_int'],'sname':'Bd'},
    {'name':'facebook','codeList':['Social_facebook','restricted','Facebook Ads','facebook','FacebookAds'],'sname':'Fb'},
    {'name':'google','codeList':['googleadwords_int'],'sname':'Gg'},
    {'name':'unknown','codeList':[],'sname':'Og'}
]

def checkCvMap(userDf,cvMapDf,usd = 'r1usd'):
    import copy
    df = copy.deepcopy(userDf)
    df.loc[:,'cv'] = 0
    for i in range(len(cvMapDf)):
        min_event_revenue = cvMapDf.min_event_revenue[i]
        max_event_revenue = cvMapDf.max_event_revenue[i]
        if pd.isna(max_event_revenue):
            continue
        df.loc[
            (df[usd] > min_event_revenue) & (df[usd] <= max_event_revenue),
            'cv'
        ] = i
    df.loc[
        (df[usd] > max_event_revenue),
        'cv'
    ] = len(cvMapDf)-1

    df.loc[:,'cv_usd'] = 0
    for i in range(len(cvMapDf)):
        min_event_revenue = cvMapDf.min_event_revenue[i]
        max_event_revenue = cvMapDf.max_event_revenue[i]
        avg = (min_event_revenue + max_event_revenue)/2
        if pd.isna(max_event_revenue):
            avg = 0
        if avg < 0:
            avg = 0
        df.loc[df.cv == i,'cv_usd'] = avg
    
    # print(df)
    mergeDf = df.groupby('install_date',as_index=False).agg({usd:'sum','cv_usd':'sum'})
    print(mergeDf)
    # 计算mergeDf中usd列与'cv_usd'列的mape 和 r2_score
    from sklearn.metrics import mean_absolute_percentage_error, r2_score

    mape = mean_absolute_percentage_error(mergeDf[usd], mergeDf['cv_usd'])
    r2 = r2_score(mergeDf[usd], mergeDf['cv_usd'])

    print(f"MAPE: {mape}")
    print(f"R2 Score: {r2}")
    return mape,r2



def cvCheck(cvMapDf):
    df = pd.read_csv(getFilename('iosCvR1R7Media_20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    df = addMediaGroup(df)

    lines = []
    head = ['Media','MAPE','R2']
    lines.append(head)
    
    line = ['total']
    
    mape,r2 = checkCvMap(df,cvMapDf,usd = 'r1usd')
    line += [mape,r2]
    lines.append(line)

    for media in mediaList:
        mediaName = media['name']
        line = [mediaName]
        mediaDf = df[df.media_group == mediaName]
        mape,r2 = checkCvMap(mediaDf,cvMapDf,usd = 'r1usd')
        line += [mape,r2]
        lines.append(line)

    return lines

from src.googleSheet import GSheet
if __name__ == '__main__':
    # step1()
    
    cvMapDf = pd.read_csv('/src/afCvMap2303.csv')
    lines = cvCheck(cvMapDf)
    for line in lines:
        print(line)
    GSheet().clearSheet('1111','Sheet1')
    GSheet().updateSheet('1111','Sheet1','A1',lines)
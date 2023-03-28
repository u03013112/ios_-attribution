import pandas as pd
import os
import sys

from sklearn.metrics import r2_score,mean_absolute_percentage_error

sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

from src.predSkan.lize.userGroupByR2 import mediaList,addMediaGroup,addCV,makeCvMap,makeLevels1
# 主要改变：首日付费金额还是按照就有方案进行分组，7日付费金额分组按照首日付费金额分组过滤后重新分组

def score1Print(message,df):
    y1 = df['r1usd']
    y1p = df['%sp'%'r1usd']

    y7 = df['r7usd']
    y7p = df['%sp'%'r7usd']
    try:
        mape1 = mean_absolute_percentage_error(y1,y1p)
        r2Score1 = r2_score(y1,y1p)

        mape7 = mean_absolute_percentage_error(y7,y7p)
        r2Score7 = r2_score(y7,y7p)
    except:
        mape1 = 0
        r2Score1 = 0

        mape7 = 0
        r2Score7 = 0

    return mape1,r2Score1,mape7,r2Score7

def main():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    df.loc[:,'count'] = 1


    head = ['r1usd 分为N组','r7usd 分为M组','共计NxM组',
            # '用户为单位计算MAPE 1','用户为单位计算R2 1','用户为单位计算MAPE 7','用户为单位计算R2 7',
            '按天汇总计算MAPE 1','按天汇总计算R2 1','按天汇总计算MAPE 7','按天汇总计算R2 7',
            'bytedance MAPE 1','bytedance R2 1','bytedance MAPE 7','bytedance R2 7',
            'facebook MAPE 1','facebook R2 1','facebook MAPE 7','facebook R2 7',
            'google MAPE 1','google R2 1','google MAPE 7','google R2 7',
            'unknown MAPE 1','unknown R2 1','unknown MAPE 7','unknown R2 7']

    lines = [head]
    for n1,n7 in (
        (8,8),(8,10),(8,12),(8,14),(8,16),
        (10,8),(10,10),(10,12),(10,14),(10,16),
        ):
        line = [n1,n7,n1*n7]
        levels1 = makeLevels1(df,usd = 'r1usd',N=n1)
        cvMapDf1 = makeCvMap(levels1)
        # print(levels1)
        df = addCV(df,cvMapDf1,usd = 'r1usd',cvName = 'cv1')

        print(cvMapDf1)
        cv1List = list(cvMapDf1['cv'].unique())
        cv1List.sort()
        print(cv1List)
        for cv1 in cv1List:
            # 给用户的首日付费金额p赋值
            min = cvMapDf1.loc[cvMapDf1.cv == cv1]['min_event_revenue'].values[0]
            max = cvMapDf1.loc[cvMapDf1.cv == cv1]['max_event_revenue'].values[0]
            avg = (min + max)/2
            if avg < 0:
                avg = 0
            df.loc[
                (df.cv1 == cv1),
                'r1usdp'
            ] = avg

            # 根据首日分组进行7日分组
            cvDf = df.loc[
                df.cv1 == cv1
            ]
            levels7 = makeLevels1(cvDf,usd = 'r7usd',N=n7)
            r7usdMin = cvDf['r7usd'].min()
            # print(r7usdMin)
            cvMapDf7 = makeCvMap(levels7,min = r7usdMin)
            print('cv1:%d'%(cv1),'\n',cvMapDf7)
            
            df.loc[df.cv1 == cv1,'cv7'] = 0
            cv7List = list(cvMapDf7['cv'].unique())
            cv7List.sort()
            for cv7 in cv7List:
                min_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['min_event_revenue'].values[0]
                max_event_revenue = cvMapDf7.loc[cvMapDf7.cv == cv7]['max_event_revenue'].values[0]
                if pd.isna(max_event_revenue):
                    continue
                avg = (min_event_revenue + max_event_revenue)/2
                if avg < 0:
                    avg = 0

                df.loc[
                    (df.cv1 == cv1) &
                    (df['r7usd'] > min_event_revenue) & (df['r7usd'] <= max_event_revenue),
                    'cv7'
                ] = cv7
                df.loc[
                    (df.cv1 == cv1) &
                    (df['r7usd'] > min_event_revenue) & (df['r7usd'] <= max_event_revenue),
                    'r7usdp'
                ] = avg
            df.loc[
                (df.cv1 == cv1) &
                (df['r7usd'] > max_event_revenue),
                'cv7'
            ] = len(cvMapDf7)-1
            df.loc[
                (df.cv1 == cv1) &
                (df['r7usd'] > max_event_revenue),
                'r7usdp'
            ] = avg
            
        # df['cv7']转成int
        df['cv7'] = df['cv7'].astype(int)

        # df.to_csv(getFilename('userGroupByR3'),index=False)
        # return
        # mape1,r2Score1,mape7,r2Score7 = score1Print('用户为单位计算',df)
        # line += [mape1,r2Score1,mape7,r2Score7]

        groupByDayDf = df.groupby(by = ['install_date'],as_index=False).agg({
            'count':'sum',
            'r1usd':'sum',
            'r7usd':'sum',
            'r1usdp':'sum',
            'r7usdp':'sum'
        })
        print(groupByDayDf)
        mape1,r2Score1,mape7,r2Score7 = score1Print('按天汇总计算',groupByDayDf)
        print('MAPE1:',mape1)
        line += [mape1,r2Score1,mape7,r2Score7]
        # 分媒体
        mediaDf = addMediaGroup(df)
        for media in mediaList:
            mediaName = media['name']
            mediaDf0 = mediaDf.loc[mediaDf.media_group == mediaName]
            mediaDf1 = mediaDf0.groupby(by = ['install_date'],as_index=False).agg({
                'count':'sum',
                'r1usd':'sum',
                'r7usd':'sum',
                'r1usdp':'sum',
                'r7usdp':'sum'
            })
            mape1,r2Score1,mape7,r2Score7 = score1Print('%s 按天汇总计算'%(mediaName),mediaDf1)
            line += [mape1,r2Score1,mape7,r2Score7]

        lines.append(line)

    return lines

from src.googleSheet import GSheet
if __name__ == '__main__':
    lines = main()

    GSheet().clearSheet('1111','Sheet4')
    GSheet().updateSheet('1111','Sheet4','A1',lines)




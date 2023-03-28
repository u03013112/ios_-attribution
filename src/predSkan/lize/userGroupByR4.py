import pandas as pd
import os
import sys

from sklearn.metrics import r2_score,mean_absolute_percentage_error

sys.path.append('/src')
from src.maxCompute import execSql
from src.tools import getFilename

from src.predSkan.lize.userGroupByR2 import mediaList,addMediaGroup,addCV,makeCvMap,makeLevels1
# 拆出第3版中的8x8的案例，详细看看里面的结论，然后考虑是否可以进行大R的削减得到更好的结果

def main():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    
    n1 = 8
    n7 = 8

    levels1 = makeLevels1(df,usd = 'r1usd',N=n1)
    cvMapDf1 = makeCvMap(levels1)
    cvMapDf1.to_csv(getFilename('cvMap1'))
    df = addCV(df,cvMapDf1,usd = 'r1usd',cvName = 'cv1')
    
    cv1List = list(cvMapDf1['cv'].unique())
    cv1List.sort()
    
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
        # 这里额外减一个极小值，是为了去掉cv7=0的情况
        if r7usdMin > 0:
            r7usdMin -= 1e-6

        # print(r7usdMin)
        cvMapDf7 = makeCvMap(levels7,min = r7usdMin)
        print('cv1:%d'%(cv1),'\n',cvMapDf7)
        cvMapDf7.to_csv(getFilename('cvMap1_%d'%cv1))
        
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

    df = addMediaGroup(df)

    df.to_csv(getFilename('groupByDayDf0'),index=False)

    groupByDayDf = df.groupby(by = ['install_date','media_group','cv1','cv7'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum',
        'r1usdp':'sum',
        'r7usdp':'sum'
    })
    
    retDf = groupByDayDf.to_csv(getFilename('groupByDayDf'),index=False)

    return retDf

# 计算cv1的分别MAPE
def cv1Mape():
    df = pd.read_csv(getFilename('groupByDayDf'))
    cv1List = list(df['cv1'].unique())
    cv1List.sort()
    # 大盘
    dfGroupByCv1 = df.groupby(by = ['install_date','cv1'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum',
        'r1usdp':'sum',
        'r7usdp':'sum'
    })
    for cv1 in cv1List:
        cvDf = dfGroupByCv1.loc[dfGroupByCv1.cv1 == cv1]

        y1 = cvDf['r1usd']
        y1p = cvDf['%sp'%'r1usd']

        y7 = cvDf['r7usd']
        y7p = cvDf['%sp'%'r7usd']
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

        print('大盘 cv1=%d mape1:%.2f%%,r2Score1:%.3f,mape7:%.2f%%,r2Score7:%3f'%(cv1,mape1*100,r2Score1,mape7*100,r2Score7))

    # 分媒体
    dfGroupByCv1 = df.groupby(by = ['install_date','media_group','cv1'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum',
        'r1usdp':'sum',
        'r7usdp':'sum'
    })
    

    for media in mediaList:
        mediaName = media['name']
        mediaDf = dfGroupByCv1.loc[dfGroupByCv1.media_group == mediaName]

        for cv1 in cv1List:
            cvDf = mediaDf.loc[mediaDf.cv1 == cv1]

            y1 = cvDf['r1usd']
            y1p = cvDf['%sp'%'r1usd']

            y7 = cvDf['r7usd']
            y7p = cvDf['%sp'%'r7usd']
            try:
                mape1 = mean_absolute_percentage_error(y1,y1p)
                r2Score1 = r2_score(y1,y1p)

                mape7 = mean_absolute_percentage_error(y7,y7p)
                r2Score7 = r2_score(y7,y7p)
            except:
                mape1 = 0
                r2Score1 = 0

            print('%s cv1=%d mape1:%.2f%%,r2Score1:%.3f,mape7:%.2f%%,r2Score7:%3f'%(mediaName,cv1,mape1*100,r2Score1,mape7*100,r2Score7))

def checkCv1Eq1():
    # df = pd.read_csv(getFilename('groupByDayDf0'))
    # df1 = df.loc[df.cv1 == 1]

    # df1.to_csv(getFilename('cv1Eq1Df'),index=False)

    df1 = pd.read_csv(getFilename('cv1Eq1Df'))

    # 尝试修改平均值，降低MAPE
    df1.loc[:,'r1usdp'] = df1['r1usd'].mean()

    print(df1['r1usd'].mean())
    print(df1['r1usd'].median())

    mape1 = mean_absolute_percentage_error(df1['r1usd'],df1['r1usdp'])
    print('按人 mape:%.2f%%'%(mape1*100))

    df1GroupByDay = df1.groupby(by = ['install_date'],as_index=False).agg({
        'r1usd':'sum',
        'r1usdp':'sum'
    })
    mape1 = mean_absolute_percentage_error(df1GroupByDay['r1usd'],df1GroupByDay['r1usdp'])
    print('按天 mape:%.2f%%'%(mape1*100))

def checkCv1Eq7():
    # df = pd.read_csv(getFilename('groupByDayDf0'))
    # df1 = df.loc[df.cv1 == 7]

    # df1.to_csv(getFilename('cv1Eq7Df'),index=False)

    df1 = pd.read_csv(getFilename('cv1Eq7Df'))

    # 尝试修改平均值，降低MAPE
    # df1.loc[:,'r1usdp'] = df1['r1usd'].mean()

    print(df1['r1usd'].mean())
    print(df1['r1usdp'].mean())

    mape1 = mean_absolute_percentage_error(df1['r1usd'],df1['r1usdp'])
    print('按人 mape:%.2f%%'%(mape1*100))

    df1GroupByDay = df1.groupby(by = ['install_date'],as_index=False).agg({
        'r1usd':'sum',
        'r1usdp':'sum'
    })
    mape1 = mean_absolute_percentage_error(df1GroupByDay['r1usd'],df1GroupByDay['r1usdp'])
    print('按天 mape:%.2f%%'%(mape1*100))

def cv7Mape():
    df = pd.read_csv(getFilename('groupByDayDf'))
    
    cv1List = list(df['cv7'].unique())
    cv1List.sort()
    # 大盘
    dfGroupByCv1 = df.groupby(by = ['install_date','cv7'],as_index=False).agg({
        'r7usd':'sum',
        'r7usdp':'sum'
    })
    for cv1 in cv1List:
        cvDf = dfGroupByCv1.loc[dfGroupByCv1.cv7 == cv1]

        y7 = cvDf['r7usd']
        y7p = cvDf['%sp'%'r7usd']
        try:
            mape7 = mean_absolute_percentage_error(y7,y7p)
            r2Score7 = r2_score(y7,y7p)
        except:
            mape7 = 0
            r2Score7 = 0

        print('大盘 cv1=%d mape7:%.2f%%,r2Score7:%3f'%(cv1,mape7*100,r2Score7))

    # 分媒体
    dfGroupByCv1 = df.groupby(by = ['install_date','media_group','cv7'],as_index=False).agg({
        'r7usd':'sum',
        'r7usdp':'sum'
    })
    
    for media in mediaList:
        mediaName = media['name']
        mediaDf = dfGroupByCv1.loc[dfGroupByCv1.media_group == mediaName]

        for cv1 in cv1List:
            cvDf = mediaDf.loc[mediaDf.cv7 == cv1]

            y7 = cvDf['r7usd']
            y7p = cvDf['%sp'%'r7usd']
            try:
                mape7 = mean_absolute_percentage_error(y7,y7p)
                r2Score7 = r2_score(y7,y7p)
            except:
                mape7 = 0
                r2Score7 = 0

            print('%s cv1=%d mape7:%.2f%%,r2Score7:%3f'%(mediaName,cv1,mape7*100,r2Score7))

def checkCv7Eq1():
    df = pd.read_csv(getFilename('groupByDayDf0'))
    df1 = df.loc[df.cv7 == 1]

    df1.to_csv(getFilename('cv7Eq1Df'),index=False)

    df1 = pd.read_csv(getFilename('cv7Eq1Df'))

    # 尝试修改平均值，降低MAPE
    # df1.loc[:,'r7usdp'] = df1['r7usd'].mean()

    print(df1['r7usd'].mean())
    print(df1['r7usd'].median())

    mape1 = mean_absolute_percentage_error(df1['r7usd'],df1['r7usdp'])
    print('按人 mape:%.2f%%'%(mape1*100))

    df1GroupByDay = df1.groupby(by = ['install_date'],as_index=False).agg({
        'r7usd':'sum',
        'r7usdp':'sum'
    })
    mape1 = mean_absolute_percentage_error(df1GroupByDay['r7usd'],df1GroupByDay['r7usdp'])
    print('按天 mape:%.2f%%'%(mape1*100))

def cv1AndCv7Mape():
    df = pd.read_csv(getFilename('groupByDayDf'))
    cv1List = list(df['cv1'].unique())
    cv1List.sort()
    cv7List = list(df['cv7'].unique())
    cv7List.sort()
    # 大盘
    dfGroupByCv = df.groupby(by = ['install_date','cv1','cv7'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum',
        'r1usdp':'sum',
        'r7usdp':'sum'
    })
    for cv1 in cv1List:
        for cv7 in cv7List:
            cvDf = dfGroupByCv.loc[(dfGroupByCv.cv1 == cv1) & (dfGroupByCv.cv7 == cv7)]

            y1 = cvDf['r1usd']
            y1p = cvDf['%sp'%'r1usd']

            y7 = cvDf['r7usd']
            y7p = cvDf['%sp'%'r7usd']
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

            print('大盘 cv1=%d cv7=%d mape1:%.2f%%,r2Score1:%.3f,mape7:%.2f%%,r2Score7:%3f'%(cv1,cv7,mape1*100,r2Score1,mape7*100,r2Score7))

    # 分媒体
    dfGroupByCv = df.groupby(by = ['install_date','media_group','cv1','cv7'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum',
        'r1usdp':'sum',
        'r7usdp':'sum'
    })
    

    for media in mediaList:
        mediaName = media['name']
        mediaDf = dfGroupByCv.loc[dfGroupByCv.media_group == mediaName]

        for cv1 in cv1List:
            for cv7 in cv7List:
                cvDf = mediaDf.loc[(mediaDf.cv1 == cv1) & (mediaDf.cv7 == cv7)]

                y1 = cvDf['r1usd']
                y1p = cvDf['%sp'%'r1usd']

                y7 = cvDf['r7usd']
                y7p = cvDf['%sp'%'r7usd']
                try:
                    mape1 = mean_absolute_percentage_error(y1,y1p)
                    r2Score1 = r2_score(y1,y1p)

                    mape7 = mean_absolute_percentage_error(y7,y7p)
                    r2Score7 = r2_score(y7,y7p)
                except:
                    mape1 = 0
                    r2Score1 = 0

                print('%s cv1=%d cv7=%d mape1:%.2f%%,r2Score1:%.3f,mape7:%.2f%%,r2Score7:%3f'%(mediaName,cv1,cv7,mape1*100,r2Score1,mape7*100,r2Score7))


def totalMape():
    df = pd.read_csv(getFilename('groupByDayDf'))
    # 大盘
    dfGroupByDay = df.groupby(by = ['install_date'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum',
        'r1usdp':'sum',
        'r7usdp':'sum'
    })
    y1 = dfGroupByDay['r1usd']
    y1p = dfGroupByDay['%sp'%'r1usd']

    y7 = dfGroupByDay['r7usd']
    y7p = dfGroupByDay['%sp'%'r7usd']
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

    print('大盘 mape1:%.2f%%,r2Score1:%.3f,mape7:%.2f%%,r2Score7:%3f'%(mape1*100,r2Score1,mape7*100,r2Score7))

    # 分媒体
    dfGroupByDay = df.groupby(by = ['install_date','media_group'],as_index=False).agg({
        'r1usd':'sum',
        'r7usd':'sum',
        'r1usdp':'sum',
        'r7usdp':'sum'
    })
    for media in mediaList:
        mediaName = media['name']
        mediaDf = dfGroupByDay.loc[dfGroupByDay.media_group == mediaName]

        y1 = mediaDf['r1usd']
        y1p = mediaDf['%sp'%'r1usd']

        y7 = mediaDf['r7usd']
        y7p = mediaDf['%sp'%'r7usd']
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

        print('%s mape1:%.2f%%,r2Score1:%.3f,mape7:%.2f%%,r2Score7:%3f'%(mediaName,mape1*100,r2Score1,mape7*100,r2Score7))


# 临时任务，用安卓数据算一版32位的档位
def tmp():
    df = pd.read_csv(getFilename('aosCvR1R7Media_20220701_20230201'))
    df = df.loc[df.install_date >= '2022-07-01']
    
    n1 = 32

    levels1 = makeLevels1(df,usd = 'r1usd',N=n1)
    cvMapDf1 = makeCvMap(levels1)
    cvMapDf1.to_csv(getFilename('cvMapAndroid32'))
    


from src.googleSheet import GSheet
if __name__ == '__main__':
    # main()

    # cv1Mape()
    # 通过上面得到结论，cv==1和cv==7的MAPE偏高，需要逐一排查
    # 首先是cv==1的人

    # checkCv1Eq1()
    # 发现用区间的平均值效果不如用区间内所有真实值的平均值进行填充效果好，这个结论有待进一步验证，但是可以先记下来

    # checkCv1Eq7()
    # 大R部分并不适用于cv==1的方案，可能需要进行大R削减方案

    # cv7Mape()

    # checkCv7Eq1()
    # 这个部分发现cv==1的偏差也比较大，但是鉴于cv7是根据cv1分组计算的，所以这个优化会比较麻烦，暂时先记录

    # cv1AndCv7Mape()

    totalMape()




    

    




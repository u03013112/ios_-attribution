import pandas as pd



def tw():
    df = pd.read_csv('twAfEventTable.csv')
    df['count'] = 1
    # 拥有列Attributed Touch Type,Attributed Touch Time,Event Time,Event Name,Media Source,Contributor 1 Touch Type,Contributor 1 Touch Time,Contributor 1 Media Source,Contributor 2 Touch Type,Contributor 2 Touch Time,Contributor 2 Media Source,Contributor 3 Touch Type,Contributor 3 Touch Time,Contributor 3 Media Source,Google Play Click Time,Google Play Install Begin Time
    print('共计行数：',df['count'].sum())

    
    # 找到所有‘Attributed Touch Time’ 晚于 ‘Event Time’的行
    # df1 = df[df['Attributed Touch Time'] > df['Event Time']]
    # print(df1)

    # 找到所有‘Attributed Touch Time’ 晚于 ‘Google Play Install Begin Time’的行
    df2 = df[df['Attributed Touch Time'] > df['Google Play Install Begin Time']]
    print('‘Attributed Touch Time’ 晚于 ‘Google Play Install Begin Time’的行数：',df2['count'].sum())
    df2GroupByMediaSource = df2.groupby('Media Source').agg({'count':'sum'}).reset_index()
    print('‘Attributed Touch Time’ 晚于 ‘Google Play Install Begin Time’的行数，按Media Source分组：')
    print(df2GroupByMediaSource)

    # # 找到所有‘Attributed Touch Time’ 晚于 ‘Google Play Click Time’的行
    # df3 = df[df['Attributed Touch Time'] > df['Google Play Click Time']]
    # print('‘Attributed Touch Time’ 晚于 ‘Google Play Click Time’的行数：',df3['count'].sum())
    # df3GroupByMediaSource = df3.groupby('Media Source').agg({'count':'sum'}).reset_index()
    # print('‘Attributed Touch Time’ 晚于 ‘Google Play Click Time’的行数，按Media Source分组：')
    # print(df3GroupByMediaSource)

    # 找到df2中‘Contributor 1 Media Source’不为空的行
    df2_1 = df2[df2['Contributor 1 Media Source'].notnull()]
    print('df2中‘Contributor 1 Media Source’不为空的行数：',df2_1['count'].sum())
    df2_1GroupByMediaSource = df2_1.groupby(['Media Source']).agg({'count':'sum'}).reset_index()
    print('df2中‘Contributor 1 Media Source’不为空的行数，按Media Source分组：')
    print(df2_1GroupByMediaSource)

    df2_1GroupByMediaSource = df2_1.groupby(['Contributor 1 Media Source']).agg({'count':'sum'}).reset_index()
    print('df2中‘Contributor 1 Media Source’不为空的行数，按Contributor 1 Media Source分组：')
    print(df2_1GroupByMediaSource)

    # df2_1GroupByMediaSource = df2_1.groupby(['Media Source','Contributor 1 Media Source']).agg({'count':'sum'}).reset_index()
    # print('df2中‘Contributor 1 Media Source’不为空的行数，按Media Source和Contributor 1 Media Source分组：')
    # print(df2_1GroupByMediaSource)

    # # 找到df3中‘Contributor 1 Media Source’不为空的行
    # df3_1 = df3[df3['Contributor 1 Media Source'].notnull()]
    # print('df3中‘Contributor 1 Media Source’不为空的行数：',df3_1['count'].sum())
    # df3_1GroupByMediaSource = df3_1.groupby(['Media Source','Contributor 1 Media Source']).agg({'count':'sum'}).reset_index()
    # print('df3中‘Contributor 1 Media Source’不为空的行数，按Media Source和Contributor 1 Media Source分组：')
    # print(df3_1GroupByMediaSource)


def twCampaign():
    df = pd.read_csv('twAfEventTableCampaign.csv')
    df['count'] = 1

    # 主要调查applovin
    applovinDf = df.loc[df['Media Source'] == 'applovin_int']

    applovinDf2 = applovinDf[applovinDf['Contributor 1 Media Source'] == 'applovin_int']

    applovinDf2 = applovinDf2.groupby(['Media Source','Campaign','Contributor 1 Media Source','Contributor 1 Campaign']).agg({'count':'sum'}).reset_index()

    print(applovinDf2)


def lw():
    df = pd.read_csv('lwAfEventTable.csv')
    df['count'] = 1
    print('共计行数：',df['count'].sum())

    # 找到所有‘Attributed Touch Time’ 晚于 ‘Google Play Install Begin Time’的行
    df2 = df[df['Attributed Touch Time'] > df['Google Play Install Begin Time']]
    print('‘Attributed Touch Time’ 晚于 ‘Google Play Install Begin Time’的行数：',df2['count'].sum())
    df2GroupByMediaSource = df2.groupby('Media Source').agg({'count':'sum'}).reset_index()
    print('‘Attributed Touch Time’ 晚于 ‘Google Play Install Begin Time’的行数，按Media Source分组：')
    print(df2GroupByMediaSource)

    # 找到df2中‘Contributor 1 Media Source’不为空的行
    df2_1 = df2[df2['Contributor 1 Media Source'].notnull()]
    print('df2中‘Contributor 1 Media Source’不为空的行数：',df2_1['count'].sum())
    df2_1GroupByMediaSource = df2_1.groupby(['Media Source']).agg({'count':'sum'}).reset_index()
    print('df2中‘Contributor 1 Media Source’不为空的行数，按Media Source分组：')
    print(df2_1GroupByMediaSource)

    df2_1GroupByMediaSource = df2_1.groupby(['Contributor 1 Media Source']).agg({'count':'sum'}).reset_index()
    print('df2中‘Contributor 1 Media Source’不为空的行数，按Contributor 1 Media Source分组：')
    print(df2_1GroupByMediaSource)

    df2_1GroupByMediaSource = df2_1.groupby(['Media Source','Contributor 1 Media Source']).agg({'count':'sum'}).reset_index()
    print('df2中‘Contributor 1 Media Source’不为空的行数，按Media Source和Contributor 1 Media Source分组：')
    print(df2_1GroupByMediaSource)

def lwCampaign():
    df = pd.read_csv('lwAfEventTable.csv')
    df['count'] = 1

    # 主要调查applovin
    applovinDf = df.loc[df['Media Source'] == 'applovin_int']

    applovinDf2 = applovinDf[applovinDf['Contributor 1 Media Source'] == 'applovin_int']

    applovinDf2 = applovinDf2.groupby(['Media Source','Campaign','Contributor 1 Media Source','Contributor 1 Campaign']).agg({'count':'sum'}).reset_index()

    print(applovinDf2)


def lwReAtt():
    df = pd.read_csv('lwReAtt.csv',dtype={
        'install_day':str
    })

    # 将字符串类型转为浮点
    df['google cost'] = df['google cost'].map(lambda x: x.replace(',',''))
    df['applovin cost'] = df['applovin cost'].map(lambda x: x.replace(',',''))
    df['facebook cost'] = df['facebook cost'].map(lambda x: x.replace(',',''))
    df['google cost'] = df['google cost'].map(lambda x: float(x))
    df['applovin cost'] = df['applovin cost'].map(lambda x: float(x))
    df['facebook cost'] = df['facebook cost'].map(lambda x: float(x))

    # 将类似 1.23% 转为 0.0123
    df['google 1日ROI'] = df['google 1日ROI'].map(lambda x: float(x.replace('%',''))/100)
    df['google 重归因1日ROI'] = df['google 重归因1日ROI'].map(lambda x: float(x.replace('%',''))/100)
    df['applovin 1日ROI'] = df['applovin 1日ROI'].map(lambda x: float(x.replace('%',''))/100)
    df['applovin 重归因1日ROI'] = df['applovin 重归因1日ROI'].map(lambda x: float(x.replace('%',''))/100)
    df['facebook 1日ROI'] = df['facebook 1日ROI'].map(lambda x: float(x.replace('%',''))/100)
    df['Facebook 重归因1日ROI'] = df['Facebook 重归因1日ROI'].map(lambda x: float(x.replace('%',''))/100)

    # 拥有列安装日期,google cost,google 重归因1日ROI,google 1日ROI,applovin cost,applovin 重归因1日ROI,applovin 1日ROI,facebook cost,Facebook 重归因1日ROI,facebook 1日ROI
    
    df['google revenue'] = df['google cost'] * df['google 1日ROI']
    df['google revenue2'] = df['google cost'] * df['google 重归因1日ROI']
    df['google revenue diff'] = df['google revenue'] - df['google revenue2']

    df['applovin revenue'] = df['applovin cost'] * df['applovin 1日ROI']
    df['applovin revenue2'] = df['applovin cost'] * df['applovin 重归因1日ROI']
    df['applovin revenue diff'] = df['applovin revenue'] - df['applovin revenue2']

    df['facebook revenue'] = df['facebook cost'] * df['facebook 1日ROI']
    df['facebook revenue2'] = df['facebook cost'] * df['Facebook 重归因1日ROI']
    df['facebook revenue diff'] = df['facebook revenue'] - df['facebook revenue2']

    df = df[['google cost','google revenue','google revenue2','google revenue diff',
             'applovin cost','applovin revenue','applovin revenue2','applovin revenue diff',
             'facebook cost','facebook revenue','facebook revenue2','facebook revenue diff']]
    
    df.rename(columns={
        'google revenue':'google 正常归因1日回收',
        'google revenue2':'google 重归因1日回收',
        'google revenue diff':'google 1日回收差值',
        'applovin revenue':'applovin 正常归因1日回收',
        'applovin revenue2':'applovin 重归因1日回收',
        'applovin revenue diff':'applovin 1日回收差值',
        'facebook revenue':'facebook 正常归因1日回收',
        'facebook revenue2':'facebook 重归因1日回收',
        'facebook revenue diff':'facebook 1日回收差值'
    },inplace=True)

    corr = df.corr()
    print(corr)

    corr.to_csv('/src/data/lwReAttCorr.csv',index=True)
    
    
    

if __name__ == '__main__':
    # tw()
    # lw()
    # twCampaign()
    # lwCampaign()
    lwReAtt()

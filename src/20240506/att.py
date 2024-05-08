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

    # 找到所有‘Attributed Touch Time’ 晚于 ‘Google Play Click Time’的行
    df3 = df[df['Attributed Touch Time'] > df['Google Play Click Time']]
    print('‘Attributed Touch Time’ 晚于 ‘Google Play Click Time’的行数：',df3['count'].sum())
    df3GroupByMediaSource = df3.groupby('Media Source').agg({'count':'sum'}).reset_index()
    print('‘Attributed Touch Time’ 晚于 ‘Google Play Click Time’的行数，按Media Source分组：')
    print(df3GroupByMediaSource)

    # 找到df2中‘Contributor 1 Media Source’不为空的行
    df2_1 = df2[df2['Contributor 1 Media Source'].notnull()]
    print('df2中‘Contributor 1 Media Source’不为空的行数：',df2_1['count'].sum())
    df2_1GroupByMediaSource = df2_1.groupby(['Media Source','Contributor 1 Media Source']).agg({'count':'sum'}).reset_index()
    print('df2中‘Contributor 1 Media Source’不为空的行数，按Media Source和Contributor 1 Media Source分组：')
    print(df2_1GroupByMediaSource)

    # 找到df3中‘Contributor 1 Media Source’不为空的行
    df3_1 = df3[df3['Contributor 1 Media Source'].notnull()]
    print('df3中‘Contributor 1 Media Source’不为空的行数：',df3_1['count'].sum())
    df3_1GroupByMediaSource = df3_1.groupby(['Media Source','Contributor 1 Media Source']).agg({'count':'sum'}).reset_index()
    print('df3中‘Contributor 1 Media Source’不为空的行数，按Media Source和Contributor 1 Media Source分组：')
    print(df3_1GroupByMediaSource)


if __name__ == '__main__':
    tw()

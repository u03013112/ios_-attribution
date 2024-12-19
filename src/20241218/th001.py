import pandas as pd

def corr(df):
    # print(df.columns)

    df['installDay'] = pd.to_datetime(df['安装日期'], format='%Y%m%d')
    df = df.sort_values('installDay', ascending=True)

    # cost 数据来自 “花费(USD)” 列，原始格式类似 "101,120.89" 的 string
    df['cost'] = df['花费(USD)'].str.replace(',', '').astype(float)
    df['installs'] = df['installs'].str.replace(',', '').astype(int)
    # p1 数据来自 “首日付费率” 列，原始格式类似 "4.72%" 的 string
    df['p1'] = df['首日付费率'].str.replace('%', '').astype(float) / 100
    df['p7'] = df['7日付费率'].str.replace('%', '').astype(float) / 100
    df['roi1'] = df['1日ROI'].str.replace('%', '').astype(float) / 100
    df['roi7'] = df['7日ROI(满日)'].str.replace('%', '').astype(float) / 100

    df = df[['installDay', 'cost', 'installs', 'p1', 'p7', 'roi1', 'roi7']]
    df['pu1'] = df['installs'] * df['p1']
    df['pu7'] = df['installs'] * df['p7']
    df['revenue1'] = df['cost'] * df['roi1']
    df['revenue7'] = df['cost'] * df['roi7']

    print('按天统计：')
    print('cost 与 pu1 的相关系数：',df.corr()['cost']['pu1'])
    print('cost 与 pu7 的相关系数：',df.corr()['cost']['pu7'])
    print('cost 与 revenue1 的相关系数：',df.corr()['cost']['revenue1'])
    print('cost 与 revenue7 的相关系数：',df.corr()['cost']['revenue7'])
    
    # 将2024-09-01 去掉，只保留年周数
    df = df[df['installDay'] != '2024-09-01']

    df['week'] = df['installDay'].dt.strftime('%Y-%W')
    # print(df)

    weekDf = df.groupby('week').agg({
        'cost': 'sum',
        'pu1': 'sum',
        'pu7': 'sum',
        'revenue1': 'sum',
        'revenue7': 'sum',
    }).reset_index()
    # print(weekDf)

    print('按周统计：')
    print('cost 与 pu1 的相关系数：',weekDf.corr()['cost']['pu1'])
    print('cost 与 pu7 的相关系数：',weekDf.corr()['cost']['pu7'])
    print('cost 与 revenue1 的相关系数：',weekDf.corr()['cost']['revenue1'])
    print('cost 与 revenue7 的相关系数：',weekDf.corr()['cost']['revenue7'])


def main():

    fileAndNameList = [
        {'file': 'TopheroesAosTotal.csv', 'name': '安卓大盘'},
        {'file': 'TopheroesAosGoogle.csv', 'name': '安卓谷歌'},
        {'file': 'TopheroesAosFacebook.csv', 'name': '安卓脸书'},
        {'file': 'TopheroesAOSFacebookGpir.csv', 'name': '安卓脸书Gpir'},
        {'file': 'TopheroesAosApplovin.csv', 'name': '安卓Applovin'},
        {'file': 'TopheroesIOSApplovinIN.csv', 'name': 'iOS ApplovinIN'},
        {'file': 'TopheroesIOSTotalIN.csv', 'name': 'iOS TotalIN'},
        {'file': 'TopheroesIosTotal.csv', 'name': 'iOS大盘'},
        {'file': 'TopheroesIosApplovin.csv', 'name': 'iOSApplovin'},
    ]

    for fileAndName in fileAndNameList:
        # 按照str的方式读取csv文件
        df = pd.read_csv(fileAndName['file'], dtype=str)
        print(f"开始计算 {fileAndName['name']} 数据的相关性：")
        corr(df)
        print('\n\n')


if __name__ == '__main__':
    main()
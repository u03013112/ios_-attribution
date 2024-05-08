import pandas as pd


def corr(filename):
    df = pd.read_csv(filename)

    # 将所有列都转化为数字，其中逗号都去掉
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace(',', '').astype(float)

    dfWithoutDate = df.drop(columns=['安装时间'])

    print(dfWithoutDate.corr())



if __name__ == '__main__':
    print('topwar android:')
    twAOSFilename = 'twAndroidCostAndRevenue.csv'
    corr(twAOSFilename)
    print('-----------------')

    print('topwar ios:')
    twIOSFilename = 'twIOSCostAndRevenue.csv'
    corr(twIOSFilename)
    print('-----------------')

    print('lastwar android:')
    lwAOSFilename = 'lwAndroidCostAndRevenue.csv'
    corr(lwAOSFilename)
    print('-----------------')

    print('lastwar ios:')
    lwIOSFilename = 'lwIOSCostAndRevenue.csv'
    corr(lwIOSFilename)
    print('-----------------')
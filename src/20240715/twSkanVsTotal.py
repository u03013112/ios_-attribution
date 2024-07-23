import pandas as pd
import matplotlib.pyplot as plt

def plot_and_save(df, columns, filename):
    # 将installDate从类似20240707的str，转成日期，并作为x。按照日期升序重排数据。
    df['installDate'] = pd.to_datetime(df['installDate'], format='%Y%m%d')
    df = df.sort_values(by='installDate')
    
    # 绘制图表
    plt.figure(figsize=(10, 6))
    for column in columns:
        plt.plot(df['installDate'], df[column], label=column)
    
    plt.xlabel('Install Date')
    plt.ylabel('Value')
    plt.title(f'{filename} Over Time')
    plt.legend()
    plt.grid(True)
    
    # 保存图表
    plt.savefig(f'/src/data/tw_{filename}.png')
    plt.close()

def getData():
    skanAfDf = pd.read_csv('Topwar_skanAf_20240101_20240716.csv')
    skanAfDf = skanAfDf[['installDate', 'installs','24hROI']]
    skanAfDf.rename(columns={'installs':'skanInstalls', '24hROI':'skan24hROI'}, inplace=True)

    iOSDf = pd.read_csv('Topwar_iOS_20240101_20240716.csv')
    iOSDf = iOSDf[['installDate', 'cost', 'installs','24hROI','7dROI']]
    iOSDf.rename(columns={'installs':'iOSInstalls', '24hROI':'iOS24hROI', '7dROI':'iOS7dROI'}, inplace=True)

    df = pd.merge(iOSDf, skanAfDf, on='installDate', how='inner')
    
    df = df[[
        'installDate', 'cost',
        'iOSInstalls',  'skanInstalls',
        'iOS24hROI', 'skan24hROI', 
        'iOS7dROI'
    ]]
    
    df['installDate'] = df['installDate'].astype(str)
    df = df[df['installDate'] < '20240708']

    # 计算revenue
    df['iOS24hRevenue'] = df['cost'] * df['iOS24hROI'].str.replace('%', '').astype(float) / 100
    df['skan24hRevenue'] = df['cost'] * df['skan24hROI'].str.replace('%', '').astype(float) / 100
    df['iOS7dRevenue'] = df['cost'] * df['iOS7dROI'].str.replace('%', '').astype(float) / 100

    df.to_csv('/src/data/tw_20240716_skanVsTotal.csv', index=False)

# 按天比对
def check1():
    df = pd.read_csv('/src/data/tw_20240716_skanVsTotal.csv')
    
    # iOSInstalls,skanInstalls 的相关性
    installDf = df[['installDate','iOSInstalls','skanInstalls']].copy()
    # skanInstalls 是类似77,094 的str，需要转换成int
    installDf['skanInstalls'] = installDf['skanInstalls'].str.replace(',', '').astype(int)

    print(installDf[['iOSInstalls','skanInstalls']].corr())
    plot_and_save(installDf, ['iOSInstalls','skanInstalls'], 'skanVsTotal_install')

    # iOS24hROI,skan24hROI 的相关性
    roi24hDf = df[['installDate','iOS24hROI','skan24hROI']].copy()
    roi24hDf['iOS24hROI'] = roi24hDf['iOS24hROI'].str.replace('%', '').astype(float)
    roi24hDf['skan24hROI'] = roi24hDf['skan24hROI'].str.replace('%', '').astype(float)

    print(roi24hDf[['iOS24hROI','skan24hROI']].corr())
    plot_and_save(roi24hDf, ['iOS24hROI',  'skan24hROI'], 'skanVsTotal_roi24hDf')

    # iOS24hROI,skan24hROI,iOS7dROI 的相关性
    roi7dDf = df[['installDate','iOS24hROI','skan24hROI','iOS7dROI']].copy()
    roi7dDf['iOS24hROI'] = roi7dDf['iOS24hROI'].str.replace('%', '').astype(float)
    roi7dDf['skan24hROI'] = roi7dDf['skan24hROI'].str.replace('%', '').astype(float)
    roi7dDf['iOS7dROI'] = roi7dDf['iOS7dROI'].str.replace('%', '').astype(float)

    print(roi7dDf[['iOS24hROI','skan24hROI','iOS7dROI']].corr())
    plot_and_save(roi7dDf, ['iOS24hROI',  'skan24hROI', 'iOS7dROI'], 'skanVsTotal_roi7dDf')

def check2():
    df = pd.read_csv('/src/data/tw_20240716_skanVsTotal.csv')
    
    # 将installDate从类似20240707的str，转成日期
    df['installDate'] = pd.to_datetime(df['installDate'], format='%Y%m%d')
    
    # 按周进行汇总
    df['week'] = df['installDate'].dt.to_period('W').apply(lambda r: r.start_time)
    
    # 汇总数据
    weekly_df = df.groupby('week').agg({
        'cost': 'sum',
        'iOSInstalls': 'sum',
        'skanInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'iOS24hRevenue': 'sum',
        'skan24hRevenue': 'sum',
        'iOS7dRevenue': 'sum'
    }).reset_index()
    
    # 计算ROI
    weekly_df['iOS24hROI'] = (weekly_df['iOS24hRevenue'] / weekly_df['cost']) * 100
    weekly_df['skan24hROI'] = (weekly_df['skan24hRevenue'] / weekly_df['cost']) * 100
    weekly_df['iOS7dROI'] = (weekly_df['iOS7dRevenue'] / weekly_df['cost']) * 100
    
    # 将 week 列重命名为 installDate
    weekly_df.rename(columns={'week': 'installDate'}, inplace=True)
    
    # iOSInstalls,skanInstalls 的相关性
    installDf = weekly_df[['installDate','iOSInstalls','skanInstalls']].copy()
    print(installDf.corr())
    plot_and_save(installDf, ['iOSInstalls',  'skanInstalls'], 'skanVsTotal_install2')

    # iOS24hROI,skan24hROI 的相关性
    roi24hDf = weekly_df[['installDate','iOS24hROI','skan24hROI']].copy()
    print(roi24hDf.corr())
    plot_and_save(roi24hDf, ['iOS24hROI',  'skan24hROI'], 'skanVsTotal_roi24hDf2')

    # iOS24hROI,skan24hROI,iOS7dROI 的相关性
    roi7dDf = weekly_df[['installDate','iOS24hROI','skan24hROI','iOS7dROI']].copy()
    print(roi7dDf.corr())
    plot_and_save(roi7dDf, ['iOS24hROI',  'skan24hROI', 'iOS7dROI'], 'skanVsTotal_roi7dDf2')

def check3():
    df = pd.read_csv('/src/data/tw_20240716_skanVsTotal.csv')
    
    # 将installDate从类似20240707的str，转成日期
    df['installDate'] = pd.to_datetime(df['installDate'], format='%Y%m%d')
    
    # 按月进行汇总
    df['month'] = df['installDate'].dt.to_period('M').apply(lambda r: r.start_time)
    
    # 汇总数据
    monthly_df = df.groupby('month').agg({
        'cost': 'sum',
        'iOSInstalls': 'sum',
        'skanInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'iOS24hRevenue': 'sum',
        'skan24hRevenue': 'sum',
        'iOS7dRevenue': 'sum'
    }).reset_index()
    
    # 计算ROI
    monthly_df['iOS24hROI'] = (monthly_df['iOS24hRevenue'] / monthly_df['cost']) * 100
    monthly_df['skan24hROI'] = (monthly_df['skan24hRevenue'] / monthly_df['cost']) * 100
    monthly_df['iOS7dROI'] = (monthly_df['iOS7dRevenue'] / monthly_df['cost']) * 100
    
    # 将 month 列重命名为 installDate
    monthly_df.rename(columns={'month': 'installDate'}, inplace=True)
    
    # iOSInstalls,skanInstalls 的相关性
    installDf = monthly_df[['installDate','iOSInstalls','skanInstalls']].copy()
    print(installDf.corr())
    plot_and_save(installDf, ['iOSInstalls',  'skanInstalls'], 'skanVsTotal_install3')

    # iOS24hROI,skan24hROI 的相关性
    roi24hDf = monthly_df[['installDate','iOS24hROI','skan24hROI']].copy()
    print(roi24hDf.corr())
    plot_and_save(roi24hDf, ['iOS24hROI',  'skan24hROI'], 'skanVsTotal_roi24hDf3')

    # iOS24hROI,skan24hROI,iOS7dROI 的相关性
    roi7dDf = monthly_df[['installDate','iOS24hROI','skan24hROI','iOS7dROI']].copy()
    print(roi7dDf.corr())
    plot_and_save(roi7dDf, ['iOS24hROI',  'skan24hROI', 'iOS7dROI'], 'skanVsTotal_roi7dDf3')


if __name__ == '__main__':
    getData()
    check1()
    check2()
    check3()

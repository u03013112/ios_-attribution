# 比对 skan 的整体走势与iOS大盘走势是否一致
# 数据源来自3个地方：skanAf，merge，iOS
# 按照自然日进行比对
# 按照周做汇总后进行比对
# 按月做汇总后进行比对

import pandas as pd
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

    df.to_csv('/src/data/tw_20240716_skanVsTotal.csv', index=False)

# 按天比对
def check1():
    df = pd.read_csv('/src/data/tw_20240716_skanVsTotal.csv')
    
    # iOSInstalls,mergeInstalls,skanInstalls 的相关性
    installDf = df[['installDate','iOSInstalls','skanInstalls']].copy()
    # mergeInstalls 和 skanInstalls 是类似77,094 的str，需要转换成int
    installDf['skanInstalls'] = installDf['skanInstalls'].str.replace(',', '').astype(int)

    print(installDf[['iOSInstalls','skanInstalls']].corr())
    plot_and_save(installDf, ['iOSInstalls','skanInstalls'], 'skanVsTotal_install')

    # iOS24hROI,merge24hROI,skan24hROI 的相关性
    roi24hDf = df[['installDate','iOS24hROI','skan24hROI']].copy()
    roi24hDf['iOS24hROI'] = roi24hDf['iOS24hROI'].str.replace('%', '').astype(float)
    roi24hDf['skan24hROI'] = roi24hDf['skan24hROI'].str.replace('%', '').astype(float)

    print(roi24hDf[['iOS24hROI','skan24hROI']].corr())
    plot_and_save(roi24hDf, ['iOS24hROI',  'skan24hROI'], 'skanVsTotal_roi24hDf')

    # iOS24hROI,merge24hROI,skan24hROI,iOS7dROI,merge7dROI 的相关性
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
        'iOSInstalls': 'sum',
        'skanInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'iOS24hROI': lambda x: x.str.replace('%', '').astype(float).sum(),
        'skan24hROI': lambda x: x.str.replace('%', '').astype(float).sum(),
        'iOS7dROI': lambda x: x.str.replace('%', '').astype(float).sum()
    }).reset_index()
    
    # 将 week 列重命名为 installDate
    weekly_df.rename(columns={'week': 'installDate'}, inplace=True)
    
    # iOSInstalls,mergeInstalls,skanInstalls 的相关性
    installDf = weekly_df[['installDate','iOSInstalls','skanInstalls']].copy()
    print(installDf.corr())
    plot_and_save(installDf, ['iOSInstalls',  'skanInstalls'], 'skanVsTotal_install2')

    # iOS24hROI,merge24hROI,skan24hROI 的相关性
    roi24hDf = weekly_df[['installDate','iOS24hROI','skan24hROI']].copy()
    print(roi24hDf.corr())
    plot_and_save(roi24hDf, ['iOS24hROI',  'skan24hROI'], 'skanVsTotal_roi24hDf2')

    # iOS24hROI,merge24hROI,skan24hROI,iOS7dROI,merge7dROI 的相关性
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
        'iOSInstalls': 'sum',
        'skanInstalls': lambda x: x.str.replace(',', '').astype(int).sum(),
        'iOS24hROI': lambda x: x.str.replace('%', '').astype(float).sum(),
        'skan24hROI': lambda x: x.str.replace('%', '').astype(float).sum(),
        'iOS7dROI': lambda x: x.str.replace('%', '').astype(float).sum()
    }).reset_index()
    
    # 将 month 列重命名为 installDate
    monthly_df.rename(columns={'month': 'installDate'}, inplace=True)
    
    # iOSInstalls,mergeInstalls,skanInstalls 的相关性
    installDf = monthly_df[['installDate','iOSInstalls','skanInstalls']].copy()
    print(installDf.corr())
    plot_and_save(installDf, ['iOSInstalls',  'skanInstalls'], 'skanVsTotal_install3')

    # iOS24hROI,merge24hROI,skan24hROI 的相关性
    roi24hDf = monthly_df[['installDate','iOS24hROI','skan24hROI']].copy()
    print(roi24hDf.corr())
    plot_and_save(roi24hDf, ['iOS24hROI',  'skan24hROI'], 'skanVsTotal_roi24hDf3')

    # iOS24hROI,merge24hROI,skan24hROI,iOS7dROI,merge7dROI 的相关性
    roi7dDf = monthly_df[['installDate','iOS24hROI','skan24hROI','iOS7dROI']].copy()
    print(roi7dDf.corr())
    plot_and_save(roi7dDf, ['iOS24hROI',  'skan24hROI', 'iOS7dROI'], 'skanVsTotal_roi7dDf3')


if __name__ == '__main__':
    # getData()
    # check1()
    # check2()
    check3()
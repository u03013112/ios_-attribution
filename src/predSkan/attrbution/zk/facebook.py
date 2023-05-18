# 对融合归因的Facebook媒体优化

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def getData():
    df = pd.read_csv('/src/data/zk/attribution1RetCheck3.csv')
    # 由于模拟iOS，所以只能看到如下这些数据，其中r7usd是为了重算MAPE用的，并不参与优化
    df = df [[
        'install_date',
        'media',
        'r1usd_att',
        'r7usd_att',
        'user_count_att',
        'pay_count_att',
        'r1usd',
        'r7usd'
    ]]
    # print(df.head(2))
    # #   install_date         media   r1usd_att    r7usd_att  user_count_att  pay_count_att        r7usd
    # # 0   2022-01-01  Facebook Ads  582.197579  2044.212390     7393.208208      97.361954  1716.189787
    # # 1   2022-01-02  Facebook Ads  978.634895  3482.217358    16812.108738     187.908390  2370.100699
    return df

def mind1(df):
    # 思路一：付费金额低的日期可信度低。
    # 具体步骤，找到所有数据中r1usd_att最小值和最大值
    # 从最小值开始，每次增加（最大值-最小值）*0.01，作为阈值
    # 将r1usd_att小于阈值的数据忽略，不参与MAPE计算
    # 计算每一行的r7usd_att与r7usd的MAPE，并计算所有行的MAPE均值
    # 结果按照 阈值，MAPE，损失行数的比例（即损失行数/总行数） 记录到csv中，路径 '/src/data/zk/mind1.csv'
    # 并将上述结果画图，路径 '/src/data/zk/mind1.jpg'
    # 阈值是x坐标，MAPE和损失行数的比例是y坐标，双y轴
    min_r1usd_att = df['r1usd_att'].min()
    max_r1usd_att = df['r1usd_att'].max()

    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = min_r1usd_att + (max_r1usd_att - min_r1usd_att) * 0.01 * i
        filtered_df = df[df['r1usd_att'] >= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind1.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind1.jpg')

def mind2(df):
    # 仿照上面mind1的思路，不过是用r7usd_att来做阈值，其他功能一致
    min_r7usd_att = df['r7usd_att'].min()
    max_r7usd_att = df['r7usd_att'].max()

    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = min_r7usd_att + (max_r7usd_att - min_r7usd_att) * 0.01 * i
        filtered_df = df[df['r7usd_att'] >= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind2.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind2.jpg')

def mind3(df):
    min_user_count_att = df['user_count_att'].min()
    max_user_count_att = df['user_count_att'].max()

    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = min_user_count_att + (max_user_count_att - min_user_count_att) * 0.01 * i
        filtered_df = df[df['user_count_att'] >= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind3.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind3.jpg')

def mind4(df):
    min_pay_count_att = df['pay_count_att'].min()
    max_pay_count_att = df['pay_count_att'].max()

    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = min_pay_count_att + (max_pay_count_att - min_pay_count_att) * 0.01 * i
        filtered_df = df[df['pay_count_att'] >= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind4.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind4.jpg')

def mind5(df):
    # 主要关注的是r1usd_att
    # 按照目前日期排序，对主要关注列进行向前滚动，计算3日平均值，不能计算3日平均值的行，直接排除掉（前两行）
    # 如果该行的值与3日平均值的差值超过阈值，则认为该行是异常值，排除掉
    # 阈值从0开始，每次增加0.01，直到1
    # 剩下的功能上述mind1-4一致
    df = df.sort_values(by='install_date')
    df['r1usd_att_rolling_mean'] = df['r1usd_att'].rolling(window=3).mean()
    df = df.dropna(subset=['r1usd_att_rolling_mean'])
    
    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = 0.01 * i
        filtered_df = df[np.abs((df['r1usd_att'] - df['r1usd_att_rolling_mean']) / df['r1usd_att_rolling_mean']) <= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind5.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind5.jpg')

def mind6(df):
    df = df.sort_values(by='install_date')
    df['r7usd_att_rolling_mean'] = df['r7usd_att'].rolling(window=3).mean()
    df = df.dropna(subset=['r7usd_att_rolling_mean'])
    
    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = 0.01 * i
        filtered_df = df[np.abs((df['r7usd_att'] - df['r7usd_att_rolling_mean']) / df['r7usd_att_rolling_mean']) <= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind6.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind6.jpg')

def mind7(df):
    df = df.sort_values(by='install_date')
    df['user_count_att_rolling_mean'] = df['user_count_att'].rolling(window=3).mean()
    df = df.dropna(subset=['user_count_att_rolling_mean'])
    
    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = 0.01 * i
        filtered_df = df[np.abs((df['user_count_att'] - df['user_count_att_rolling_mean']) / df['user_count_att_rolling_mean']) <= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind7.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind7.jpg')

def mind8(df):
    df = df.sort_values(by='install_date')
    df['pay_count_att_rolling_mean'] = df['pay_count_att'].rolling(window=3).mean()
    df = df.dropna(subset=['pay_count_att_rolling_mean'])
    
    threshold_values = []
    mape_values = []
    loss_ratios = []

    for i in range(101):
        threshold = 0.01 * i
        filtered_df = df[np.abs((df['pay_count_att'] - df['pay_count_att_rolling_mean']) / df['pay_count_att_rolling_mean']) <= threshold]
        mape = np.mean(np.abs((filtered_df['r7usd_att'] - filtered_df['r7usd']) / filtered_df['r7usd'])) * 100
        loss_ratio = (len(df) - len(filtered_df)) / len(df)

        threshold_values.append(threshold)
        mape_values.append(mape)
        loss_ratios.append(loss_ratio)

    result_df = pd.DataFrame({'threshold': threshold_values, 'mape': mape_values, 'loss_ratio': loss_ratios})
    result_df.to_csv('/src/data/zk/mind8.csv', index=False)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(threshold_values, mape_values, 'g-')
    ax2.plot(threshold_values, loss_ratios, 'b-')

    ax1.set_xlabel('Threshold')
    ax1.set_ylabel('MAPE', color='g')
    ax2.set_ylabel('Loss Ratio', color='b')

    plt.savefig('/src/data/zk/mind8.jpg')

# 每天媒体用户的质量差异计算方式
# 先按照cv给不同媒体分组
# 按照安装日期给每个媒体分组
# 然后按照媒体计算每天每个cv的均值和标准差
# 按照当日cv的回收金额（首日or7日）计算权重
# 将上面计算的均值和标准差的偏差（MAPE）乘以该CV在媒体当日的权重，然后求和获得当日媒体的质量差异
# 然后对比每天每个媒体的质量，质量相似的媒体质量相似

# 这个可以尝试看到是否是媒体质量差异较大的时间，融合归因的结果较差
# 如果真是这样，不太好处理。因为iOS无法有效的区分媒体
import os
def mind9():
    if not os.path.exists('/src/data/zk/mind9-0.csv'):
        df = pd.read_csv('/src/data/zk/androidFp03.csv')
        # 列 media_source 改名 media
        df = df.rename(columns={'media_source':'media'})
        # 列 uid 改名 appsflyer_id
        df = df.rename(columns={'uid':'appsflyer_id'})

        cvMapDf = pd.read_csv('/src/afCvMap2304.csv')
        cvMapDf = cvMapDf.loc[(cvMapDf['event_name'] == 'af_skad_revenue') & (cvMapDf['conversion_value']<32)]
        cvMapDf = cvMapDf[['conversion_value','min_event_revenue','max_event_revenue']]

        df.loc[:,'cv'] = 0
        for index, row in cvMapDf.iterrows():
            df.loc[(df['r1usd'] > row['min_event_revenue']) & (df['r1usd'] <= row['max_event_revenue']),'cv'] = row['conversion_value']
        # 如果r1usd > 最大max_event_revenue，则取最大值
        df.loc[df['r1usd'] > cvMapDf['max_event_revenue'].max(),'cv'] = cvMapDf['conversion_value'].max()

        df['cv'] = df['cv'].astype(int)
        df['cvGroup'] = df['cv']//10
        df['cv'] = df['cvGroup'].astype(int)

        df.to_csv('/src/data/zk/mind9-0.csv', index=False)
    else:
        df = pd.read_csv('/src/data/zk/mind9-0.csv')

    df['count'] = 1
    df = df.groupby(['media','install_date','cv']).agg({'r1usd':'sum','r7usd':'sum','count':'sum'}).reset_index()
    # print(df.head(10))

    # 从mediaList中遍历media
    # 从df中取出media的数据，与所有数据进行比对，比如方法如下：
    # 按install_date（精度为天）进行分天比较，每天计算出一个整体偏差程度
    # 将指定天的数据按照cv的均值进行比较，计算出偏差（类似MAPE，但是有符号，可以是负值），然后乘以权重
    # 权重计算方式是该cv首日付费金额（r1usd）所占比例
    # 最后将所有cv的偏差求和，得到该天的媒体质量差异
    # 每个媒体输出一个csv，文件路径为 /src/data/zk/mind9-1-{media}.csv
    # 要有列 install_date, 偏差
    mediaList = [
        'googleadwords_int',
        'Facebook Ads',
        'bytedanceglobal_int',
        'snapchat_int'
    ]
    all_data_df = df.groupby(['install_date', 'cv']).agg({'r1usd': 'sum', 'count': 'sum'}).reset_index()
    all_data_df['r1usd_mean_all'] = all_data_df['r1usd'] / all_data_df['count']

    for media in mediaList:
        media_df = df[df['media'] == media]
        deviation_list = []
        for date in media_df['install_date'].unique():
            date_df = media_df[media_df['install_date'] == date]
            all_data_date_df = all_data_df[all_data_df['install_date'] == date]
            date_df = date_df.merge(all_data_date_df, on=['install_date', 'cv'], suffixes=('', '_all'))
            date_df['r1usd_mean'] = date_df['r1usd'] / date_df['count']
            date_df['weight'] = date_df['r1usd_all'] / date_df['r1usd_all'].sum()
            date_df['deviation'] = (date_df['r1usd_mean'] - date_df['r1usd_mean_all'])/date_df['r1usd_mean'] * date_df['weight']
            daily_deviation = date_df['deviation'].sum()
            deviation_list.append({'install_date': date, 'deviation': daily_deviation})

        deviation_df = pd.DataFrame(deviation_list)
        deviation_df.sort_values(by='install_date', inplace=True)
        deviation_df.to_csv(f'/src/data/zk/mind9-1-{media}.csv', index=False)

import matplotlib.dates as mdates
def mind9_1(df):
    dDf = pd.read_csv('/src/data/zk/mind9-1-Facebook Ads.csv')
    df = df.merge(dDf, on=['install_date'])
    df['mape'] = abs(df['r7usd'] - df['r7usd_att']) / df['r7usd']
    df['deviation'] = abs(df['deviation'])
    df['install_date'] = pd.to_datetime(df['install_date'])

    # 计算3日均线
    df['mape_3d_avg'] = df['mape'].rolling(window=3).mean()
    df['deviation_3d_avg'] = df['deviation'].rolling(window=3).mean()

    fig, ax1 = plt.subplots(figsize=(15, 6))

    ax1.set_xlabel('install_date')
    ax1.set_ylabel('mape', color='tab:blue')
    ax1.plot(df['install_date'], df['mape_3d_avg'], color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')

    ax2 = ax1.twinx()
    ax2.set_ylabel('deviation', color='tab:red')
    ax2.plot(df['install_date'], df['deviation_3d_avg'], color='tab:red')
    ax2.axhline(y=0, color='gray', linestyle='--')
    ax2.tick_params(axis='y', labelcolor='tab:red')

    ax1.xaxis.set_major_locator(mdates.MonthLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    fig.autofmt_xdate()

    fig.tight_layout()
    plt.savefig('/src/data/zk/mind9.jpg')

def mind10(df):
    df['r7/r1'] = df['r7usd'] / df['r1usd']
    df['r7_att/r1_att'] = df['r7usd_att'] / df['r1usd_att']
    df['r1_att/r1'] = df['r1usd_att'] / df['r1usd']

    df['install_date'] = pd.to_datetime(df['install_date'])

    fig, ax = plt.subplots(figsize=(15, 6))

    ax.set_xlabel('install_date')
    ax.set_ylabel('values')
    ax.plot(df['install_date'], df['r7/r1'], label='r7/r1')
    ax.plot(df['install_date'], df['r7_att/r1_att'], label='r7_att/r1_att')
    ax.plot(df['install_date'], df['r1_att/r1'], label='r1_att/r1')
    ax.legend()

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    fig.autofmt_xdate()

    fig.tight_layout()
    plt.savefig('/src/data/zk/mind10.jpg')

if __name__ == '__main__':
    df = getData()
    # mind1(df)
    # mind2(df)
    # mind3(df)
    # mind4(df)

    # mind5(df)
    # mind6(df)
    # mind7(df)
    # mind8(df)
    # mind9()
    mind9_1(df)
    # mind10(df)